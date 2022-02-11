package com.wizard.warehouse.realtime.jobs;

import com.wizard.warehouse.realtime.kafka.UniqDeserializer;
import com.wizard.warehouse.realtime.pojo.DataBean;
import com.wizard.warehouse.realtime.udf.JsonToBeanFuncWithUniq;
import com.wizard.warehouse.realtime.utils.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Add data to clickhouse
 */
public class DataToClickHouse {
    public static void main(String[] args) throws Exception {

        DataStream<Tuple2<String, String>> kafkaStreamWithUniq = FlinkUtils.createKafkaStreamWithUniq(args, UniqDeserializer.class);
        // parse data
        SingleOutputStreamOperator<DataBean> beanStream = kafkaStreamWithUniq.process(new JsonToBeanFuncWithUniq());

        beanStream.map(new MapFunction<DataBean, DataBean>() {

            private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH");

            @Override
            public DataBean map(DataBean bean) throws Exception {
                Long timestamp = bean.getTimestamp();
                String format = dateFormat.format(new Date(timestamp));
                String[] fields = format.split("-");

                bean.setDate(fields[0]);
                bean.setHour(fields[1]);

                return bean;
            }
        }).addSink(JdbcSink.sink(
                "insert into tb_user_event values (?,?,?,?,?,?,?,?,?,?,?,?)",
                (ps, tp) -> {
                    ps.setString(1, tp.getId());
                    ps.setString(2, tp.getDeviceId());
                    ps.setString(3, tp.getEventId());
                    ps.setInt(4, tp.getIsN());
                    ps.setString(5, tp.getProvince());
                    ps.setString(6, tp.getOsName());
                    ps.setString(7, tp.getReleaseChannel());
                    ps.setString(8, tp.getDeviceType());
                    ps.setLong(9, tp.getTimestamp());
                    ps.setString(10, tp.getDate());
                    ps.setString(11, tp.getHour());
                    ps.setDate(12, new java.sql.Date(System.currentTimeMillis()));

                },
                JdbcExecutionOptions.builder()
                        // clickhouse.batch.size => 3
                        .withBatchSize(FlinkUtils.parameterTool.getInt("clickhouse.batch.size", 3))
                        // clickhouse.batch.interval => 5s
                        .withBatchIntervalMs(FlinkUtils.parameterTool.getInt("clickhouse.batch.interval", 5000))
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        // clickhouse.url => jdbc:clickhouse://marketing:8123/default
                        .withUrl(FlinkUtils.parameterTool.getRequired("clickhouse.url"))
                        // clickhouse.driver => ru.yandex.clickhouse.ClickHouseDriver
                        .withDriverName(FlinkUtils.parameterTool.getRequired("clickhouse.driver"))
                        .build()));


        FlinkUtils.env.execute();


    }
}
