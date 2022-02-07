package com.wizard.warehouse.realtime.jobs;

import com.wizard.warehouse.realtime.kafka.UniqDeserializer;
import com.wizard.warehouse.realtime.pojo.DataBean;
import com.wizard.warehouse.realtime.udf.JsonToBeanFuncWithUniq;
import com.wizard.warehouse.realtime.utils.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * add data to clickhouse
 */
public class DataToClickHouse {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        DataStream<Tuple2<String, String>> kafkaStreamWithUniq = FlinkUtils.createKafkaStreamWithUniq(parameterTool, UniqDeserializer.class);
        // parse data
        SingleOutputStreamOperator<DataBean> beanStream = kafkaStreamWithUniq.process(new JsonToBeanFuncWithUniq());

        beanStream.map(new MapFunction<DataBean, DataBean>() {

            private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH");

            @Override
            public DataBean map(DataBean bean) throws Exception {
                Long timestamp = bean.getTimestamp();
                String format = dateFormat.format(new Date(timestamp));
                String[] fields = format.split("-");


                return null;
            }
        })



    }
}
