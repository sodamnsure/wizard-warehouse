package com.wizard.warehouse.capture.main;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.wizard.warehouse.capture.functions.JsonToBeanFunction;
import com.wizard.warehouse.capture.functions.KafkaSourceBuilder;
import com.wizard.warehouse.capture.functions.MySqlSourceBuilder;
import com.wizard.warehouse.capture.functions.ProcessDataFunction;
import com.wizard.warehouse.capture.utils.StateDescContainer;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author: sodamnsure
 * @Date: 2021/11/18 2:10 下午
 * @Desc: 主类
 */
public class Main {
    public static void main(String[] args) throws Exception {
        /*
          构建env
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(1000);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        /*
          读取canal采集的数据
         */
        KafkaSourceBuilder kafkaSourceBuilder = new KafkaSourceBuilder();
        DataStream<String> dataStream = env.addSource(kafkaSourceBuilder.build());

        /*
          Json解析
         */
        SingleOutputStreamOperator<JSONObject> jsonDStream = dataStream.map(new JsonToBeanFunction());

        /*
          使用FlinkCDC消费配置表并处理成广播流
         */
        MySqlSourceBuilder mySqlSourceBuilder = new MySqlSourceBuilder();
        MySqlSource<String> mySqlSource = mySqlSourceBuilder.build();
        DataStreamSource<String> tableConfDStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        BroadcastStream<String> broadcastStream = tableConfDStream.broadcast(StateDescContainer.getConfigState());

        /*
          链接主流与广播流
         */
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonDStream.connect(broadcastStream);

        /*
          处理"Change Data Capture"
         */
        SingleOutputStreamOperator<JSONObject> resultStream = connectedStream.process(new ProcessDataFunction());

        resultStream.print();

        //Properties pro = new Properties();
        //pro.setProperty("format", "json");
        //pro.setProperty("strip_outer_array", "true");
        //pro.setProperty("function_column.sequence_col", "update_time");
        //
        //resultStream.addSink(
        //        DorisSink.sink(
        //                DorisReadOptions.builder().build(),
        //                DorisExecutionOptions.builder()
        //                        .setBatchSize(3)
        //                        .setBatchIntervalMs(0L)
        //                        .setMaxRetries(3)
        //                        .setStreamLoadProp(pro).build(),
        //                DorisOptions.builder()
        //                        .setFenodes("test:8030")
        //                        .setTableIdentifier("example_db.user_log_1")
        //                        .setUsername("test")
        //                        .setPassword("test").build()
        //        )
        //);

        env.execute();
    }

}
