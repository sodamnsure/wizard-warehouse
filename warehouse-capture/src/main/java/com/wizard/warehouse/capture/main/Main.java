package com.wizard.warehouse.capture.main;

import com.alibaba.fastjson.JSONObject;
import com.wizard.warehouse.capture.functions.JsonToBeanFunction;
import com.wizard.warehouse.capture.functions.KafkaSourceBuilder;
import com.wizard.warehouse.capture.functions.ProcessDataFunction;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        /*
          读取canal采集的数据
         */
        KafkaSourceBuilder kafkaSourceBuilder = new KafkaSourceBuilder();
        DataStream<String> dataStream = env.addSource(kafkaSourceBuilder.build());

        /*
          Json解析
         */
        SingleOutputStreamOperator<JSONObject> jsonStream = dataStream.map(new JsonToBeanFunction());

        /*
          处理"Change Data Capture"函数
         */
        SingleOutputStreamOperator<String> resultStream = jsonStream.process(new ProcessDataFunction());

        resultStream.print();

        resultStream.addSink(
                DorisSink.sink(
                        DorisOptions.builder()
                                .setFenodes("test:8030")
                                .setTableIdentifier("example_db.user")
                                .setUsername("test")
                                .setPassword("test").build()
                )
        );

        env.execute();
    }

}
