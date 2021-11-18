package com.wizard.warehouse.cdc.main;

import com.fasterxml.jackson.databind.JsonNode;
import com.wizard.warehouse.cdc.functions.JsonToBeanFunction;
import com.wizard.warehouse.cdc.functions.KafkaSourceBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

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
        DataStream<String> stream = env.addSource(kafkaSourceBuilder.build("topic1"));

        /*
          Json解析
         */
        SingleOutputStreamOperator<JsonNode> filter = stream.map(new JsonToBeanFunction()).filter(Objects::nonNull);

        filter.print();

        env.execute();
    }

}
