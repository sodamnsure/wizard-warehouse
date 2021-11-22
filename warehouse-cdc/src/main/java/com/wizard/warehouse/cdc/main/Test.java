package com.wizard.warehouse.cdc.main;

import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Properties;

/**
 * @Author: sodamnsure
 * @Date: 2021/11/22 2:45 下午
 * @Desc:
 */
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.put("fenodes","marketing:8030");
        properties.put("username","doris");
        properties.put("password","Pandeng8848");
        properties.put("table.identifier","example_db.user");
        DataStreamSink<List<?>> source = env.addSource(new DorisSourceFunction(new DorisStreamOptions(properties), new SimpleListDeserializationSchema())).print();
        env.execute();
    }
}
