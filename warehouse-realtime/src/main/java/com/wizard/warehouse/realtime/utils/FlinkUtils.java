package com.wizard.warehouse.realtime.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * This class provides simple utility methods for creating data stream from different sources.
 */
public class FlinkUtils {
    // obtain an final execution environment
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * @param parameterTool reading and parsing program arguments from different sources
     * @param deserializer  defines how to deserialize binaries of Kafka message value
     * @param <T>           type of the returned stream
     * @return the data stream constructed from kafka
     */
    public static <T> DataStream<T> createKafkaStream(ParameterTool parameterTool, Class<? extends DeserializationSchema<T>> deserializer) throws Exception {
        // get the String value for the given key.
        long checkpointInterval = parameterTool.getLong("checkpoint.interval", 30000L);
        String checkpointStorage = parameterTool.getRequired("checkpoint.storage");
        // enabling and configuring checkpointing, start a checkpoint every 30000 ms
        env.enableCheckpointing(checkpointInterval);
        // set state backends
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        // set mode to exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // sets the checkpoint storage where checkpoint snapshots will be written
        env.getCheckpointConfig().setCheckpointStorage(checkpointStorage);

        // build a kafka source to consume messages
        Properties properties = parameterTool.getProperties();
        List<String> topics = Arrays.asList(parameterTool.getRequired("kafka.input.topics").split(","));
        // subscribing messages from all partitions in a list of topics
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializer.newInstance(), properties);
        return env.addSource(kafkaConsumer);
    }

}
