package com.wizard.warehouse.realtime.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * This class provides simple utility methods for creating data stream from different sources.
 */
public class FlinkUtils {
    // obtain an final execution environment
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // reading and parsing program arguments from different sources
    public static ParameterTool parameterTool;

    /**
     * @param args         the param that contains the configuration file path
     * @param deserializer defines how to deserialize binaries of Kafka message value
     * @param <T>          type of the returned stream
     * @return the data stream constructed from kafka
     */
    public static <T> DataStream<T> createKafkaStream(String[] args, Class<? extends DeserializationSchema<T>> deserializer) throws Exception {
        parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        // get the String value for the given key.
        long checkpointInterval = parameterTool.getLong("checkpoint.interval", 30000L);
        String checkpointStorage = parameterTool.getRequired("checkpoint.storage");
        // enabling and configuring checkpointing, start a checkpoint every 30000 ms
        env.enableCheckpointing(checkpointInterval);
        // set state backends
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        CheckpointConfig config = env.getCheckpointConfig();
        // set mode to exactly-once
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // set the checkpoint storage where checkpoint snapshots will be written
        config.setCheckpointStorage(checkpointStorage);
        // retain the checkpoint when the job is cancelled.
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // build a kafka source to consume messages
        Properties properties = parameterTool.getProperties();
        List<String> topics = Arrays.asList(parameterTool.getRequired("kafka.input.topics").split(","));
        // subscribing messages from all partitions in a list of topics
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializer.newInstance(), properties);
        // specifies the consumer should not commit offsets back to Kafka on checkpoints.
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        return env.addSource(kafkaConsumer);
    }

    /**
     * @param args         args the param that contains the configuration file path
     * @param deserializer defines how to deserialize binaries of Kafka message value or metadata such as topic、partition、offset
     * @param <T>          type of the returned stream
     * @return the data stream constructed from kafka
     */
    public static <T> DataStream<T> createKafkaStreamWithUniq(String[] args, Class<? extends KafkaDeserializationSchema<T>> deserializer) throws Exception {
        parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        // get the String value for the given key.
        long checkpointInterval = parameterTool.getLong("checkpoint.interval", 30000L);
        String checkpointStorage = parameterTool.getRequired("checkpoint.storage");
        // enabling and configuring checkpointing, start a checkpoint every 30000 ms
        env.enableCheckpointing(checkpointInterval);
        // set state backends
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        CheckpointConfig config = env.getCheckpointConfig();
        // set mode to exactly-once
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // set the checkpoint storage where checkpoint snapshots will be written
        config.setCheckpointStorage(checkpointStorage);
        // retain the checkpoint when the job is cancelled.
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // build a kafka source to consume messages
        Properties properties = parameterTool.getProperties();
        List<String> topics = Arrays.asList(parameterTool.getRequired("kafka.input.topics").split(","));
        // subscribing messages from all partitions in a list of topics
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializer.newInstance(), properties);
        // specifies the consumer should not commit offsets back to Kafka on checkpoints.
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        return env.addSource(kafkaConsumer);
    }
}
