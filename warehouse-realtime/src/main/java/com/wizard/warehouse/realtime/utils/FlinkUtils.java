package com.wizard.warehouse.realtime.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Author: sodamnsure
 * @Date: 2021/7/23 4:59 下午
 */
public class FlinkUtils {
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T> createKafkaStream(String[] args, Class<? extends DeserializationSchema<T>> deserializer) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        long checkpointInterval = parameterTool.getLong("checkpoint.interval", 60000);
        String checkpointPath = parameterTool.getRequired("checkpoint.path");

        // 开启Checkpoint, 每隔60000 ms进行启动一个检查点, 设置模式为exactly-once,
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        // 设置状态的存储后端
        env.setStateBackend(new FsStateBackend(checkpointPath));
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 读取Kafka中的Topic
        List<String> topics = Arrays.asList(parameterTool.getRequired("kafka.input.topics").split(","));

        // 读取属性
        Properties properties = parameterTool.getProperties();

        // 从Kafka中读取数据
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializer.newInstance(), properties);

        // 是否将偏移量写入到kafka topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);
    }
}
