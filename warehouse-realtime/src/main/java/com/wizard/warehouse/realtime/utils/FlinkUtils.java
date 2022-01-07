package com.wizard.warehouse.realtime.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sodamnsure
 * @Date: 2022/1/7 11:30 AM
 * @Desc: flink 工具类
 */
public class FlinkUtils {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        long checkpointInterval = parameterTool.getLong("checkpoint.interval", 30000L);
        String checkpointStorage = parameterTool.getRequired("checkpoint.storage");
        // enabling and configuring checkpointing
        env.enableCheckpointing(checkpointInterval);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(checkpointStorage);



    }
}
