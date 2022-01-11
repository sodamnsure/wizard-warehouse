package com.wizard.warehouse.realtime.jobs;

import com.wizard.warehouse.realtime.utils.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 *
 */
public class PreEtl {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(parameterTool, SimpleStringSchema.class);

        FlinkUtils.env.execute();
    }
}
