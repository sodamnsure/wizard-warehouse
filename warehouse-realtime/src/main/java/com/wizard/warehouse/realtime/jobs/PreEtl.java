package com.wizard.warehouse.realtime.jobs;

import com.wizard.warehouse.realtime.utils.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 *
 */
public class PreEtl {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(args, SimpleStringSchema.class);

        FlinkUtils.env.execute();
    }
}
