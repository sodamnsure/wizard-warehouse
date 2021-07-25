package com.feelings.warehouse.realtime.jobs;

import com.feelings.warehouse.realtime.utils.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @Author: sodamnsure
 * @Date: 2021/7/23 5:56 下午
 */
public class PreEtl {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(args, SimpleStringSchema.class);
        // Transformation

        // Sink

        FlinkUtils.env.execute();
    }
}
