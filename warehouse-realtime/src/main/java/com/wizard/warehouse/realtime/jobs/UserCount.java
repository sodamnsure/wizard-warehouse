package com.wizard.warehouse.realtime.jobs;

import com.wizard.warehouse.realtime.utils.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @Author: sodamnsure
 * @Date: 2021/9/1 5:20 下午
 * @Desc: 当日新增用户、老用户、活跃用户
 * 维度：区域、系统、手机品牌
 */
public class UserCount {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(args, SimpleStringSchema.class);
        kafkaStream.print();

        FlinkUtils.env.execute();
    }
}
