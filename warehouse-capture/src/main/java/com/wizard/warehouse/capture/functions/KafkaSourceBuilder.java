package com.wizard.warehouse.capture.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.wizard.warehouse.capture.constant.InitialConstants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: sodamnsure
 * @Date: 2021/8/18 11:24 上午
 * @Desc: 各类kafka source的构建工具
 */
public class KafkaSourceBuilder {
    Config config;

    public KafkaSourceBuilder() {
        config = ConfigFactory.load();
    }

    public FlinkKafkaConsumer<String> build() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.getString(InitialConstants.KAFKA_BOOTSTRAP_SERVERS));
        props.setProperty("auto.offset.reset", config.getString(InitialConstants.KAFKA_AUTO_OFFSET_RESET));

        String topic = config.getString(InitialConstants.KAFKA_CANAL_DETAIL_TOPIC);

        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
    }
}
