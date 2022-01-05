package com.wizard.warehouse.capture.functions;

import com.alibaba.fastjson.JSONObject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.wizard.warehouse.capture.constant.InitialConstants;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;

/**
 * @Author: sodamnsure
 * @Date: 2022/1/4 6:06 PM
 * @Desc: Doris Sink
 */
public class DorisSinkBuilder {
    Config config;

    public DorisSinkBuilder() {
        config = ConfigFactory.load();
    }

    public SinkFunction<JSONObject> build() {
        Properties pro = new Properties();
        pro.setProperty("format", config.getString(InitialConstants.DORIS_EXECUTION_PROP_FORMAT));
        pro.setProperty("strip_outer_array", config.getString(InitialConstants.DORIS_EXECUTION_PROP_STRIP_OUTER_ARRAY));
        pro.setProperty("function_column.sequence_col", config.getString(InitialConstants.DORIS_EXECUTION_PROP_SEQUENCE_COL));

        return DorisSink.sink(
                DorisReadOptions.builder().build(),
                DorisExecutionOptions.builder()
                        .setBatchSize(config.getInt(InitialConstants.DORIS_EXECUTION_BATCH_SIZE))
                        .setBatchIntervalMs(config.getLong(InitialConstants.DORIS_EXECUTION_BATCH_INTERVAL))
                        .setMaxRetries(config.getInt(InitialConstants.DORIS_EXECUTION_MAX_RETRIES))
                        .setStreamLoadProp(pro).build(),
                DorisOptions.builder()
                        .setFenodes(config.getString(InitialConstants.DORIS_OPTIONS_FE_NODES))
                        .setTableIdentifier(config.getString(InitialConstants.DORIS_OPTIONS_TABLE))
                        .setUsername(config.getString(InitialConstants.DORIS_OPTIONS_USERNAME))
                        .setPassword(config.getString(InitialConstants.DORIS_OPTIONS_PASSWORD)).build());
    }
}
