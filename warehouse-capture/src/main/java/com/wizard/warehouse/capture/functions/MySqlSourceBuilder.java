package com.wizard.warehouse.capture.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.wizard.warehouse.capture.constant.InitialConstants;

/**
 * @Author: sodamnsure
 * @Date: 2021/12/30 3:29 PM
 * @Desc: MySql Source构建工具
 */
public class MySqlSourceBuilder {
    Config config;

    public MySqlSourceBuilder() {
        config = ConfigFactory.load();
    }

    public MySqlSource<String> build() {
        return MySqlSource.<String>builder()
                .hostname(config.getString(InitialConstants.MYSQL_DATASOURCE_HOSTNAME))
                .port(config.getInt(InitialConstants.MYSQL_DATASOURCE_PORT))
                .databaseList(config.getString(InitialConstants.MYSQL_DATASOURCE_DATABASE_LIST)) // set captured database
                .tableList(config.getString(InitialConstants.MYSQL_DATASOURCE_TABLE_LIST)) // set captured table
                .username(config.getString(InitialConstants.MYSQL_DATASOURCE_USERNAME))
                .password(config.getString(InitialConstants.MYSQL_DATASOURCE_PASSWORD))
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
    }

}
