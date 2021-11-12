package com.wizard.warehouse.realtime.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sodamnsure
 * @Date: 2021/11/12 4:48 下午
 * @Desc:
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("Pandeng8848")
                .databaseList("business")
                .tableList("business.crawlmonitor")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();

        env.execute("Flink CDC");

    }
}
