package com.wizard.warehouse.capture.main;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.wizard.warehouse.capture.functions.*;
import com.wizard.warehouse.capture.utils.StateDescContainer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @Author: sodamnsure
 * @Date: 2021/11/18 2:10 下午
 * @Desc: 主类
 */
public class Main {
    public static void main(String[] args) throws Exception {
        /*
          构建env
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        /*
          读取canal采集的数据
         */
        KafkaSourceBuilder kafkaSourceBuilder = new KafkaSourceBuilder();
        DataStream<String> dataStream = env.addSource(kafkaSourceBuilder.build());

        /*
          json解析
         */
        SingleOutputStreamOperator<JSONObject> jsonDStream = dataStream.map(new JsonToBeanFunction());

        /*
          使用flink cdc消费配置表并处理成广播流
         */
        MySqlSourceBuilder mySqlSourceBuilder = new MySqlSourceBuilder();
        MySqlSource<String> mySqlSource = mySqlSourceBuilder.build();
        DataStreamSource<String> tableConfDStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        BroadcastStream<String> broadcastStream = tableConfDStream.broadcast(StateDescContainer.getConfigState());

        /*
          链接主流与广播流
         */
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonDStream.connect(broadcastStream);

        /*
          处理"change data capture"
         */
        SingleOutputStreamOperator<JSONObject> resultStream = connectedStream.process(new ProcessDataFunction());

        resultStream.print();

        DorisSinkBuilder dorisSinkBuilder = new DorisSinkBuilder();
        SinkFunction<JSONObject> sinkFunction = dorisSinkBuilder.build();

        resultStream.addSink(sinkFunction);

        env.execute();
    }

}
