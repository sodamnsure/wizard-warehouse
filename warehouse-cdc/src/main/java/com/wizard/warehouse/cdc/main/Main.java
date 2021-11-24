package com.wizard.warehouse.cdc.main;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wizard.warehouse.cdc.functions.JsonToBeanFunction;
import com.wizard.warehouse.cdc.functions.KafkaSourceBuilder;
import com.wizard.warehouse.cdc.operator.JsonOperator;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

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
        DataStream<String> stream = env.addSource(kafkaSourceBuilder.build("topic1"));

        /*
          Json解析
         */
        SingleOutputStreamOperator<JSONObject> jsonStream = stream.map(new JsonToBeanFunction());

        /*
          只读取插入和更新操作
         */
        SingleOutputStreamOperator<JSONObject> JsonFilter = jsonStream.filter((FilterFunction<JSONObject>) JsonOperator::filterType);

        SingleOutputStreamOperator<String> data = JsonFilter.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) {
                JSONArray jsonArray = jsonObject.getJSONArray("data");
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject object = jsonArray.getObject(i, JSONObject.class);
                    collector.collect(object.toString());
                }
            }
        });

        data.addSink(
                DorisSink.sink(
                        DorisOptions.builder()
                                .setFenodes("test:8030")
                                .setTableIdentifier("example_db.user")
                                .setUsername("test")
                                .setPassword("test").build()
                )
        );

        env.execute();
    }

}
