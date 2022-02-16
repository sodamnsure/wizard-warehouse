package com.wizard.warehouse.realtime.jobs;

import com.wizard.warehouse.realtime.pojo.DataBean;
import com.wizard.warehouse.realtime.udf.JsonToBeanFunc;
import com.wizard.warehouse.realtime.utils.FlinkUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 *
 */
public class LiveAudienceCount {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(args, SimpleStringSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = kafkaStream.process(new JsonToBeanFunc());

        SingleOutputStreamOperator<DataBean> liveDataStream = beanStream.filter(new FilterFunction<DataBean>() {
            @Override
            public boolean filter(DataBean value) throws Exception {
                return value.getEventId().startsWith("live");
            }
        });

        // count the cumulative number of viewers of each anchor
        KeyedStream<DataBean, String> keyByAnchorStream = liveDataStream.keyBy(bean -> bean.getProperties().get("anchor_id").toString());


        // count the real-time online number of each anchor



        FlinkUtils.env.execute();

    }
}
