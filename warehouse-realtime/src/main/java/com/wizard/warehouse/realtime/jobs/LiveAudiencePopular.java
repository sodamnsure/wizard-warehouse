package com.wizard.warehouse.realtime.jobs;

import com.wizard.warehouse.realtime.pojo.DataBean;
import com.wizard.warehouse.realtime.udf.JsonToBeanFunc;
import com.wizard.warehouse.realtime.utils.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *
 */
public class LiveAudiencePopular {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(args, SimpleStringSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = kafkaStream.process(new JsonToBeanFunc());

        KeyedStream<DataBean, Tuple2<String, String>> keyed = beanStream.keyBy(new KeySelector<DataBean, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(DataBean value) throws Exception {
                String deviceId = value.getDeviceId();
                String anchorId = value.getProperties().get("anchor_id").toString();
                return Tuple2.of(anchorId, deviceId);
            }
        });


        keyed.process(new KeyedProcessFunction<Tuple2<String, String>, DataBean, Tuple2<String, Integer>>() {
            private transient MapState<String, Long> mapState;
            private transient ValueState<Long> tsState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, Long.class);
                mapState = getRuntimeContext().getMapState(mapStateDescriptor);

                ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("ts-state", Long.class);
                tsState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(DataBean bean, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Long timestamp = bean.getTimestamp();
                String liveSession = bean.getProperties().get("live_session").toString();

                String eventId = bean.getEventId();

                if ("liveEnter".equals(eventId)) {
                    mapState.put(liveSession, timestamp);
                    // when the system time exceeds the value, the timer is triggered
                    ctx.timerService().registerProcessingTimeTimer(timestamp + 60000 + 1);
                } else if ("liveLeave".equals(eventId)) {
                    Long ts = mapState.get(liveSession);
                    if (ts != null) {
                        mapState.remove(liveSession);
                        ctx.timerService().deleteProcessingTimeTimer(ts + 60000 + 1);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            }
        });


        FlinkUtils.env.execute();

    }
}
