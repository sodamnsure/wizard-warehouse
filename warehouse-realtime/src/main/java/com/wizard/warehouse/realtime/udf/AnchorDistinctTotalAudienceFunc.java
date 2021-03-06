package com.wizard.warehouse.realtime.udf;

import com.wizard.warehouse.realtime.pojo.DataBean;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Int;

/**
 * Real-time statistics of the number of viewers and times of anchors in each live broadcast room
 */
public class AnchorDistinctTotalAudienceFunc extends KeyedProcessFunction<String, DataBean, Tuple4<String, Integer, Integer, Integer>> {
    private transient ValueState<Integer> uvState;
    private transient ValueState<Integer> pvState;
    private transient ValueState<Integer> onlineState;
    private transient ValueState<BloomFilter<String>> bloomFilterState;


    @Override
    public void open(Configuration parameters) throws Exception {
        // configuration of state TTL logic.
        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.hours(6))
                // last access timestamp is initialised when state is created and updated on every write operation.
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // Never return expired user value.
                .neverReturnExpired().build();

        // creates a new ValueStateDescriptor to count uv of each anchor
        ValueStateDescriptor<Integer> uvStateDescriptor = new ValueStateDescriptor<>("uv-state", Integer.class);
        uvStateDescriptor.enableTimeToLive(stateTtlConfig);
        // creates a new ValueStateDescriptor to count pv of each anchor
        ValueStateDescriptor<Integer> pvStateDescriptor = new ValueStateDescriptor<>("pv-state", Integer.class);
        pvStateDescriptor.enableTimeToLive(stateTtlConfig);
        // creates a new ValueStateDescriptor to count the real-time online number of each anchor
        ValueStateDescriptor<Integer> onlineStateDescriptor = new ValueStateDescriptor<>("online-state", Integer.class);
        onlineStateDescriptor.enableTimeToLive(stateTtlConfig);

        //
        ValueStateDescriptor<BloomFilter<String>> bloomFilterStateDescriptor = new ValueStateDescriptor<>("bloom-filter-state", TypeInformation.of(new TypeHint<BloomFilter<String>>() {
        }));
        bloomFilterStateDescriptor.enableTimeToLive(stateTtlConfig);

        uvState = getRuntimeContext().getState(uvStateDescriptor);
        pvState = getRuntimeContext().getState(pvStateDescriptor);
        bloomFilterState = getRuntimeContext().getState(bloomFilterStateDescriptor);
        onlineState = getRuntimeContext().getState(onlineStateDescriptor);

    }


    @Override
    public void processElement(DataBean bean, Context ctx, Collector<Tuple4<String, Integer, Integer, Integer>> out) throws Exception {
        String eventId = bean.getEventId();

        String deviceId = bean.getDeviceId();
        Integer uv = uvState.value();
        Integer pv = pvState.value();
        Integer onlineCounts = onlineState.value();
        BloomFilter<String> bloomFilter = bloomFilterState.value();

        if (onlineCounts == null) {
            onlineCounts = 0;
        }
        if ("liveEnter".equals(eventId)) {
            if (bloomFilterState == null) {
                uv = 0;
                pv = 0;
                onlineCounts = 0;
                bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 1000000);
            }

            if (!bloomFilter.mightContain(deviceId)) {
                uv++;
                uvState.update(uv);
                bloomFilter.put(deviceId);
                bloomFilterState.update(bloomFilter);
            }
            pv++;
            pvState.update(pv);
            onlineCounts++;
            onlineState.update(onlineCounts);
        } else {
            onlineCounts--;
            onlineState.update(onlineCounts);
        }

        out.collect(Tuple4.of(ctx.getCurrentKey(), uv, pv, onlineCounts));
    }
}
