package com.wizard.warehouse.realtime.udf;

import com.wizard.warehouse.realtime.pojo.DataBean;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Real-time statistics of the number of viewers and times of anchors in each live broadcast room
 */
public class AnchorDistinctTotalAudienceFunc extends KeyedProcessFunction<String, DataBean, Tuple3<String, Integer, Integer>> {
    private transient ValueState<Integer> uvState;
    private transient ValueState<Integer> pvState;
    private transient ValueState<BloomFilter<String>> bloomFilterState;


    @Override
    public void open(Configuration parameters) throws Exception {
        // configuration of state TTL logic.
        StateTtlConfig stateTtlConfig = StateTtlConfig
                .newBuilder(Time.hours(6))
                // last access timestamp is initialised when state is created and updated on every write operation.
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // Never return expired user value.
                .neverReturnExpired()
                .build();

        // creates a new ValueStateDescriptor to count the cumulative number of viewers of each anchor
        ValueStateDescriptor<Integer> uvStateDescriptor = new ValueStateDescriptor<>("uv-state", Integer.class);
        uvStateDescriptor.enableTimeToLive(stateTtlConfig);
        // creates a new ValueStateDescriptor to count the real-time online number of each anchor
        ValueStateDescriptor<Integer> pvStateDescriptor = new ValueStateDescriptor<>("pv-state", Integer.class);
        pvStateDescriptor.enableTimeToLive(stateTtlConfig);
        //
        ValueStateDescriptor<BloomFilter<String>> bloomFilterStateDescriptor = new ValueStateDescriptor<>("bloom-filter-state", TypeInformation.of(new TypeHint<BloomFilter<String>>() {
        }));
        bloomFilterStateDescriptor.enableTimeToLive(stateTtlConfig);

        uvState = getRuntimeContext().getState(uvStateDescriptor);
        pvState = getRuntimeContext().getState(pvStateDescriptor);
        bloomFilterState = getRuntimeContext().getState(bloomFilterStateDescriptor);


    }

    @Override
    public void processElement(DataBean bean, KeyedProcessFunction<String, DataBean, Tuple3<String, Integer, Integer>>.Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
        String deviceId = bean.getDeviceId();
        BloomFilter<String> bloomFilter = bloomFilterState.value();

        if (bloomFilterState == null) {
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 1000000);
        }

    }
}
