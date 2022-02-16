package com.wizard.warehouse.realtime.udf;

import com.alibaba.fastjson.JSON;
import com.wizard.warehouse.realtime.pojo.DataBean;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Parse JSON Strings into Java Objects
 */
public class JsonToUniqBeanFunc extends ProcessFunction<Tuple2<String, String>, DataBean> {

    /**
     * Parse json into DataBean
     */
    @Override
    public void processElement(Tuple2<String, String> tp, ProcessFunction<Tuple2<String, String>, DataBean>.Context ctx, Collector<DataBean> out) throws Exception {
        try {
            // unique id : topic-partition-offset;
            String id = tp.f0;
            // deserializes json into DataBean
            DataBean dataBean = JSON.parseObject(tp.f1, DataBean.class);
            // set up id
            dataBean.setId(id);
            // emits a record.
            out.collect(dataBean);
        } catch (Exception e) {
            // e.printStackTrace();
            // TODO: problematic data can be saved
        }
    }
}
