package com.wizard.warehouse.realtime.udf;

import com.alibaba.fastjson.JSON;
import com.wizard.warehouse.realtime.pojo.DataBean;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Parse JSON Strings into Java Objects
 */
public class JsonToBeanFunc extends ProcessFunction<String, DataBean> {

    @Override
    public void processElement(String value, ProcessFunction<String, DataBean>.Context ctx, Collector<DataBean> out) throws Exception {
        try {
            DataBean dataBean = JSON.parseObject(value, DataBean.class);
            out.collect(dataBean);
        } catch (Exception e) {
            // TODO
        }
    }
}
