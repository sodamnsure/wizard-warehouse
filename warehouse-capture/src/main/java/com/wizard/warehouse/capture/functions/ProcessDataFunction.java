package com.wizard.warehouse.capture.functions;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: sodamnsure
 * @Date: 2021/11/29 10:11 上午
 * @Desc: 处理"Change Data Capture"函数
 */
public class ProcessDataFunction extends ProcessFunction<JSONObject, String> {
    @Override
    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
        // 过滤无效数据
        String type = jsonObject.getString("type");
        if (type.equals("INSERT") || type.equals("UPDATE")) {
            JSONArray jsonArray = jsonObject.getJSONArray("data");
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject object = jsonArray.getObject(i, JSONObject.class);
                collector.collect(object.toString());
            }
        }
    }
}
