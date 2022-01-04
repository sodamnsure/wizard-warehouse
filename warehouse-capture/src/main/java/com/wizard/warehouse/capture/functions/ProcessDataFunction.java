package com.wizard.warehouse.capture.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wizard.warehouse.capture.beans.LoadTableConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;

/**
 * @Author: sodamnsure
 * @Date: 2021/11/29 10:11 上午
 * @Desc: 处理"Change Data Capture"函数
 */
public class ProcessDataFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private MapStateDescriptor<String, LoadTableConfig> mapStateDescriptor;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

    }

    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        // 获取并解析数据
        JSONObject jsonObject = JSON.parseObject(s);
        String data = jsonObject.getString("after");
        LoadTableConfig loadTableConfig = JSON.parseObject(data, LoadTableConfig.class);

        // 建表
        if (LoadTableConfig.SINK_TYPE_DORIS.equals(loadTableConfig.getSinkType())) {

        }
    }
}
