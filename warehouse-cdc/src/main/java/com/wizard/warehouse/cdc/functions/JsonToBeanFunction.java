package com.wizard.warehouse.cdc.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;


/**
 * @Author: sodamnsure
 * @Date: 2021/8/19 2:54 下午
 * @Desc: Json转Bean对象
 */
public class JsonToBeanFunction implements MapFunction<String, JSONObject> {
    @Override
    public JSONObject map(String json) {
        return JSON.parseObject(json, JSONObject.class);
    }
}
