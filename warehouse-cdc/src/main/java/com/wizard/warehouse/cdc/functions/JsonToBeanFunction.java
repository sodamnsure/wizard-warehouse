package com.wizard.warehouse.cdc.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.wizard.warehouse.cdc.utils.JsonParseUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: sodamnsure
 * @Date: 2021/8/19 2:54 下午
 * @Desc: Json转Bean对象
 */
public class JsonToBeanFunction implements MapFunction<String, JsonNode> {
    @Override
    public JsonNode map(String json) {
        return JsonParseUtils.parseObject(json, JsonNode.class);
    }
}
