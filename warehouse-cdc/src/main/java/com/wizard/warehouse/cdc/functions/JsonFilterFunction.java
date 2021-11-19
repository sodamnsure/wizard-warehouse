package com.wizard.warehouse.cdc.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.wizard.warehouse.cdc.operator.FilterOperator;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @Author: sodamnsure
 * @Date: 2021/11/19 5:14 下午
 * @Desc:
 */
public class JsonFilterFunction implements FilterFunction<JsonNode> {

    @Override
    public boolean filter(JsonNode jsonNode) {
        return FilterOperator.filterType(jsonNode);
    }
}
