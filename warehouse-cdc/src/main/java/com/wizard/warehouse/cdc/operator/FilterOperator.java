package com.wizard.warehouse.cdc.operator;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * @Author: sodamnsure
 * @Date: 2021/11/19 4:26 下午
 * @Desc: Json对象过滤器
 */
public class FilterOperator {
    public static boolean filterType(JsonNode jsonNode) {
        // 获取是否是DDL语句
        String isDdl = jsonNode.get("isDdl").toString();
        // 获取操作类型
        String type = jsonNode.get("type").toString().replace("\"", "");
        // 过滤DDL语句和删除操作
        return !isDdl.equals("true") && !type.equals("DELETE");
    }
}
