package com.wizard.warehouse.cdc.operator;

import com.alibaba.fastjson.JSONObject;

/**
 * @Author: sodamnsure
 * @Date: 2021/11/19 4:26 下午
 * @Desc: Json对象过滤器
 */
public class JsonOperator {
    public static boolean filterType(JSONObject jsonObject) throws Exception {
        // 获取操作类型
        String type = jsonObject.getString("type");
        // 只留下插入和更新操作
        return type.equals("INSERT") || type.equals("UPDATE");
    }
}
