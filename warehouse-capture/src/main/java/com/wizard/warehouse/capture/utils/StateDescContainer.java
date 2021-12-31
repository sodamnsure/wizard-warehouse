package com.wizard.warehouse.capture.utils;

import com.wizard.warehouse.capture.beans.LoadTableConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;

/**
 * @Author: sodamnsure
 * @Date: 2021/12/30 4:14 PM
 * @Desc: 状态统一管理类
 */
public class StateDescContainer {
    /**
     * 配置表的状态描述
     *
     * @return 主键+表配置
     */
    public static MapStateDescriptor<String, LoadTableConfig> getConfigState() {
        return new MapStateDescriptor<>("config_state", String.class, LoadTableConfig.class);
    }
}
