package com.wizard.warehouse.capture.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: sodamnsure
 * @Date: 2021/12/29 5:03 PM
 * @Desc: 导入配置表
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoadTableConfig {
    /**
     * 表主键
     */
    private String id;

    /**
     * 来源表
     */
    private String sourceTable;

    /**
     * 操作类型
     */
    private String operateType;

    /**
     * 输出类型
     */
    private String sinkType;

    /**
     * 输出表
     */
    private String sinkTable;

    /**
     * 输出字段
     */
    private String sinkColumns;

    /**
     * 主键字段
     */
    private String sinkPk;

    /**
     * 建表扩展
     */
    private String sinkExtend;

    /**
     * 创建时间
     */
    private String createTime;

    /**
     * 修改时间
     */
    private String updateTime;

    /**
     * 当前用户配置是否生效 1 生效，0 失效
     */
    private String status;
}
