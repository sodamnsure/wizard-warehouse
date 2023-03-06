package com.wizard.warehouse.doris.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: sodamnsure
 * @Date: 2023/3/6 2:59 PM
 * @Desc:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataBaseExtractorVo extends SparkApplicationParam {
    /**
     * Oracle数据库连接地址
     */
    private String url;
    /**
     * Oracle数据库驱动
     */
    private String driver;
    /**
     * 指定的表名
     */
    private String table;
    /**
     * Doris数据库连接账号
     */
    private String userName;
    /**
     * 数据库密码
     */
    private String password;
}
