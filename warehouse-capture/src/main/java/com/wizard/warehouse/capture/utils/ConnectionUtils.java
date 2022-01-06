package com.wizard.warehouse.capture.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.wizard.warehouse.capture.constant.InitialConstants;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @Author: sodamnsure
 * @Date: 2022/1/5 10:06 AM
 * @Desc: 客户端连接创建工具
 */
@Slf4j
public class ConnectionUtils {
    static Config config = ConfigFactory.load();

    /**
     * 获取MySql链接
     *
     * @return 返回MySql链接
     */
    public static Connection getMySqlConnection() throws Exception {
        log.debug("MySql连接准备创建...........");
        String driver = config.getString(InitialConstants.MYSQL_DATASOURCE_PORT);
        String url = config.getString("");

        Class.forName(driver);
        java.sql.Connection conn = DriverManager.getConnection(url);
        log.debug("创建MySql连接成功...........");
        return conn;
    }
}
