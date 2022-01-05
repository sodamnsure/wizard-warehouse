package com.wizard.warehouse.capture.constant;

/**
 * @Author: sodamnsure
 * @Date: 2021/8/18 12:15 下午
 * @Desc: 配置文件，参数名称统一管理类
 */
public class InitialConstants {
    /*
      kafka config
     */
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static final String KAFKA_AUTO_OFFSET_RESET = "kafka.auto.offset.reset";
    public static final String KAFKA_CANAL_DETAIL_TOPIC = "kafka.canal_detail.topic";

    /*
      mysql config
     */
    public static final String MYSQL_DATASOURCE_HOSTNAME = "mysql.datasource.hostname";
    public static final String MYSQL_DATASOURCE_PORT = "mysql.datasource.port";
    public static final String MYSQL_DATASOURCE_DATABASE_LIST = "mysql.datasource.databaseList";
    public static final String MYSQL_DATASOURCE_TABLE_LIST = "mysql.datasource.tableList";
    public static final String MYSQL_DATASOURCE_USERNAME = "mysql.datasource.username";
    public static final String MYSQL_DATASOURCE_PASSWORD = "mysql.datasource.password";

    /*
      doris config
     */
    public static final String DORIS_OPTIONS_FE_NODES = "doris.options.fe.nodes";
    public static final String DORIS_OPTIONS_TABLE = "doris.options.table";
    public static final String DORIS_OPTIONS_USERNAME = "doris.options.username";
    public static final String DORIS_OPTIONS_PASSWORD = "doris.options.password";
    public static final String DORIS_EXECUTION_BATCH_SIZE = "doris.execution.batch_size";
    public static final String DORIS_EXECUTION_BATCH_INTERVAL = "doris.execution.batch_interval";
    public static final String DORIS_EXECUTION_MAX_RETRIES = "doris.execution.max_retries";
    public static final String DORIS_EXECUTION_PROP_FORMAT = "doris.execution.prop.format";
    public static final String DORIS_EXECUTION_PROP_STRIP_OUTER_ARRAY = "doris.execution.prop.strip_outer_array";
    public static final String DORIS_EXECUTION_PROP_SEQUENCE_COL = "doris.execution.prop.sequence_col";


}
