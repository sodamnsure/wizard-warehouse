package com.wizard.warehouse.doris.service;

import com.wizard.warehouse.doris.domain.SparkApplicationParam;

import java.io.IOException;

/**
 * @Author: sodamnsure
 * @Date: 2023/3/3 10:05 AM
 * @Desc:
 */
public interface ISparkSubmitService {
    /**
     * 提交spark任务入口
     *
     * @param sparkAppParams spark任务运行所需参数
     * @param otherParams    单独的job所需参数
     * @return 结果
     * @throws IOException          io
     * @throws InterruptedException 线程等待中断异常
     */
    String submitApplication(SparkApplicationParam sparkAppParams, String... otherParams) throws IOException, InterruptedException;
}
