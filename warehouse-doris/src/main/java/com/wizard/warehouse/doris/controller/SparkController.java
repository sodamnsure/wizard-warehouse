package com.wizard.warehouse.doris.controller;

import com.wizard.warehouse.doris.domain.DataBaseExtractorVo;
import com.wizard.warehouse.doris.domain.Result;
import com.wizard.warehouse.doris.service.ISparkSubmitService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * @Author: sodamnsure
 * @Date: 2023/3/3 9:52 AM
 * @Desc: SparkController
 */
@Slf4j
@Controller
public class SparkController {
    @Resource
    private ISparkSubmitService iSparkSubmitService;
    /**
     * 调用service进行远程提交spark任务
     * @param vo 页面参数
     * @return 执行结果
     */
    @ResponseBody
    @PostMapping("/extract/database")
    public Object dbExtractAndLoad2Oracle(@RequestBody DataBaseExtractorVo vo){
        try {
            return iSparkSubmitService.submitApplication(vo.getSparkApplicationParam(),
                    vo.getUrl(),
                    vo.getDriver(),
                    vo.getTable(),
                    vo.getUserName(),
                    vo.getPassword());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            log.error("执行出错：{}", e.getMessage());
            return Result.err(500, e.getMessage());
        }
    }

}
