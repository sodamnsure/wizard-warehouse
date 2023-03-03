package com.wizard.warehouse.doris.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @Author: sodamnsure
 * @Date: 2023/3/3 9:52 AM
 * @Desc: SparkController
 */
@Controller
public class SparkController {
    @ResponseBody
    @PostMapping("/extract/database")
    public Object test(){
        return "hello";
    }

}
