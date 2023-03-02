package com.example.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @Author: sodamnsure
 * @Date: 2023/3/1 4:51 PM
 * @Desc:
 */
@Component
public class MyComponent {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyComponent.class);

    @Value("${my.property}")
    private String myProperty;

    public void doSomething() {
        LOGGER.info("My property: {}", myProperty);
    }

}