package com.wizard.warehouse.realtime.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;

/**
 * the object represents a element of the stream
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataBean {
    private Integer id;

    private String deviceId;

    private String guid;

    private String account;

    private String appId;

    private String appVersion;

    private String carrier;

    private String deviceType;

    private String eventId;

    private String ip;

    private Double latitude;

    private Double longitude;

    private String netType;

    private String osName;

    private String osVersion;

    private String releaseChannel;

    private String resolution;

    private String sessionId;

    private Long timestamp;

    private String newSessionId;

    private String country;

    private String province;

    private String city;

    private String region;

    private HashMap<String, Object> properties;

    private Long lastUpdate;

    private int isNew;
}
