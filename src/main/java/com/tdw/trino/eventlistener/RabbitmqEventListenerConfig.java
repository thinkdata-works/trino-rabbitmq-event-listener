package com.tdw.trino.eventlistener;

import java.util.Map;

public class RabbitmqEventListenerConfig {
    // TODO require not null on url

    private String rabbitMQUrl;

    private static final String RABBIT_MQ_SERVER_URL = "rabbit-mq-server-url";

    public RabbitmqEventListenerConfig(String rabbitMQUrl) {
        this.rabbitMQUrl = rabbitMQUrl;
    }

    public static RabbitmqEventListenerConfig create(Map<String, String> config) {
        return new RabbitmqEventListenerConfig(config.get(RABBIT_MQ_SERVER_URL));
    }

}
