package com.tdw.trino.eventlistener;

import java.util.Map;

public class QueryListenerConfig {
    // TODO require not null on url

    private String rabbitMQUrl;

    private static final String RABBIT_MQ_SERVER_URL = "rabbit-mq-server-url";

    public QueryListenerConfig(String rabbitMQUrl) {
        this.rabbitMQUrl = rabbitMQUrl;
    }

    public static QueryListenerConfig create(Map<String, String> config) {
        return new QueryListenerConfig(config.get(RABBIT_MQ_SERVER_URL));
    }

}
