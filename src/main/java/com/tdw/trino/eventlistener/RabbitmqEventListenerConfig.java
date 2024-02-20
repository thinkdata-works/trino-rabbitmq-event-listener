package com.tdw.trino.eventlistener;

import java.util.List;
import java.util.Map;

public class RabbitmqEventListenerConfig {
    // TODO require not null on url

    private String rabbitmqUrl;
    private String rabbitmqExchangeName;
    private String rabbitmqExchangeType;
    private String rabbitmqDurableExchange;
    private List<String> rabbitmqPublishOnQueues;
    private boolean rabbitmqPublishQueryCreated;
    private boolean rabbitmqPublishQueryFinished;
    private boolean rabbitmqPublishSplitCreated;

    private static final String RABBITMQ_SERVER_URL = "rabbitmq-server-url";

    public RabbitmqEventListenerConfig(String rabbitmqUrl) {
        this.rabbitmqUrl = rabbitmqUrl;
    }

    public static RabbitmqEventListenerConfig create(Map<String, String> config) {
        return new RabbitmqEventListenerConfig(config.get(RABBITMQ_SERVER_URL));
    }

}
