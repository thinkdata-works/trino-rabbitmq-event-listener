package com.tdw.trino.eventlistener;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;

public class RabbitmqEventListenerFactory implements EventListenerFactory {
    @Override
    public String getName() {
        return "rabbitmq";
    }

    @Override
    public EventListener create(Map<String, String> config) {
        var listenerConfig = RabbitmqEventListenerConfig.create(config);
        return new RabbitmqEventListener(listenerConfig);
    }
}
