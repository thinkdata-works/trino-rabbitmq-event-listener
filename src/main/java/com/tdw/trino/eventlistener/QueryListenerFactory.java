package com.tdw.trino.eventlistener;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;

public class QueryListenerFactory implements EventListenerFactory {
    @Override
    public String getName() {
        return "tdwplatform";
    }

    @Override
    public EventListener create(Map<String, String> config) {
        var listenerConfig = QueryListenerConfig.create(config);
        return new QueryListener(listenerConfig);
    }
}
