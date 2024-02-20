package com.tdw.trino.eventlistener;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RabbitmqEventListener implements EventListener {
    private static final Logger LOGGER = LogManager.getLogger(RabbitmqEventListener.class);
    private RabbitmqEventListenerConfig config;

    public RabbitmqEventListener(RabbitmqEventListenerConfig config) {
        this.config = config;
        // TODO - configure rabbitmq client
    }

    @Override
    public void queryCompleted(final QueryCompletedEvent queryCompletedEvent) {
        LOGGER.info("Received query event: " + queryCompletedEvent.toString());
        if (!config.publishQueryCreated()) {
            // TODO - log
            return;
        }
    }

    @Override
    public void queryCreated(final QueryCreatedEvent queryCreatedEvent) {
        LOGGER.info("Received query created event: " + queryCreatedEvent.toString());
        if (!config.publishQueryCreated()) {
            // TODO - log
            return;
        }
    }

    @Override
    public void splitCompleted(final SplitCompletedEvent splitCompletedEvent) {
        LOGGER.info("Received split created event: " + splitCompletedEvent.toString());
        if (!config.publishSplitCompleted()) {
            // TODO - log
            return;
        }
    }
}