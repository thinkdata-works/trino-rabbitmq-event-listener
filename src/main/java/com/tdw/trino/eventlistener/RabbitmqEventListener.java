package com.tdw.trino.eventlistener;

import com.tdw.trino.rabbitmq.RabbitmqClient;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RabbitmqEventListener implements EventListener {
    private static final Logger LOGGER = LogManager.getLogger(RabbitmqEventListener.class);
    private RabbitmqEventListenerConfig config;
    private RabbitmqClient client;

    public RabbitmqEventListener(RabbitmqEventListenerConfig config) {
        this.config = config;
        this.client = new RabbitmqClient(config);
    }

    @Override
    public void queryCompleted(final QueryCompletedEvent queryCompletedEvent) {
        LOGGER.info("Received query event: " + queryCompletedEvent.toString());
        if (!config.shouldPublishQueryCompleted()) {
            // TODO - log
            return;
        }
    }

    @Override
    public void queryCreated(final QueryCreatedEvent queryCreatedEvent) {
        LOGGER.info("Received query created event: " + queryCreatedEvent.toString());
        if (!config.shouldPublishQueryCreated()) {
            // TODO - log
            return;
        }
    }

    @Override
    public void splitCompleted(final SplitCompletedEvent splitCompletedEvent) {
        LOGGER.info("Received split created event: " + splitCompletedEvent.toString());
        if (!config.shouldPublishSplitCompleted()) {
            // TODO - log
            return;
        }
    }
}