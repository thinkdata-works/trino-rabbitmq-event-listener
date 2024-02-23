package com.tdw.trino.eventlistener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import com.tdw.trino.rabbitmq.PublicationException;
import com.tdw.trino.rabbitmq.RabbitmqClient;
import com.tdw.trino.serialized.Payload;
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
        this.client = new RabbitmqClient(config.getUrl(), config.getExchangeName(), config.getExchangeType(), config.isDurableExchange());
    }

    // TODO - move all publication to a future where we can capture the exception without interruption
    @Override
    public void queryCompleted(final QueryCompletedEvent queryCompletedEvent) {
        if (config.shouldPublishQueryCompleted()) {
            client.Publish(this.config.getQueryCompletedQueues(), serializePayload(queryCompletedEvent));
        }
    }

    @Override
    public void queryCreated(final QueryCreatedEvent queryCreatedEvent) {
        LOGGER.info("Received query created event");
        if (config.shouldPublishQueryCreated()) {
            client.Publish(this.config.getQueryCreatedQueues(), serializePayload(queryCreatedEvent));
        }
    }

    @Override
    public void splitCompleted(final SplitCompletedEvent splitCompletedEvent) {
        if (config.shouldPublishSplitCompleted()) {
            client.Publish(this.config.getSplitCompletedQueues(), serializePayload(splitCompletedEvent));
        }
    }

    // For debugging
    private byte[] serializePayload(Object val) throws PublicationException {
        try {
            Payload p = new Payload();
            p.set(config.getPayloadParentKey(), val);
            config.getCustomProperties().entrySet().forEach(prop -> {
                p.set(prop.getKey(), prop.getValue());
            });

            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new JSR310Module())
                    .registerModule(new Jdk8Module());
            return mapper.writeValueAsBytes(p);
        } catch (JsonProcessingException e) {
            // TODO - get logging working
            System.out.println("Got error parsing object to json: " + e.getMessage());
            System.out.println("Payload: " + val.toString());
            throw new PublicationException("Got JSON processing error writing " + val);
        }
    }
}