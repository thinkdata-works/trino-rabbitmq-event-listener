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

import java.util.List;
import java.util.Map;


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
            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new JSR310Module())
                    .registerModule(new Jdk8Module());
            return mapper.writeValueAsBytes(
                    constructPayload(config.getPayloadParentKeys(), config.getCustomProperties(), val)
            );
        } catch (JsonProcessingException e) {
            // TODO - get logging working
            System.out.println("Got error parsing object to json: " + e.getMessage());
            System.out.println("Payload: " + val.toString());
            throw new PublicationException("Got JSON processing error writing " + val);
        }
    }

    static Payload constructPayload(List<String> allKeys, Map<String, String> customProps, Object val) {
        Payload rootpayload = new Payload();
        Payload lastchild = rootpayload;

        for(int i = 0; i < allKeys.size(); i++) {
            if (i == allKeys.size() - 1) {
                // If we are on the last element, append the input payload
                lastchild.getPayload().put(allKeys.get(i), val);
                // And add our custom properties
                for(Map.Entry<String, String> prop : customProps.entrySet()) {
                    lastchild.getPayload().put(prop.getKey(), prop.getValue());
                }
            } else {
                // Otherwise create a new payload and move the parent chain
                Payload newPayload = new Payload();
                lastchild.getPayload().put(allKeys.get(i), newPayload);
                lastchild = newPayload;
            }
        }

        return rootpayload;
    }
}