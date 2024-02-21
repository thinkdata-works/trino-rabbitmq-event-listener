package com.tdw.trino.eventlistener;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// TODO - add support for routing key?

public class RabbitmqEventListenerConfig {
    private String url;
    private String exchangeName;
    private String exchangeType;
    private boolean durableExchange;
    private Set<String> publishOnQueues;
    private boolean publishQueryCreated;
    private boolean publishQueryFinished;
    private boolean publishSplitCompleted;

    private static final String RABBITMQ_SERVER_URL = "rabbitmq-server-url";
    private static final String RABBITMQ_EXCHANGE_NAME = "rabbitmq-exchange-name";
    private static final String RABBITMQ_EXCHANGE_TYPE = "rabbitmq-exchange-type";
    private static final String RABBITMQ_DURABLE_EXCHANGE = "rabbitmq-durable-exchange";
    private static final String RABBITMQ_PUBLISH_QUEUES = "rabbitmq-publish-queues";
    private static final String RABBITMQ_PUBLISH_QUERY_CREATED = "rabbitmq-publish-query-created";
    private static final String RABBITMQ_PUBLISH_QUERY_FINISHED = "rabbitmq-publish-query-finished";
    private static final String RABBITMQ_PUBLISH_SPLIT_COMPLETED = "rabbitmq-publish-split-completed";

    public static class Builder {
        // required params
        private String url;
        private String exchangeName;
        private String queueNames;


        // defaulted params
        private String exchangeType;
        private boolean durableExchange;
        private boolean publishQueryCreated;
        private boolean publishQueryFinished;
        private boolean publishSplitCompleted;

        public Builder(String url, String exchangeName, String queueNames, String exchangeType) {
            // Assign values
            this.url = url;
            this.exchangeName = exchangeName;
            this.queueNames = queueNames;
            this.exchangeType = exchangeType;

            // Assign defaults
            this.durableExchange = false;
            this.publishQueryCreated = false;
            this.publishQueryFinished = false;
            this.publishSplitCompleted = false;
        }

        public Builder setExchangeType(String exchangeType) {
            this.exchangeType = exchangeType;
            return this;
        }

        public Builder setDurableExchange(boolean durableExchange) {
            this.durableExchange = durableExchange;
            return this;
        }

        public Builder setPublishQueryCreated(boolean publishQueryCreated) {
            this.publishQueryCreated = publishQueryCreated;
            return this;
        }

        public Builder setPublishQueryFinished(boolean publishQueryFinished) {
            this.publishQueryFinished = publishQueryFinished;
            return this;
        }

        public Builder setPublishSplitCompleted(boolean publishSplitCompleted) {
            this.publishSplitCompleted = publishSplitCompleted;
            return this;
        }

        public RabbitmqEventListenerConfig Build() throws IllegalArgumentException {
            // Ensure that at least 1 queue is supplied
            Set<String> queueNames = Arrays.stream(this.queueNames.split(",")).map(String::strip).collect(Collectors.toSet());
            if (queueNames.size() < 1) {
                throw new IllegalArgumentException("Configuration must provide at least 1 queue name");
            }

            return new RabbitmqEventListenerConfig(
                this.url, this.exchangeName, this.exchangeType, queueNames, this.durableExchange,
                    this.publishQueryCreated, this.publishQueryFinished, this.publishSplitCompleted
            );
        }
     }

    private RabbitmqEventListenerConfig(
            String url,
            String exchangeName,
            String exchangeType,
            Set<String> queueNames,
            boolean durableExchange,
            boolean publishQueryCreated,
            boolean publishQueryFinished,
            boolean publishSplitCompleted
    ) {
        this.url = url;
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.publishOnQueues = queueNames;
        this.durableExchange = durableExchange;
        this.publishQueryCreated = publishQueryCreated;
        this.publishQueryFinished = publishQueryFinished;
        this.publishSplitCompleted = publishSplitCompleted;
    }

    public static RabbitmqEventListenerConfig create(Map<String, String> config) throws IllegalArgumentException {
        // Extract and create builder
        RabbitmqEventListenerConfig.Builder builder = new Builder(
                config.get(RABBITMQ_SERVER_URL),
                config.get(RABBITMQ_EXCHANGE_NAME),
                config.get(RABBITMQ_PUBLISH_QUEUES),
                config.get(RABBITMQ_EXCHANGE_TYPE)
        );

        

        return builder.Build();
    }

    public boolean shouldPublishQueryCreated() {
        return publishQueryCreated;
    }

    public boolean shouldPublishQueryFinished() {
        return publishQueryFinished;
    }

    public boolean shouldPublishSplitCompleted() {
        return publishSplitCompleted;
    }
}
