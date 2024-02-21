package com.tdw.trino.eventlistener;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// TODO - add support for routing key?

public class RabbitmqEventListenerConfig {
    public String getUrl() {
        return url;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public String getExchangeType() {
        return exchangeType;
    }

    public boolean isDurableExchange() {
        return durableExchange;
    }

    public Set<String> getPublishQueues() {
        return publishQueues;
    }

    private String url;
    private String exchangeName;
    private String exchangeType;
    private boolean durableExchange;
    private Set<String> publishQueues;
    private boolean publishQueryCreated;
    private boolean publishQueryCompleted;
    private boolean publishSplitCompleted;

    private static final String RABBITMQ_SERVER_URL = "rabbitmq-server-url";
    private static final String RABBITMQ_EXCHANGE_NAME = "rabbitmq-exchange-name";
    private static final String RABBITMQ_EXCHANGE_TYPE = "rabbitmq-exchange-type";
    private static final String RABBITMQ_DURABLE_EXCHANGE = "rabbitmq-durable-exchange";
    private static final String RABBITMQ_PUBLISH_QUEUES = "rabbitmq-publish-queues";
    private static final String RABBITMQ_PUBLISH_QUERY_CREATED = "rabbitmq-publish-query-created";
    private static final String RABBITMQ_PUBLISH_QUERY_COMPLETED = "rabbitmq-publish-query-completed";
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
        private boolean publishQueryCompleted;
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
            this.publishQueryCompleted = false;
            this.publishSplitCompleted = false;
        }

        public Builder setDurableExchange(boolean durableExchange) {
            this.durableExchange = durableExchange;
            return this;
        }

        public Builder setPublishQueryCreated(boolean publishQueryCreated) {
            this.publishQueryCreated = publishQueryCreated;
            return this;
        }

        public Builder setPublishQueryCompleted(boolean publishQueryCompleted) {
            this.publishQueryCompleted = publishQueryCompleted;
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
                    this.publishQueryCreated, this.publishQueryCompleted, this.publishSplitCompleted
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
            boolean publishQueryCompleted,
            boolean publishSplitCompleted
    ) {
        this.url = url;
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.publishQueues = queueNames;
        this.durableExchange = durableExchange;
        this.publishQueryCreated = publishQueryCreated;
        this.publishQueryCompleted = publishQueryCompleted;
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

        builder.setDurableExchange(parseBoolFromConfigValue(config.get(RABBITMQ_DURABLE_EXCHANGE), false));
        builder.setPublishQueryCreated(parseBoolFromConfigValue(config.get(RABBITMQ_PUBLISH_QUERY_CREATED), false));
        builder.setPublishQueryCompleted(parseBoolFromConfigValue(config.get(RABBITMQ_PUBLISH_QUERY_COMPLETED), false));
        builder.setPublishSplitCompleted(parseBoolFromConfigValue(config.get(RABBITMQ_PUBLISH_SPLIT_COMPLETED), false));

        return builder.Build();
    }

    private static boolean parseBoolFromConfigValue(String value, boolean defaultValue) {
        return Optional.ofNullable(value).map(Boolean::parseBoolean).orElse(defaultValue);
    }

    public boolean shouldPublishQueryCreated() {
        return publishQueryCreated;
    }

    public boolean shouldPublishQueryCompleted() {
        return publishQueryCompleted;
    }

    public boolean shouldPublishSplitCompleted() {
        return publishSplitCompleted;
    }
}
