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

    public Set<String> getQueryCreatedQueues() {
        return queryCreatedQueues;
    }

    public Set<String> getQueryCompletedQueues() {
        return queryCompletedQueues;
    }

    public Set<String> getSplitCompletedQueues() {
        return splitCompletedQueues;
    }

    private String url;
    private String exchangeName;
    private String exchangeType;
    private boolean durableExchange;
    private Set<String> queryCreatedQueues;
    private Set<String> queryCompletedQueues;
    private Set<String> splitCompletedQueues;
    private boolean publishQueryCreated;
    private boolean publishQueryCompleted;
    private boolean publishSplitCompleted;

    private static final String RABBITMQ_SERVER_URL = "rabbitmq-server-url";
    private static final String RABBITMQ_EXCHANGE_NAME = "rabbitmq-exchange-name";
    private static final String RABBITMQ_EXCHANGE_TYPE = "rabbitmq-exchange-type";
    private static final String RABBITMQ_DURABLE_EXCHANGE = "rabbitmq-durable-exchange";

    private static final String RABBITMQ_PUBLISH_QUERY_CREATED = "rabbitmq-publish-query-created";
    private static final String RABBITMQ_QUERY_CREATED_QUEUES = "rabbitmq-query-created-queues";

    private static final String RABBITMQ_PUBLISH_QUERY_COMPLETED = "rabbitmq-publish-query-completed";
    private static final String RABBITMQ_QUERY_COMPLETED_QUEUES = "rabbitmq-query-completed-queues";

    private static final String RABBITMQ_PUBLISH_SPLIT_COMPLETED = "rabbitmq-publish-split-completed";
    private static final String RABBITMQ_SPLIT_COMPLETED_QUEUES = "rabbitmq-split-completed-queues";

    public static class Builder {
        // required params
        private String url;
        private String exchangeName;


        // defaulted params
        private String exchangeType;
        private boolean durableExchange;
        private boolean publishQueryCreated;
        private String queryCreatedQueues;
        private boolean publishQueryCompleted;
        private String queryCompletedQueues;
        private boolean publishSplitCompleted;
        private String splitCompletedQueues;

        public Builder(String url, String exchangeName, String exchangeType) {
            // Assign values
            this.url = url;
            this.exchangeName = exchangeName;
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

        public Builder setPublishQueryCreated(boolean publishQueryCreated, String queryCreatedQueues) {
            this.publishQueryCreated = publishQueryCreated;
            this.queryCreatedQueues = queryCreatedQueues;
            return this;
        }

        public Builder setPublishQueryCompleted(boolean publishQueryCompleted, String queryCompletedQueues) {
            this.publishQueryCompleted = publishQueryCompleted;
            this.queryCompletedQueues = queryCompletedQueues;
            return this;
        }

        public Builder setPublishSplitCompleted(boolean publishSplitCompleted, String splitCompletedQueues) {
            this.publishSplitCompleted = publishSplitCompleted;
            this.splitCompletedQueues = splitCompletedQueues;
            return this;
        }

        public RabbitmqEventListenerConfig Build() throws IllegalArgumentException {
            // Split queue names and enforce argument exception based on boolean setting
            Set<String> queryCreatedQueueNames = Arrays.stream(this.queryCreatedQueues.split(",")).map(String::strip).collect(Collectors.toSet());
            if (this.publishQueryCreated && queryCreatedQueueNames.size() < 1) {
                throw new IllegalArgumentException("At least one queue name must be supplied for " + RABBITMQ_QUERY_CREATED_QUEUES);
            }

            Set<String> queryCompletedQueueNames = Arrays.stream(this.queryCompletedQueues.split(",")).map(String::strip).collect(Collectors.toSet());
            if (this.publishQueryCompleted && queryCompletedQueueNames.size() < 1) {
                throw new IllegalArgumentException("At least one queue name must be supplied for " + RABBITMQ_QUERY_COMPLETED_QUEUES);
            }

            Set<String> splitCompletedQueueNames = Arrays.stream(this.splitCompletedQueues.split(",")).map(String::strip).collect(Collectors.toSet());
            if (this.publishSplitCompleted && splitCompletedQueueNames.size() < 1) {
                throw new IllegalArgumentException("At least one queue name must be supplied for " + RABBITMQ_SPLIT_COMPLETED_QUEUES);
            }

            return new RabbitmqEventListenerConfig(
                this.url, this.exchangeName, this.exchangeType, queryCreatedQueueNames, queryCompletedQueueNames, splitCompletedQueueNames, this.durableExchange,
                    this.publishQueryCreated, this.publishQueryCompleted, this.publishSplitCompleted
            );
        }
     }

    private RabbitmqEventListenerConfig(
            String url,
            String exchangeName,
            String exchangeType,
            Set<String> queryCreatedQueueNames,
            Set<String> queryCompletedQueueNames,
            Set<String> splitCompletedQueueNames,
            boolean durableExchange,
            boolean publishQueryCreated,
            boolean publishQueryCompleted,
            boolean publishSplitCompleted
    ) {
        this.url = url;
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.queryCreatedQueues = queryCreatedQueueNames;
        this.queryCompletedQueues = queryCompletedQueueNames;
        this.splitCompletedQueues = splitCompletedQueueNames;
        this.durableExchange = durableExchange;
        this.publishQueryCreated = publishQueryCreated;
        this.publishQueryCompleted = publishQueryCompleted;
        this.publishSplitCompleted = publishSplitCompleted;
    }

    public static RabbitmqEventListenerConfig create(Map<String, String> config) {
        // Extract and create builder
        RabbitmqEventListenerConfig.Builder builder = new Builder(
                config.get(RABBITMQ_SERVER_URL),
                config.get(RABBITMQ_EXCHANGE_NAME),
                config.get(RABBITMQ_EXCHANGE_TYPE)
        );

        builder.setDurableExchange(parseBoolFromConfigValue(config.get(RABBITMQ_DURABLE_EXCHANGE), false));
        builder.setPublishQueryCreated(
                parseBoolFromConfigValue(config.get(RABBITMQ_PUBLISH_QUERY_CREATED), false),
                config.getOrDefault(RABBITMQ_QUERY_CREATED_QUEUES, "")
        );
        builder.setPublishQueryCompleted(
                parseBoolFromConfigValue(config.get(RABBITMQ_PUBLISH_QUERY_COMPLETED), false),
                config.getOrDefault(RABBITMQ_QUERY_COMPLETED_QUEUES, "")
        );
        builder.setPublishSplitCompleted(
                parseBoolFromConfigValue(config.get(RABBITMQ_PUBLISH_SPLIT_COMPLETED), false),
                config.getOrDefault(RABBITMQ_SPLIT_COMPLETED_QUEUES, "")
        );


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
