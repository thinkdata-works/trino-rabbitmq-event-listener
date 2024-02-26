package com.tdw.trino.eventlistener;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

    public List<String> getPayloadParentKeys() {
        return this.payloadParentKeys;
    }

    public Map<String, String> getCustomProperties() {
        return this.customProperties;
    }

    private String url;
    private String exchangeName;
    private String exchangeType;
    private boolean durableExchange;
    private Set<String> queryCreatedQueues;
    private Set<String> queryCompletedQueues;
    private Set<String> splitCompletedQueues;
    private List<String> payloadParentKeys;
    private Map<String, String> customProperties;
    private boolean publishQueryCreated;
    private boolean publishQueryCompleted;
    private boolean publishSplitCompleted;

    private static final String SERVER_URL = "server-url";
    private static final String EXCHANGE_NAME = "exchange-name";
    private static final String EXCHANGE_TYPE = "exchange-type";
    private static final String DURABLE_EXCHANGE = "durable-exchange";

    private static final String PUBLISH_QUERY_CREATED = "publish-query-created";
    private static final String QUERY_CREATED_QUEUES = "query-created-queues";

    private static final String PUBLISH_QUERY_COMPLETED = "publish-query-completed";
    private static final String QUERY_COMPLETED_QUEUES = "query-completed-queues";

    private static final String PUBLISH_SPLIT_COMPLETED = "publish-split-completed";
    private static final String SPLIT_COMPLETED_QUEUES = "split-completed-queues";

    private static final String PAYLOAD_PARENT_KEYS = "payload-parent-keys";
    private static final String CUSTOM_PROPERTIES_PATTERN = "x-custom-";

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
        private Map<String, String> customProperties;
        private String payloadParentKeys;

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
            this.customProperties = new HashMap<>();
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

        public Builder setPayloadParentKeys(String payloadParentKeys) {
            this.payloadParentKeys = payloadParentKeys;
            return this;
        }

        public Builder addCustomProperty(String key, String value) {
            this.customProperties.put(key, value);
            return this;
        }

        public RabbitmqEventListenerConfig Build() throws IllegalArgumentException {
            // Split queue names and enforce argument exception based on boolean setting
            Set<String> queryCreatedQueueNames = Arrays.stream(this.queryCreatedQueues.split(",")).map(String::strip).collect(Collectors.toSet());
            if (this.publishQueryCreated && queryCreatedQueueNames.size() < 1) {
                throw new IllegalArgumentException("At least one queue name must be supplied for " + QUERY_CREATED_QUEUES);
            }

            Set<String> queryCompletedQueueNames = Arrays.stream(this.queryCompletedQueues.split(",")).map(String::strip).collect(Collectors.toSet());
            if (this.publishQueryCompleted && queryCompletedQueueNames.size() < 1) {
                throw new IllegalArgumentException("At least one queue name must be supplied for " + QUERY_COMPLETED_QUEUES);
            }

            Set<String> splitCompletedQueueNames = Arrays.stream(this.splitCompletedQueues.split(",")).map(String::strip).collect(Collectors.toSet());
            if (this.publishSplitCompleted && splitCompletedQueueNames.size() < 1) {
                throw new IllegalArgumentException("At least one queue name must be supplied for " + SPLIT_COMPLETED_QUEUES);
            }

            System.out.println("Using payload parent keys " + payloadParentKeys);
            List<String> payloadParentKeys = Arrays.stream(this.payloadParentKeys.split(".")).collect(Collectors.toList());
            if(payloadParentKeys.size() < 1 || payloadParentKeys.get(0) == "") {
                throw new IllegalArgumentException("At least 1 key must be supplied for " + PAYLOAD_PARENT_KEYS);
            }

            return new RabbitmqEventListenerConfig(
                this.url, this.exchangeName, this.exchangeType, queryCreatedQueueNames, queryCompletedQueueNames, splitCompletedQueueNames,
                    payloadParentKeys, this.customProperties,
                    this.durableExchange, this.publishQueryCreated, this.publishQueryCompleted, this.publishSplitCompleted
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
            List<String> payloadParentKeys,
            Map<String, String> customProperties,
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
        this.payloadParentKeys = payloadParentKeys;
        this.customProperties = customProperties;
        this.durableExchange = durableExchange;
        this.publishQueryCreated = publishQueryCreated;
        this.publishQueryCompleted = publishQueryCompleted;
        this.publishSplitCompleted = publishSplitCompleted;
    }

    public static RabbitmqEventListenerConfig create(Map<String, String> config) {
        // Extract and create builder
        RabbitmqEventListenerConfig.Builder builder = new Builder(
                config.get(SERVER_URL),
                config.get(EXCHANGE_NAME),
                config.get(EXCHANGE_TYPE)
        );

        builder.setDurableExchange(parseBoolFromConfigValue(config.get(DURABLE_EXCHANGE), false));
        builder.setPublishQueryCreated(
                parseBoolFromConfigValue(config.get(PUBLISH_QUERY_CREATED), false),
                config.getOrDefault(QUERY_CREATED_QUEUES, "")
        );
        builder.setPublishQueryCompleted(
                parseBoolFromConfigValue(config.get(PUBLISH_QUERY_COMPLETED), false),
                config.getOrDefault(QUERY_COMPLETED_QUEUES, "")
        );
        builder.setPublishSplitCompleted(
                parseBoolFromConfigValue(config.get(PUBLISH_SPLIT_COMPLETED), false),
                config.getOrDefault(SPLIT_COMPLETED_QUEUES, "")
        );
        builder.setPayloadParentKeys(
                config.getOrDefault(PAYLOAD_PARENT_KEYS, "")
        );

        // See if config has any keys that match the custom pattern
        config.entrySet().stream().forEach(configEntry -> {
            String key = configEntry.getKey();
            if (key.startsWith(CUSTOM_PROPERTIES_PATTERN)) {
               builder.addCustomProperty(key, config.get(key));
           }
        });

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
