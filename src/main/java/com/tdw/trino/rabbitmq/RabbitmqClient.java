package com.tdw.trino.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.tdw.trino.eventlistener.RabbitmqEventListenerConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class RabbitmqClient {
    // TO support - uri, exchange name and properties
    // Options to broadcast the three different ones

    private Channel channel;
    private RabbitmqEventListenerConfig config;

    public RabbitmqClient(RabbitmqEventListenerConfig config) {
        try {
            this.config = config;
            this.channel = getConnectionFactory(config.getUrl()).newConnection().createChannel();

            // TODO - add option to declare exchange, we may not want to do this by default
            this.channel.exchangeDeclare(config.getExchangeName(), config.getExchangeType(), config.isDurableExchange());
        } catch(TimeoutException e) {
            // TODO
        } catch (IOException e) {
            // TODO
        }
    }

    private static ConnectionFactory getConnectionFactory(String uri) throws IOException {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(uri);
            return factory;
        } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            // TODO - handle error properly
            throw new IOException(e.getMessage());
        }
    }

    // TODO - add in proper payload type
    public void Publish(String message) throws TimeoutException, IOException {
        // Create a list of queues that we will be publishing on
        Set<String> toPublishRetry = new HashSet<>();

        for(String queueName: this.config.getPublishQueues()) {
            try  {
                // TODO - write message to byte array
                this.channel.basicPublish(this.config.getExchangeName(), queueName, null, null);
            } catch(IOException e) {
                toPublishRetry.add(queueName);
            }
        }

        for(String queueName: toPublishRetry) {
            // TODO - add retry logic
            if (!this.channel.isOpen()) {
                try {
                    // Reset the channel
                    this.channel = getConnectionFactory(this.config.getUrl()).newConnection().createChannel();
                } catch (TimeoutException | IOException e) {
                    // TODO - handle properly
                    throw e;
                }
            }

            // Attempt to re-publish on the new channel
            // TODO - write message to byte array
            this.channel.basicPublish(this.config.getExchangeName(), queueName, null, null);

            // TODO - what if we can't publish a second time?
        }
    }
}
