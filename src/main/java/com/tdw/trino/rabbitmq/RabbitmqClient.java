package com.tdw.trino.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class RabbitmqClient {
    private static final Logger LOGGER = LogManager.getLogger(RabbitmqClient.class);

    private Channel channel;
    private String uri;
    private String exchangeName;

    public RabbitmqClient(String url, String exchangeName, String exchangeType, boolean durable) {
        try {
            this.uri = url;
            this.exchangeName = exchangeName;
            System.out.println("Connecting to url with exchange: " + url + "/" + exchangeName);
            this.channel = getConnectionFactory(this.uri).newConnection().createChannel();
            System.out.println("Created channel " + this.channel);
            this.channel.exchangeDeclare(this.exchangeName, exchangeType, durable);
            System.out.println("Declared exchange");
        } catch(TimeoutException | IOException e) {
            LOGGER.error("Unable to create Rabbitmq client, got exception " + e.getClass() + ": " + e.getMessage());
            throw new ConnectionException(e.getMessage());
        }
    }

    private static ConnectionFactory getConnectionFactory(String uri) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(uri);
            return factory;
        } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            LOGGER.error("Unable to create Rabbitmq connection factory, got exception " + e.getClass() + ": " + e.getMessage());
            throw new ConnectionException(e.getMessage());
        }
    }

    // TODO - run this async in a future?
    // TODO - add in proper payload type
    public void Publish(Set<String> queues, byte[] message) {
        // Create a list of queues that we will be publishing on
        Set<String> toPublishRetry = new HashSet<>();


        synchronized(this) {
            for(String queueName: queues) {
                System.out.println("Publishing to queue " + queueName);
                try  {
                    // TODO - write message to byte array
                    this.channel.basicPublish(this.exchangeName, queueName, null, message);
                } catch(IOException e) {
                    toPublishRetry.add(queueName);
                }
            }

            for(String queueName: toPublishRetry) {
                if (!this.channel.isOpen()) {
                    try {
                        System.out.println("Attempting to recreating channel for publishing");
                        // Reset the channel if it closed on us
                        this.channel = getConnectionFactory(this.uri).newConnection().createChannel();
                    } catch (TimeoutException | IOException e) {
                        LOGGER.error("Unable to recreate channel for publishing. Got exception " + e.getClass() + ": " + e.getMessage());
                        throw new PublicationException(e.getMessage());
                    }
                }

                try {
                    // Attempt to re-publish on the new channel
                    // TODO - write message to byte array
                    this.channel.basicPublish(this.exchangeName, queueName, null, message);
                } catch (IOException e) {
                    LOGGER.error("Unable to re-publish message. Got exception " + e.getClass() + ": " + e.getMessage());
                }

                // TODO - what if we can't publish a second time? Do we retry again?
            }
        }
    }
}
