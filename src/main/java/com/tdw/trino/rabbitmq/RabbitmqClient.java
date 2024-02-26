package com.tdw.trino.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class RabbitmqClient {
    private Channel channel;
    private String uri;
    private String exchangeName;

    public RabbitmqClient(String url, String exchangeName, String exchangeType, boolean durable) {
        try {
            this.uri = url;
            this.exchangeName = exchangeName;
            this.channel = getConnectionFactory(this.uri).newConnection().createChannel();
            this.channel.exchangeDeclare(this.exchangeName, exchangeType, durable);
        } catch(TimeoutException | IOException e) {
            throw new ConnectionException(e.getMessage());
        }
    }

    private static ConnectionFactory getConnectionFactory(String uri) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(uri);
            return factory;
        } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            throw new ConnectionException(e.getMessage());
        }
    }

    public void Publish(Set<String> queues, byte[] message) {
        // Create a list of queues to re-try publishing on if the first attempt fails
        Set<String> toPublishRetry = new HashSet<>();

        // Lock on this publisher because we may need to re-open the channel if it's closed
        synchronized(this) {
            for(String queueName: queues) {
                System.out.println("Publishing to queue " + queueName);
                try  {
                    this.channel.basicPublish(this.exchangeName, queueName, null, message);
                } catch(IOException e) {
                    toPublishRetry.add(queueName);
                }
            }

            for(String queueName: toPublishRetry) {
                if (!this.channel.isOpen()) {
                    try {
                        System.out.println("Rabbitmq channel has been closed -- attempting to reopen and republish");
                        // Reset the channel if it closed on us
                        this.channel = getConnectionFactory(this.uri).newConnection().createChannel();
                    } catch (TimeoutException | IOException e) {
                        throw new PublicationException(e.getMessage());
                    }
                }

                try {
                    // Attempt to re-publish on the new channel
                    this.channel.basicPublish(this.exchangeName, queueName, null, message);
                } catch (IOException e) {
                    throw new PublicationException(e.getMessage());
                }

                // TODO - what if we can't publish a second time? Do we retry again?
            }
        }
    }
}
