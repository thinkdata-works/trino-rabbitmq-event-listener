package com.tdw.trino.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class RabbitmqClient {
    // TO support - uri, exchange name and properties
    // Options to broadcast the three different ones

    public RabbitmqClient(String uri, String exchangeName, String exchangeType, boolean durableExchange) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(uri);
            Connection connection = factory.newConnection();
            Channel chanel = connection.createChannel();
            chanel.exchangeDeclare(exchangeName, exchangeType, durableExchange);
        } catch(URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            // TODO
        } catch(TimeoutException e) {
            // TODO
        } catch (IOException e) {
            // TODO
        }
    }
}
