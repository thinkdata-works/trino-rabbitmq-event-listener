package com.tdw.trino.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.tdw.trino.eventlistener.proto.Eventlistener;
import jdk.jfr.Event;

public class RabbitmqClient {
    // TO support - uri, exchange name and properties
    // Options to broadcast the three different ones

    public RabbitmqClient(String uri, String exchangeName, String exchangeType, boolean durableExchange) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(uri);
            Connection connection = factory.newConnection();
        }
    }
}
