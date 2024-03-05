package com.tdw.trino.rabbitmq;

public class ConnectionException extends RuntimeException {
    public ConnectionException(String msg) {
        super(msg);
    }
}
