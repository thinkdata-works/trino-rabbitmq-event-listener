package com.tdw.trino.rabbitmq;

public class PublicationException extends RuntimeException {
    public PublicationException(String msg) {
        super(msg);
    }
}
