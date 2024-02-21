# Trino Rabbitmq Event Listener

Event listener plugin for Trino to send query and split events to rabbitmq.

## Building

```bash
./build.sh
```

Then copy the generated jar file into your Trino plugins directory

## Registering

Add `event-listener.properties` with the structure

```
event-listener.name=rabbitmq-event-listener
rabbitmq-server-url=amqp://<server-url>
rabbitmq-exchange-name=<exchange-name>
rabbitmq-exchange-type=<exchange-type>
rabbitmq-durable-exchange=<true|false>
rabbitmq-publish-query-created=<true|false>
rabbitmq-query-created-queues=<separated-list>
rabbitmq-publish-query-completed=<true|false>
rabbitmq-query-completed-queues=<separated-list>
rabbitmq-publish-split-completed=<true|false>
rabbitmq-split-completed-queues=<separated-list>
```