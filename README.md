# Trino Rabbitmq Event Listener

Event listener plugin for Trino to send query and split events to rabbitmq.

## Building

```bash
./build.sh
```

Then copy the generated jar file into your Trino plugins directory

## Registering

Add `event-listener.properties` with the structure

```properties
event-listener.name=rabbitmq-event-listener
server-url=amqp://<server-url>
exchange-name=<exchange-name>
exchange-type=<exchange-type>
durable-exchange=<true|false>
publish-query-created=<true|false>
query-created-queues=<separated-list>
publish-query-completed=<true|false>
query-completed-queues=<separated-list>
publish-split-completed=<true|false>
split-completed-queues=<separated-list>
payload-parent-keys=<key1>.<key2>...
x-custom-<key_string1>=<value1>
x-custom-<key_string2>=<value2>
...
x-custom-<key_stringN>=<valueN>
```

# Parent nesting key & payload publication

The payload will be published on the queue like

```json
{
  "<payload-parent-key1>": {
    "<payload-parent-key2>": {
      "<trino-field1>": "...",
      "<trino-field2>": "..."
    },
    "x-custom-<key_string1>": "<value1>",
    "x-custom-<key_stringN>": "<valueN>"
  }
}
```

The parent keys will determine where the payload is nested inside. 
The custom properties will live as a sibling field to the payload