# Trino Rabbitmq Event Listener

Event listener plugin for Trino to send query and split events to rabbitmq. Trino provides a event-listener plugin
framework to receive notifications when (1) queries are created, (2) queries are finished, or (3) splits are completed.
The event payload contains a wealth of information such as:
- what query has been run and who ran it
- what columns and tables has the query touched
- whether the query succeeded or failed
- performance statistics for the query

Currently, Trino only supplies built in support for HTTP and MySQL event listeners.

This plugin allows these event messages to be published to Rabbitmq. It includes the abilities to:
- specify which events are published
- configure the exchange and type for publication
- publish the same event to multiple queues
- specify the payload structure before publication
- configure custom properties to be sent along with the event payload

## Building and Distributing

The distributed is a fatjar that contains all dependencies. The build procedure uses docker, and can be run with:

```bash
./build.sh
```

The constructed jar must be added to the plugin directory in Trino, which should be located in a plugin path like `/usr/lib/trino/plugin/rabbitmq-event-listener/*.jar`.

## Registering the Plugin

Add `event-listener.properties` with the structure

```properties
event-listener.name=rabbitmq
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

## Ensuring plugin detection on startup

Trino logs on startup should show that the plugin is detected and loaded like

```
<timestamp>   INFO	main	io.trino.server.PluginManager	-- Loading plugin /data/trino/plugin/rabbitmq-event-listener --
<timestamp>   INFO	main	io.trino.server.PluginManager	Installing com.tdw.trino.eventlistener.RabbitmqEventListenerPlugin
<timestamp>   INFO	main	io.trino.server.PluginManager	Registering event listener rabbitmq
<timestamp>   INFO	main	io.trino.server.PluginManager	-- Finished loading plugin /data/trino/plugin/rabbitmq-event-listener --
```

It should then pick up the listener from the properties

```
<timestamp>   INFO	main	io.trino.eventlistener.EventListenerManager	-- Loading event listener etc/events/rabbitmq-event-listener.properties --
<timestamp>   INFO	main	io.trino.eventlistener.EventListenerManager	-- Loaded event listener /data/trino/etc/events/rabbitmq-event-listener.properties --
```

## Configuring custom properties

To allow for the Trino to publish details that may be pertinent to it's downstream listeners. For instance, if multiple Trino clusters are being one, and you want to include information about which one has processed the query,
that information can be baked into the configuration and sent with each payload.

## Parent nesting key & payload publication

The structure of the payload can be configurable, such that the payload Trino creates can be nested under some number of keys.
If ommitted, then the payload will not be given any nesting.

For example, given a `parent-parent-keys` property like `key1.key2`, the payload will look like:

```json
{
  "key1": {
    "key2": {
      "<trino-field1>": "...",
      "<trino-field2>": "..."
    },
    "x-custom-<key_string1>": "<value1>",
    "x-custom-<key_stringN>": "<valueN>"
  }
}
```

The custom properties will live as a sibling field to the payload.

# License

This plugin is licensed under the Apache 2.0 License, found in the LICENSE file.