#!/bin/bash

echo "Deploying trino-rabbitmq-event-listener plugin"
rm -f ../tdw-catalog-platform/tools/shared-services/docker/trino_event_listeners/etc/event-listeners/trino-rabbitmq-event-listener.jar
cp ./build/trino-rabbitmq-event-listener.jar ../tdw-catalog-platform/tools/shared-services/docker/trino/etc/event-listeners/