#!/bin/bash

echo "Deploying trino-rabbitmq-event-listener plugin"
rm -rf ../tdw-catalog-platform/tools/shared-services/docker/trino_event_listeners/trino-rabbitmq-event-listener/*
cp ./build/trino-rabbitmq-event-listener.jar ../tdw-catalog-platform/tools/shared-services/docker/trino_event_listeners/trino-rabbitmq-event-listener/trino-rabbitmq-event-listener-435.jar