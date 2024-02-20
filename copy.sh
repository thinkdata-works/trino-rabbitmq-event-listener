#!/bin/bash

echo "Deploying trino-rabbitmq-event-listener plugin"
rm -rf ../tdw-catalog-platform/tools/shared-services/docker/trino_event_listeners/rabbitmq/*
cp ./build/trino-rabbitmq-event-listener-435.jar ../tdw-catalog-platform/tools/shared-services/docker/trino_event_listeners/rabbitmq