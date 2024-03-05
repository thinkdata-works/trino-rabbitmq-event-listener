#!/bin/bash

echo "Building trino-rabbitmq-event-listener plugin"
docker build --tag trino-rabbitmq-event-listener-builder .

echo "Copying trino-rabbitmq-event-listener output"

id=$(docker create trino-rabbitmq-event-listener-builder)
docker cp $id:/trino-rabbitmq-event-listener/build/libs/trino-rabbitmq-event-listener-435.jar ./build >/dev/null
cp ./build/trino-rabbitmq-event-listener-435.jar ./jars
docker rm -v $id