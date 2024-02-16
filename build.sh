#!/bin/bash

echo "Building trino-event-listener plugin"
docker build --tag trino-event-listener-builder .

echo "Copying trino-event-listener output"

id=$(docker create trino-event-listener-builder)
docker cp $id:/trino-event-listener/build/libs/trino-event-listener.jar ./build >/dev/null
docker rm -v $id