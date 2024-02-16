#!/bin/bash

echo "Deploying trino-event-listener plugin"
rm -rf ../tdw-catalog-platform/tools/shared-services/docker/trino_event_listeners/platform/trino-event-lsitener
cp ./build/trino-event-listener.jar ../tdw-catalog-platform/tools/shared-services/docker/trino_event_listeners/platform