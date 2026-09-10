#!/usr/bin/env bash
# Creates the 'orders' and 'orders-dlq' topics explicitly (instead of relying
# on auto-create) so partition counts are predictable for the demo.
set -euo pipefail

docker exec -it $(docker ps --filter "name=kafka" --format "{{.Names}}" | grep -v zookeeper | grep -v ui | head -n1) \
  kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic orders --partitions 3 --replication-factor 1

docker exec -it $(docker ps --filter "name=kafka" --format "{{.Names}}" | grep -v zookeeper | grep -v ui | head -n1) \
  kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic orders-dlq --partitions 1 --replication-factor 1

echo "Topics created."
