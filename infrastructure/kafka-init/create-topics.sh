#!/bin/sh

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic main \
  --partitions 1 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic events \
  --partitions 1 \
  --replication-factor 1

echo "Topics currently in cluster:"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
