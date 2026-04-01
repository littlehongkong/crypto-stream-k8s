#!/bin/bash
# Kafka 토픽 초기화 스크립트
# 사용법: bash scripts/create-topics.sh

set -e

BROKER="localhost:9092"
CONTAINER="kafka"

echo "Kafka 토픽 생성 시작..."

docker exec $CONTAINER kafka-topics --create \
  --bootstrap-server $BROKER \
  --topic crypto-trades \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists  # 이미 있으면 에러 없이 스킵

echo "생성된 토픽 목록:"
docker exec $CONTAINER kafka-topics --list \
  --bootstrap-server $BROKER

echo "파티션 상세:"
docker exec $CONTAINER kafka-topics --describe \
  --bootstrap-server $BROKER \
  --topic crypto-trades

echo "완료"
