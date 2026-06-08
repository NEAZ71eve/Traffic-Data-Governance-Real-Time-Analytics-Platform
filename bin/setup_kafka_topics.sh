#!/bin/bash
# ============================================
# Kafka Topic 初始化脚本
# 用法：bash setup_kafka_topics.sh
# ============================================

set -euo pipefail

ZOOKEEPER="${ZOOKEEPER:-localhost:2181}"
BROKER="${BROKER_LIST:-localhost:9092}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"

echo "======== 创建 Kafka Topics ========"

# 车辆通行数据 Topic —— 数据量大，分区多
kafka-topics.sh --create \
    --zookeeper "${ZOOKEEPER}" \
    --topic traffic_vehicle \
    --partitions 8 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config compression.type=snappy \
    --if-not-exists

# 路况监测数据 Topic
kafka-topics.sh --create \
    --zookeeper "${ZOOKEEPER}" \
    --topic traffic_status \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config compression.type=snappy \
    --if-not-exists

# 设备状态数据 Topic
kafka-topics.sh --create \
    --zookeeper "${ZOOKEEPER}" \
    --topic device_status \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config compression.type=snappy \
    --if-not-exists

# 故障告警数据 Topic —— 保留7天
kafka-topics.sh --create \
    --zookeeper "${ZOOKEEPER}" \
    --topic device_alarm \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config compression.type=snappy \
    --if-not-exists

# Maxwell CDC Topic
kafka-topics.sh --create \
    --zookeeper "${ZOOKEEPER}" \
    --topic maxwell_traffic_cdc \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=259200000 \
    --if-not-exists

echo "======== 验证 Topics ========"
kafka-topics.sh --list --zookeeper "${ZOOKEEPER}"

echo "======== Kafka Topics 初始化完成 ========"
