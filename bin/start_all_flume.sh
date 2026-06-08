#!/bin/bash
# ============================================
# Flume Agent 启动脚本
# 用法：bash start_all_flume.sh
# 说明：启动三个 Flume Agent 采集日志到 Kafka
# ============================================

set -euo pipefail

FLUME_HOME="${FLUME_HOME:-/opt/flume}"
PROJECT_HOME="$(cd "$(dirname "$0")/.." && pwd)"

echo "======== 启动 Flume Agents ========"

# Agent 1: 车辆通行日志
nohup "${FLUME_HOME}/bin/flume-ng" agent \
    --name agent \
    --conf "${FLUME_HOME}/conf" \
    --conf-file "${PROJECT_HOME}/flume/flume_vehicle_to_kafka.conf" \
    -Dflume.monitoring.type=http \
    -Dflume.monitoring.port=34545 \
    >"${PROJECT_HOME}/logs/flume_vehicle.log" 2>&1 &
echo "Flume Vehicle Agent 启动, PID=$!"

# Agent 2: 路况监测日志
nohup "${FLUME_HOME}/bin/flume-ng" agent \
    --name agent \
    --conf "${FLUME_HOME}/conf" \
    --conf-file "${PROJECT_HOME}/flume/flume_status_to_kafka.conf" \
    -Dflume.monitoring.type=http \
    -Dflume.monitoring.port=34546 \
    >"${PROJECT_HOME}/logs/flume_status.log" 2>&1 &
echo "Flume Status Agent 启动, PID=$!"

# Agent 3: 设备状态日志
nohup "${FLUME_HOME}/bin/flume-ng" agent \
    --name agent \
    --conf "${FLUME_HOME}/conf" \
    --conf-file "${PROJECT_HOME}/flume/flume_device_status_to_kafka.conf" \
    -Dflume.monitoring.type=http \
    -Dflume.monitoring.port=34547 \
    >"${PROJECT_HOME}/logs/flume_device.log" 2>&1 &
echo "Flume Device Agent 启动, PID=$!"

sleep 3
echo "======== Flume Agents 启动完成 ========"
