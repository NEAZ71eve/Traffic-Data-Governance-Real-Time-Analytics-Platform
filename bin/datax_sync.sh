#!/bin/bash
# ============================================
# DataX 数据同步脚本
# 用法：bash datax_sync.sh [date]
# 说明：同步 MySQL 静态维表到 HDFS ODS 层
# ============================================

set -euo pipefail

DATE="${1:-$(date +%Y-%m-%d)}"
YESTERDAY=$(date -d "${DATE} -1 day" +%Y-%m-%d)
DATAX_HOME="${DATAX_HOME:-/opt/datax}"
PROJECT_HOME="$(cd "$(dirname "$0")/.." && pwd)"

echo "======== DataX 同步开始: ${DATE} ========"

# 同步区域信息（全量覆盖）
echo "同步 t_area -> HDFS"
python "${DATAX_HOME}/bin/datax.py" \
    -p "-Ddate=${DATE}" \
    -p "-Dyesterday=${YESTERDAY}" \
    "${PROJECT_HOME}/datax/area_to_hive.json"

# 同步道路信息（增量）
echo "同步 t_road -> HDFS"
python "${DATAX_HOME}/bin/datax.py" \
    -p "-Ddate=${DATE}" \
    -p "-Dyesterday=${YESTERDAY}" \
    "${PROJECT_HOME}/datax/road_to_hive.json"

# 同步设备信息（增量）
echo "同步 t_device -> HDFS"
python "${DATAX_HOME}/bin/datax.py" \
    -p "-Ddate=${DATE}" \
    -p "-Dyesterday=${YESTERDAY}" \
    "${PROJECT_HOME}/datax/device_to_hive.json"

echo "======== DataX 同步完成 ========"
