#!/bin/bash
# ============================================
# 分区清理脚本
# 用法：bash partition_cleanup.sh [date] [retention_days]
# 示例：bash partition_cleanup.sh 2026-06-08 90
# ============================================

set -euo pipefail

DATE="${1:-$(date +%Y-%m-%d)}"
RETENTION_DAYS="${2:-90}"
CUTOFF_DATE=$(date -d "${DATE} -${RETENTION_DAYS} days" +%Y-%m-%d)

echo "清理 ${CUTOFF_DATE} 之前的过期分区..."

# 所有分区表的列表
TABLES=(
    "traffic_db.ods_vehicle_pass_di"
    "traffic_db.ods_traffic_status_di"
    "traffic_db.ods_device_status_di"
    "traffic_db.ods_alarm_log_di"
    "traffic_db.dim_road_zip"
    "traffic_db.dim_device_zip"
    "traffic_db.dwd_vehicle_pass_di"
    "traffic_db.dwd_traffic_status_di"
    "traffic_db.dwd_device_status_di"
    "traffic_db.dwd_alarm_log_di"
    "traffic_db.dws_road_hour_flow"
    "traffic_db.dws_area_jam_hour"
    "traffic_db.dws_device_health_day"
    "traffic_db.dws_alarm_day"
    "traffic_db.ads_traffic_operation"
    "traffic_db.ads_top_jam_roads"
    "traffic_db.ads_device_health_score"
    "traffic_db.ads_device_mtbf_mttr"
)

for table in "${TABLES[@]}"; do
    echo "清理表: ${table}"
    hive -e "ALTER TABLE ${table} DROP IF EXISTS PARTITION(dt < '${CUTOFF_DATE}');" 2>&1 || \
        echo "[WARN] ${table} 分区清理异常，跳过"
done

echo "分区清理完成"
