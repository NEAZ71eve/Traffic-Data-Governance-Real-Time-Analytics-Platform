#!/bin/bash
# ============================================
# Hive 数仓表初始化脚本
# 用法：bash init_hive_tables.sh
# 说明：按 ODS -> DIM -> DWD -> DWS -> ADS 顺序建表
# ============================================

set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "$0")/.." && pwd)"
SQL_DIR="${PROJECT_HOME}/sql"

echo "======== 开始初始化 Hive 表 ========"

# 先建库
hive -e "CREATE DATABASE IF NOT EXISTS traffic_db COMMENT '智慧城市交通数仓';"

# ODS 层
echo "--- ODS 层建表 ---"
hive -f "${SQL_DIR}/ods/ods_vehicle_pass_di.sql"
hive -f "${SQL_DIR}/ods/ods_traffic_status_di.sql"
hive -f "${SQL_DIR}/ods/ods_device_status_di.sql"
hive -f "${SQL_DIR}/ods/ods_alarm_log_di.sql"

# DIM 层
echo "--- DIM 层建表 ---"
hive -f "${SQL_DIR}/dim/dim_road_zip.sql"
hive -f "${SQL_DIR}/dim/dim_device_zip.sql"
hive -f "${SQL_DIR}/dim/dim_time.sql"
hive -f "${SQL_DIR}/dim/dim_area.sql"

# DWD 层
echo "--- DWD 层建表 ---"
hive -f "${SQL_DIR}/dwd/dwd_vehicle_pass_di.sql"
hive -f "${SQL_DIR}/dwd/dwd_traffic_status_di.sql"
hive -f "${SQL_DIR}/dwd/dwd_device_status_di.sql"
hive -f "${SQL_DIR}/dwd/dwd_alarm_log_di.sql"

# DWS 层
echo "--- DWS 层建表 ---"
hive -f "${SQL_DIR}/dws/dws_road_hour_flow.sql"
hive -f "${SQL_DIR}/dws/dws_area_jam_hour.sql"
hive -f "${SQL_DIR}/dws/dws_device_health_day.sql"
hive -f "${SQL_DIR}/dws/dws_alarm_day.sql"

# ADS 层
echo "--- ADS 层建表 ---"
hive -f "${SQL_DIR}/ads/ads_traffic_operation.sql"
hive -f "${SQL_DIR}/ads/ads_top_jam_roads.sql"
hive -f "${SQL_DIR}/ads/ads_device_health_score.sql"
hive -f "${SQL_DIR}/ads/ads_device_mtbf_mttr.sql"
hive -f "${SQL_DIR}/ads/ads_device_fault_top.sql"

echo "======== 验证建表结果 ========"
hive -e "USE traffic_db; SHOW TABLES;" 2>/dev/null

echo "======== Hive 表初始化完成 ========"
