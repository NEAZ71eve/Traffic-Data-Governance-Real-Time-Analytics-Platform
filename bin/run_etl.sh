#!/bin/bash
# ============================================
# 智慧城市交通数仓 - 主调度脚本
# 用法：bash run_etl.sh [YYYY-MM-DD]
# 示例：bash run_etl.sh 2026-06-08
# 说明：按 DAG 依赖顺序执行全链路 ETL
# ============================================

set -euo pipefail

# -------- 参数处理 --------
DATE="${1:-$(date +%Y-%m-%d)}"
YESTERDAY=$(date -d "${DATE} -1 day" +%Y-%m-%d)
PROJECT_HOME="/data/traffic-data-platform"
LOG_DIR="${PROJECT_HOME}/logs/${DATE}"
SQL_DIR="${PROJECT_HOME}/sql"

mkdir -p "${LOG_DIR}"

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "${LOG_DIR}/etl.log"; }
err() { echo "[$(date '+%H:%M:%S')] [ERROR] $*" | tee -a "${LOG_DIR}/etl_error.log"; }

log "======== 开始执行 ${DATE} 日 ETL ========"

# -------- ODS 层（可并行）--------
run_ods() {
    log "--- ODS 层开始 ---"
    
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/ods/ods_vehicle_pass_di.sql" \
        >"${LOG_DIR}/ods_vehicle_pass.log" 2>&1 &
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/ods/ods_traffic_status_di.sql" \
        >"${LOG_DIR}/ods_traffic_status.log" 2>&1 &
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/ods/ods_device_status_di.sql" \
        >"${LOG_DIR}/ods_device_status.log" 2>&1 &
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/ods/ods_alarm_log_di.sql" \
        >"${LOG_DIR}/ods_alarm_log.log" 2>&1 &
    wait
    
    # 检查结果
    for job in ods_vehicle_pass ods_traffic_status ods_device_status ods_alarm_log; do
        if grep -q "FAILED\|Error\|Exception" "${LOG_DIR}/${job}.log" 2>/dev/null; then
            err "ODS ${job} 执行失败"; return 1
        fi
    done
    log "--- ODS 层完成 ---"
}

# -------- DIM 层 ---------
run_dim() {
    log "--- DIM 层开始 ---"
    
    hive --hivevar date="${DATE}" --hivevar yesterday="${YESTERDAY}" \
        -f "${SQL_DIR}/dim/dim_road_zip.sql" >"${LOG_DIR}/dim_road_zip.log" 2>&1
    
    hive --hivevar date="${DATE}" --hivevar yesterday="${YESTERDAY}" \
        -f "${SQL_DIR}/dim/dim_device_zip.sql" >"${LOG_DIR}/dim_device_zip.log" 2>&1
    
    log "--- DIM 层完成 ---"
}

# -------- DWD 层 ---------
run_dwd() {
    log "--- DWD 层开始 ---"
    
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/dwd/dwd_vehicle_pass_di.sql" \
        >"${LOG_DIR}/dwd_vehicle_pass.log" 2>&1 &
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/dwd/dwd_traffic_status_di.sql" \
        >"${LOG_DIR}/dwd_traffic_status.log" 2>&1 &
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/dwd/dwd_device_status_di.sql" \
        >"${LOG_DIR}/dwd_device_status.log" 2>&1 &
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/dwd/dwd_alarm_log_di.sql" \
        >"${LOG_DIR}/dwd_alarm_log.log" 2>&1 &
    wait
    
    for job in dwd_vehicle_pass dwd_traffic_status dwd_device_status dwd_alarm_log; do
        if grep -q "FAILED\|Error" "${LOG_DIR}/${job}.log" 2>/dev/null; then
            err "DWD ${job} 执行失败"; return 1
        fi
    done
    log "--- DWD 层完成 ---"
}

# -------- DWS 层 ---------
run_dws() {
    log "--- DWS 层开始 ---"
    
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/dws/dws_road_hour_flow.sql" \
        >"${LOG_DIR}/dws_road_hour_flow.log" 2>&1
    
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/dws/dws_area_jam_hour.sql" \
        >"${LOG_DIR}/dws_area_jam_hour.log" 2>&1
    
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/dws/dws_device_health_day.sql" \
        >"${LOG_DIR}/dws_device_health_day.log" 2>&1
    
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/dws/dws_alarm_day.sql" \
        >"${LOG_DIR}/dws_alarm_day.log" 2>&1
    
    log "--- DWS 层完成 ---"
}

# -------- ADS 层 ---------
run_ads() {
    log "--- ADS 层开始 ---"
    
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/ads/ads_traffic_operation.sql" \
        >"${LOG_DIR}/ads_traffic_operation.log" 2>&1
    
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/ads/ads_top_jam_roads.sql" \
        >"${LOG_DIR}/ads_top_jam_roads.log" 2>&1
    
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/ads/ads_device_health_score.sql" \
        >"${LOG_DIR}/ads_device_health_score.log" 2>&1
    
    hive --hivevar date="${DATE}" -f "${SQL_DIR}/ads/ads_device_mtbf_mttr.sql" \
        >"${LOG_DIR}/ads_device_mtbf_mttr.log" 2>&1
    
    log "--- ADS 层完成 ---"
}

# -------- 数据质量检查 ---------
run_quality() {
    log "--- 数据质量检查开始 ---"
    python "${PROJECT_HOME}/python/data_quality_monitor.py" --date "${DATE}" \
        >"${LOG_DIR}/data_quality.log" 2>&1
    
    if grep -q "FAIL" "${LOG_DIR}/data_quality.log" 2>/dev/null; then
        err "数据质量检查未通过"; return 1
    fi
    log "--- 数据质量检查通过 ---"
}

# -------- 分区清理 ---------
run_cleanup() {
    log "--- 分区清理开始 ---"
    bash "${PROJECT_HOME}/bin/partition_cleanup.sh" "${DATE}" 90 \
        >"${LOG_DIR}/partition_cleanup.log" 2>&1
    log "--- 分区清理完成 ---"
}

# ======== 主流程：严格按 DAG 依赖 ========
run_ods   || { err "ODS 失败，中止"; exit 1; }
log "ODS -> DIM/DWD 依赖就绪"
run_dim   || { err "DIM 失败，中止"; exit 1; }
run_dwd   || { err "DWD 失败，中止"; exit 1; }
log "DWD -> DWS 依赖就绪"
run_dws   || { err "DWS 失败，中止"; exit 1; }
log "DWS -> ADS 依赖就绪"
run_ads   || { err "ADS 失败，中止"; exit 1; }
log "ADS -> 质量检查 依赖就绪"
run_quality || { err "质量检查失败，中止"; exit 1; }
run_cleanup

log "======== ${DATE} 日 ETL 全链路完成 ========"
