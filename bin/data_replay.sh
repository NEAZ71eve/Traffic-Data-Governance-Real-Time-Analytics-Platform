#!/bin/bash
# ============================================
# 数据回溯/重跑脚本
# 用法：bash data_replay.sh [start_date] [end_date]
# 示例：bash data_replay.sh 2026-06-01 2026-06-05
# 说明：按日期回刷指定范围内所有分区
# ============================================

set -euo pipefail

START="${1}"
END="${2}"

if [ -z "${START}" ] || [ -z "${END}" ]; then
    echo "用法: bash data_replay.sh <start_date> <end_date>"
    echo "示例: bash data_replay.sh 2026-06-01 2026-06-05"
    exit 1
fi

PROJECT_HOME="$(cd "$(dirname "$0")/.." && pwd)"
MAX_DAYS=90

# 检查回溯范围
start_ts=$(date -d "${START}" +%s)
end_ts=$(date -d "${END}" +%s)
max_ts=$(date -d "$(date +%Y-%m-%d) -${MAX_DAYS} days" +%s)

if [ ${start_ts} -lt ${max_ts} ]; then
    echo "错误: 最大回溯 ${MAX_DAYS} 天，${START} 超出范围"
    exit 1
fi

echo "======== 数据回溯: ${START} ~ ${END} ========"

CURRENT="${START}"
while [ "$(date -d "${CURRENT}" +%s)" -le "$(date -d "${END}" +%s)" ]; do
    echo ""
    echo ">>>> 处理日期: ${CURRENT}"
    bash "${PROJECT_HOME}/bin/run_etl.sh" "${CURRENT}"
    
    # 数据质量校验
    python "${PROJECT_HOME}/python/data_quality_monitor.py" --date "${CURRENT}"
    
    CURRENT=$(date -d "${CURRENT} +1 day" +%Y-%m-%d)
done

echo ""
echo "======== 数据回溯完成: ${START} ~ ${END} ========"
