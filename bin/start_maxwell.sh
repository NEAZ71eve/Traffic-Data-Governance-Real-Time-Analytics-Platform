#!/bin/bash
# ============================================
# Maxwell CDC 启动脚本
# 用法：bash start_maxwell.sh
# ============================================

set -euo pipefail

MAXWELL_HOME="${MAXWELL_HOME:-/opt/maxwell}"
PROJECT_HOME="$(cd "$(dirname "$0")/.." && pwd)"

echo "======== 启动 Maxwell CDC ========"

nohup "${MAXWELL_HOME}/bin/maxwell" \
    --config "${PROJECT_HOME}/maxwell/maxwell_config.properties" \
    --log_level INFO \
    >"${PROJECT_HOME}/logs/maxwell.log" 2>&1 &

echo "Maxwell CDC 启动, PID=$!"
sleep 2
echo "======== Maxwell CDC 启动完成 ========"
