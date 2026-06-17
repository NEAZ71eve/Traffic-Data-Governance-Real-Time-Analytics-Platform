#!/bin/bash
# ============================================================================
# startup_with_simulation.sh
# 智慧城市交通数据治理平台 — 带数据模拟的启动脚本
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_HOME"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC}    $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}     $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}   $1"; }

cleanup() {
    log_info "停止模拟器..."
    if [ -n "${VIZ_PID:-}" ]; then kill "$VIZ_PID" 2>/dev/null || true; fi
    log_info "已退出"
}
trap cleanup EXIT

echo ""
echo "============================================================"
echo "  智慧城市交通数据治理平台 — 带数据模拟的启动"
echo "============================================================"
echo ""

# 1. Check Docker
log_info "[1/4] 检查 Docker 环境..."
if docker info &> /dev/null; then
    log_success "Docker 已就绪"
else
    log_info "Docker 未运行，请先启动 Docker Desktop"
    exit 1
fi

# 2. Start core services
log_info "[2/4] 启动核心服务..."
docker compose -p traffic up -d

log_info "等待服务就绪..."
for i in $(seq 1 30); do
    if curl -s http://localhost:8088/api/health 2>/dev/null | grep -q "healthy"; then
        log_success "核心服务已就绪"
        break
    fi
    [ "$i" -eq 30 ] && log_warn "服务启动超时，继续..."
    sleep 3
done

# 3. Start Kafka data simulator (containerized)
log_info "[3/4] 启动 Kafka 数据采集模拟器 (容器化)..."
docker compose -p traffic --profile simulators up -d simulator-kafka
log_success "数据模拟器已启动 (容器内运行)"

# 4. Start realtime pipeline visualization
log_info "[4/4] 启动实时管道可视化 (端口 8090)..."
python realtime_pipeline.py &
VIZ_PID=$!
sleep 2

echo ""
echo "============================================================"
echo "  启动完成!"
echo "============================================================"
echo ""
echo -e "  ${GREEN}仪表盘:${NC}        http://localhost:8088"
echo -e "  ${GREEN}管道可视化:${NC}    http://localhost:8090"
echo -e "  ${GREEN}Flink UI:${NC}      http://localhost:8081"
echo ""
echo -e "  按 Ctrl+C 停止模拟器 (服务继续运行)"
echo -e "  停止全部: docker compose -p traffic down"
echo "============================================================"
echo ""

# Wait for signal
wait
