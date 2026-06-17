#!/bin/bash
# ============================================================================
# startup_superset.sh
# 启动 Superset 可视化大屏 (端口 8089)
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_HOME"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo ""
echo "============================================================"
echo "  启动 Superset 可视化大屏 (端口 8089)"
echo "============================================================"
echo ""

# 1. Check Docker
echo -e "${BLUE}[1/3]${NC} 检查 Docker..."
if ! docker info &> /dev/null; then
    echo "Docker 未运行，请先启动 Docker Desktop"
    exit 1
fi

# 2. Start Superset
echo -e "${BLUE}[2/3]${NC} 启动 Superset + PostgreSQL..."
docker compose -f docker-compose-phase2.yml up -d superset-db superset

echo "  等待 Superset 就绪 (最多120秒)..."
for i in $(seq 1 24); do
    if curl -sf http://localhost:8089/health > /dev/null 2>&1; then
        echo -e "${GREEN}[OK]${NC} Superset 已就绪"
        break
    fi
    [ "$i" -eq 24 ] && echo -e "${YELLOW}[WARN]${NC} Superset 启动较慢，继续..."
    sleep 5
done

# 3. Import dashboards
echo ""
echo -e "${BLUE}[3/3]${NC} 导入仪表盘和数据源..."
python bin/import_superset_dashboards.py --host http://localhost:8089

echo ""
echo "============================================================"
echo -e "  ${GREEN}Superset 可视化大屏已就绪!${NC}"
echo "============================================================"
echo ""
echo "  访问地址:  http://localhost:8089"
echo "  用户名:    admin"
echo "  密码:      admin123"
echo ""
echo "  Flask 仪表盘: http://localhost:8088"
echo "============================================================"
