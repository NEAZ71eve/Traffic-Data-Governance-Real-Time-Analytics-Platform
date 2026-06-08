#!/bin/bash
# 容器入口 — 初始化 + 启动仪表盘
set -e

echo "==========================================="
echo "  智慧城市交通数据治理平台"
echo "  Traffic Data Governance Platform"
echo "==========================================="

# 初始化 (创建Kafka Topics、检查Redis、验证DB)
python /app/init/init_containers.py

echo ""
echo "启动仪表盘..."
exec python /app/dashboard_app.py
