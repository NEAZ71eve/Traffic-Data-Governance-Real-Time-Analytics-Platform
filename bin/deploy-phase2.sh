#!/bin/bash
# ============================================================================
# deploy-phase2.sh
# 智慧城市交通大数据平台 — 第二阶段整改部署脚本
#
# 部署内容：
#   - DataX (全量同步)
#   - Maxwell (CDC Binlog采集)
#   - Flume (日志采集)
#   - DolphinScheduler (任务调度)
#   - Superset (可视化看板)
#   - SCD2 ETL 初始化
#   - 工程优化验证
#
# 前置条件：docker-compose-production.yml 已启动
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_HOME}/docker-compose-phase2.yml"
export COMPOSE_PROJECT_NAME="traffic"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# 检查第一阶段是否已部署
check_phase1() {
    log_info "检查第一阶段集群状态..."
    
    local required_services=("traffic-kafka-1" "traffic-flink-jm-1" "traffic-redis-1" "traffic-hdfs-namenode" "traffic-hiveserver2" "traffic-mysql")
    local missing=()
    
    for svc in "${required_services[@]}"; do
        if ! docker ps --format '{{.Names}}' | grep -q "^${svc}$"; then
            missing+=("$svc")
        fi
    done
    
    if [ ${#missing[@]} -gt 0 ]; then
        log_error "以下服务未运行，请先部署第一阶段:"
        for svc in "${missing[@]}"; do
            echo "  - $svc"
        done
        echo ""
        echo "运行: bash bin/deploy-production.sh deploy"
        exit 1
    fi
    
    log_success "第一阶段集群已就绪"
}

# 部署第二阶段
deploy_phase2() {
    log_info "部署第二阶段整改内容..."
    
    # 1. 启动 MySQL 业务数据初始化
    log_info "1. 初始化 MySQL 业务数据..."
    docker-compose -f "$COMPOSE_FILE" up -d mysql-init
    sleep 10
    
    # 2. 启动数据采集组件
    log_info "2. 启动数据采集组件..."
    docker-compose -f "$COMPOSE_FILE" up -d datax maxwell flume
    sleep 15
    
    # 3. 启动 DolphinScheduler
    log_info "3. 启动 DolphinScheduler 调度系统..."
    docker-compose -f "$COMPOSE_FILE" up -d dolphinscheduler-db
    sleep 15
    docker-compose -f "$COMPOSE_FILE" up -d dolphinscheduler-api dolphinscheduler-master dolphinscheduler-worker
    sleep 20
    
    # 4. 启动 Superset
    log_info "4. 启动 Superset 可视化..."
    docker-compose -f "$COMPOSE_FILE" up -d superset-db
    sleep 15
    docker-compose -f "$COMPOSE_FILE" up -d superset
    sleep 30
    
    # 5. 初始化 SCD2 拉链表
    log_info "5. 初始化 SCD2 拉链表..."
    docker-compose -f "$COMPOSE_FILE" up -d scd2-init
    sleep 30
    
    log_success "第二阶段部署完成"
}

# 检查状态
check_status() {
    log_info "检查第二阶段服务状态..."
    
    echo ""
    echo "=========================================="
    echo "           第二阶段服务状态"
    echo "=========================================="
    docker-compose -f "$COMPOSE_FILE" ps
    
    echo ""
    echo "=========================================="
    echo "           健康检查"
    echo "=========================================="
    
    # DataX 检查
    if docker ps | grep -q "traffic-datax"; then
        log_success "DataX [OK]"
    else
        log_error "DataX [FAIL]"
    fi
    
    # Maxwell 检查
    if docker ps | grep -q "traffic-maxwell"; then
        log_success "Maxwell [OK]"
    else
        log_error "Maxwell [FAIL]"
    fi
    
    # Flume 检查
    if docker ps | grep -q "traffic-flume"; then
        log_success "Flume [OK]"
    else
        log_error "Flume [FAIL]"
    fi
    
    # DolphinScheduler 检查
    if curl -s http://localhost:12345/dolphinscheduler &> /dev/null; then
        log_success "DolphinScheduler [OK]"
    else
        log_error "DolphinScheduler [FAIL]"
    fi
    
    # Superset 检查
    if curl -s http://localhost:8088/health &> /dev/null; then
        log_success "Superset [OK]"
    else
        log_error "Superset [FAIL]"
    fi
}

# 显示访问信息
show_access_info() {
    echo ""
    echo "=========================================="
    echo "         🚀 第二阶段整改完成"
    echo "=========================================="
    echo ""
    echo "📊 新增服务访问地址:"
    echo "  • DolphinScheduler: http://localhost:12345"
    echo "  • Superset:         http://localhost:8088"
    echo "  • DataX:            容器内 /datax/bin/datax.py"
    echo "  • Maxwell:          容器内自动运行"
    echo "  • Flume:            容器内自动运行"
    echo ""
    echo "📋 常用命令:"
    echo "  • 查看日志:   docker-compose -f docker-compose-phase2.yml logs -f [服务名]"
    echo "  • 停止服务:   docker-compose -f docker-compose-phase2.yml down"
    echo "  • 验证优化:   bash bin/verify_optimizations.sh"
    echo "  • SCD2更新:   bash bin/scd2_etl.sh daily"
    echo ""
    echo "🔧 DataX 同步示例:"
    echo "  docker exec traffic-datax python /datax/bin/datax.py /datax/job/road_to_hive.json"
    echo ""
    echo "🔧 Maxwell CDC 监控:"
    echo "  docker logs traffic-maxwell -f"
    echo ""
    echo "🔧 SCD2 拉链表管理:"
    echo "  bash bin/scd2_etl.sh init    # 首次初始化"
    echo "  bash bin/scd2_etl.sh daily   # 每日增量更新"
    echo "  bash bin/scd2_etl.sh verify  # 数据验证"
    echo ""
}

# 停止服务
stop_phase2() {
    log_info "停止第二阶段服务..."
    docker-compose -f "$COMPOSE_FILE" down
    log_success "第二阶段服务已停止"
}

# 主函数
main() {
    case "${1:-deploy}" in
        deploy)
            check_phase1
            deploy_phase2
            check_status
            show_access_info
            ;;
        status)
            check_status
            ;;
        stop)
            stop_phase2
            ;;
        *)
            echo "用法: $0 {deploy|status|stop}"
            echo ""
            echo "命令:"
            echo "  deploy - 部署第二阶段整改内容"
            echo "  status - 检查服务状态"
            echo "  stop   - 停止第二阶段服务"
            exit 1
            ;;
    esac
}

main "$@"
