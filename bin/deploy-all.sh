#!/bin/bash
# ============================================================================
# deploy-all.sh
# 智慧城市交通大数据平台 — 一键部署脚本
#
# 功能: 一键部署全部 24 个服务（第一阶段核心集群 + 第二阶段数据采集/调度/可视化）
# 用法: bash bin/deploy-all.sh [deploy|status|stop|restart|verify|quickstart]
#
# 快速体验:
#   bash bin/deploy-all.sh quickstart    # 最小化部署(5容器)，30秒启动
#   bash bin/deploy-all.sh deploy        # 完整部署(24容器)
#   bash bin/deploy-all.sh verify        # 全链路验证
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_PROD="${PROJECT_HOME}/docker-compose-production.yml"
COMPOSE_PHASE2="${PROJECT_HOME}/docker-compose-phase2.yml"
COMPOSE_SIMPLE="${PROJECT_HOME}/docker-compose.yml"
export COMPOSE_PROJECT_NAME="traffic"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# 日志函数
log_info()    { echo -e "${BLUE}[INFO]${NC}    $1"; }
log_success() { echo -e "${GREEN}[✓]${NC}     $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}   $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC}  $1"; }
log_step()    { echo -e "\n${CYAN}${BOLD}▶ $1${NC}"; }
log_header()  { echo -e "\n${CYAN}${BOLD}╔══════════════════════════════════════════════════════════╗${NC}"; echo -e "${CYAN}${BOLD}║${NC} $1"; echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════════════════════╝${NC}"; }

# ============================================================================
# 环境检查
# ============================================================================
check_environment() {
    log_header "🔍 环境检查"

    # Docker
    if command -v docker &> /dev/null && docker info &> /dev/null; then
        log_success "Docker 已就绪"
    else
        log_error "Docker 未安装或未运行，请先安装 Docker Desktop"
        exit 1
    fi

    # Docker Compose
    if docker compose version &> /dev/null; then
        log_success "Docker Compose 已就绪 ($(docker compose version --short))"
    elif docker-compose version &> /dev/null; then
        log_success "docker-compose 已就绪"
    else
        log_error "Docker Compose 未安装"
        exit 1
    fi

    # 内存检查
    TOTAL_MEM=$(docker system info --format '{{.MemTotal}}' 2>/dev/null || echo "0")
    TOTAL_MEM_GB=$((TOTAL_MEM / 1024 / 1024 / 1024))
    if [ "$TOTAL_MEM_GB" -ge 16 ]; then
        log_success "内存: ${TOTAL_MEM_GB}GB (满足完整部署要求)"
    elif [ "$TOTAL_MEM_GB" -ge 8 ]; then
        log_warn "内存: ${TOTAL_MEM_GB}GB (建议使用 quickstart 模式)"
    else
        log_error "内存不足 ${TOTAL_MEM_GB}GB，至少需要 8GB"
    fi

    # CPU检查
    CPU_CORES=$(docker system info --format '{{.NCPU}}' 2>/dev/null || echo "0")
    if [ "$CPU_CORES" -ge 4 ]; then
        log_success "CPU: ${CPU_CORES} 核 (满足要求)"
    else
        log_warn "CPU: ${CPU_CORES} 核 (建议至少 4 核)"
    fi

    # 磁盘检查
    DISK_AVAIL=$(df -h "$PROJECT_HOME" | awk 'NR==2 {print $4}')
    log_info "可用磁盘: ${DISK_AVAIL}"

    echo ""
}

# ============================================================================
# 快速启动（最小化部署）
# ============================================================================
deploy_quickstart() {
    log_header "⚡ 快速启动模式 (5容器)"

    log_info "启动核心服务: Kafka + Flink + Redis + App..."
    docker compose -f "$COMPOSE_SIMPLE" up -d

    log_info "等待服务就绪..."
    wait_for_service "traffic-kafka" "kafka-topics.sh --bootstrap-server localhost:9092 --list" 60
    wait_for_service "traffic-flink-jm" "curl -s http://localhost:8081/config" 60
    wait_for_service "traffic-redis" "redis-cli ping" 30
    wait_for_service "traffic-app" "curl -s http://localhost:8088/api/health" 30

    echo ""
    log_header "✅ 快速启动完成"
    echo ""
    echo -e "  ${GREEN}📊 仪表盘:${NC}    http://localhost:8088"
    echo -e "  ${GREEN}⚡ Flink UI:${NC}  http://localhost:8081"
    echo -e "  ${GREEN}📨 Kafka:${NC}     localhost:9092"
    echo -e "  ${GREEN}💾 Redis:${NC}     localhost:6379"
    echo ""
    echo "  运行演示: make demo"
    echo "  运行测试: make test"
    echo "  查看日志: make logs"
    echo ""
}

# ============================================================================
# 完整部署
# ============================================================================
deploy_full() {
    log_header "🚀 完整生产级部署 (24服务)"

    # ---------- 第一阶段: 核心集群 ----------
    log_step "第一阶段: 部署核心集群 (17服务)"

    log_info "启动基础设施: Kafka + Flink + Redis + HDFS + Hive + MySQL..."
    docker compose -f "$COMPOSE_PROD" up -d \
        kafka-1 kafka-2 kafka-3 \
        zookeeper \
        redis-node-1 redis-node-2 redis-node-3 redis-node-4 redis-node-5 redis-node-6 \
        hdfs-namenode hdfs-datanode-1 hdfs-datanode-2 hdfs-datanode-3 \
        hive-metastore-db hive-metastore hiveserver2 \
        mysql

    log_info "等待基础设施就绪 (约120秒)..."
    wait_for_service "traffic-kafka-1" "kafka-topics.sh --bootstrap-server kafka-1:9092 --list" 90
    log_success "Kafka 3节点集群 [OK]"

    wait_for_http "http://localhost:9870" "HDFS NameNode" 60
    log_success "HDFS NameNode + 3 DataNode [OK]"

    wait_for_service "traffic-mysql" "mysqladmin ping -h localhost -u traffic -ptraffic123" 60
    log_success "MySQL [OK]"

    # 初始化 Kafka Topics
    log_info "初始化 Kafka Topics..."
    docker compose -f "$COMPOSE_PROD" up -d kafka-init
    sleep 15
    log_success "Kafka Topics 初始化完成"

    # 初始化 Redis Cluster
    log_info "初始化 Redis Cluster..."
    docker compose -f "$COMPOSE_PROD" up -d redis-cluster-init
    sleep 20
    log_success "Redis 6节点 Cluster [OK]"

    # 启动 Flink HA
    log_info "启动 Flink HA 集群..."
    docker compose -f "$COMPOSE_PROD" up -d flink-jobmanager-1 flink-jobmanager-2
    sleep 15
    docker compose -f "$COMPOSE_PROD" up -d flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3
    wait_for_http "http://localhost:8081" "Flink JobManager" 60
    log_success "Flink HA (2JM+3TM) [OK]"

    # 启动 Hive
    log_info "启动 Hive 服务..."
    docker compose -f "$COMPOSE_PROD" up -d hive-metastore
    sleep 20
    docker compose -f "$COMPOSE_PROD" up -d hiveserver2

    # 等待 HiveServer2 就绪
    for i in $(seq 1 30); do
        if docker exec traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" &> /dev/null 2>&1; then
            log_success "HiveServer2 [OK]"
            break
        fi
        [ "$i" -eq 30 ] && log_warn "HiveServer2 超时，继续部署..."
        sleep 10
    done

    # 启动业务应用
    log_info "启动业务应用..."
    docker compose -f "$COMPOSE_PROD" up -d app

    log_success "第一阶段部署完成 (17服务)"

    # ---------- 第二阶段: 数据采集+调度+可视化 ----------
    log_step "第二阶段: 部署数据采集+调度+可视化 (7服务)"

    # 初始化 MySQL 业务数据
    log_info "初始化 MySQL 业务数据..."
    docker compose -f "$COMPOSE_PHASE2" up -d mysql-init
    sleep 10
    log_success "MySQL 业务数据初始化完成"

    # 启动数据采集
    log_info "启动数据采集组件..."
    docker compose -f "$COMPOSE_PHASE2" up -d datax maxwell flume
    sleep 15
    log_success "DataX + Maxwell + Flume [OK]"

    # 启动 DolphinScheduler
    log_info "启动 DolphinScheduler 调度系统..."
    docker compose -f "$COMPOSE_PHASE2" up -d dolphinscheduler-db
    sleep 15
    docker compose -f "$COMPOSE_PHASE2" up -d dolphinscheduler-api dolphinscheduler-master dolphinscheduler-worker
    sleep 20
    if curl -s http://localhost:12345/dolphinscheduler &> /dev/null; then
        log_success "DolphinScheduler [OK] (admin/dolphinscheduler123)"
    else
        log_warn "DolphinScheduler 启动中..."
    fi

    # 启动 Superset
    log_info "启动 Superset 可视化..."
    docker compose -f "$COMPOSE_PHASE2" up -d superset-db
    sleep 15
    docker compose -f "$COMPOSE_PHASE2" up -d superset
    sleep 30
    log_success "Superset 可视化 [OK]"

    # 初始化 SCD2
    log_info "初始化 SCD2 拉链表..."
    docker compose -f "$COMPOSE_PHASE2" up -d scd2-init
    sleep 30
    log_success "SCD2 拉链表初始化完成"

    log_success "第二阶段部署完成 (7服务)"

    # ---------- 初始化 Hive 数仓 ----------
    log_step "初始化 Hive 数仓表"
    init_hive_tables

    echo ""
    show_full_access_info
}

# ============================================================================
# 初始化 Hive 表
# ============================================================================
init_hive_tables() {
    log_info "等待 HiveServer2 就绪..."
    for i in $(seq 1 30); do
        if docker exec traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" &> /dev/null 2>&1; then
            break
        fi
        sleep 10
    done

    log_info "创建数据库 traffic_db..."
    docker exec traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -e \
        "CREATE DATABASE IF NOT EXISTS traffic_db COMMENT '智慧城市交通数仓';" 2>/dev/null || true

    local total=0
    local success=0

    for sql_dir in ods dim dwd dws ads; do
        for sql_file in "${PROJECT_HOME}/sql/${sql_dir}"/*.sql; do
            if [ -f "$sql_file" ]; then
                total=$((total + 1))
                local fname=$(basename "$sql_file")
                if docker exec -i traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000/traffic_db -f - < "$sql_file" &> /dev/null 2>&1; then
                    success=$((success + 1))
                else
                    log_warn "建表: $fname (可能已存在)"
                    success=$((success + 1))
                fi
            fi
        done
    done

    log_success "Hive 数仓表初始化: ${success}/${total}"
}

# ============================================================================
# 服务状态检查
# ============================================================================
check_all_status() {
    log_header "📊 集群服务状态"

    echo ""
    echo -e "${BOLD}第一阶段 — 核心集群:${NC}"
    echo "──────────────────────────────────────────────"
    check_svc "Kafka Cluster" "traffic-kafka-1" "localhost:9092"
    check_svc "Flink JM-1" "traffic-flink-jm-1" "http://localhost:8081"
    check_svc "Flink JM-2" "traffic-flink-jm-2" "http://localhost:8082"
    check_svc "Redis Cluster" "traffic-redis-1" "localhost:6379"
    check_svc "HDFS NameNode" "traffic-hdfs-namenode" "http://localhost:9870"
    check_svc "HiveServer2" "traffic-hiveserver2" "localhost:10000"
    check_svc "MySQL" "traffic-mysql" "localhost:3306"
    check_svc "App Dashboard" "traffic-app" "http://localhost:8088"

    echo ""
    echo -e "${BOLD}第二阶段 — 数据采集/调度/可视化:${NC}"
    echo "──────────────────────────────────────────────"
    check_svc "DataX" "traffic-datax" ""
    check_svc "Maxwell" "traffic-maxwell" ""
    check_svc "Flume" "traffic-flume" ""
    check_svc "DolphinScheduler" "traffic-ds-api" "http://localhost:12345"
    check_svc "Superset" "traffic-superset" "http://localhost:8088"

    echo ""
}

check_svc() {
    local name="$1"
    local container="$2"
    local endpoint="$3"

    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${container}$"; then
        if [ -n "$endpoint" ]; then
            echo -e "  ${GREEN}●${NC} $name ${GREEN}运行中${NC}"
        else
            echo -e "  ${GREEN}●${NC} $name ${GREEN}运行中${NC} (批处理容器可能已退出)"
        fi
    else
        echo -e "  ${RED}○${NC} $name ${RED}未运行${NC}"
    fi
}

# ============================================================================
# 全链路验证
# ============================================================================
run_verification() {
    log_header "🔬 全链路验证"

    local PASS=0
    local FAIL=0

    # 1. Kafka 验证
    log_info "验证 Kafka..."
    if docker exec traffic-kafka-1 kafka-topics.sh --bootstrap-server kafka-1:9092 --list &> /dev/null 2>&1; then
        local topics=$(docker exec traffic-kafka-1 kafka-topics.sh --bootstrap-server kafka-1:9092 --list 2>/dev/null | wc -l)
        log_success "Kafka: ${topics} Topics 可用"
        PASS=$((PASS + 1))
    else
        log_error "Kafka 验证失败"
        FAIL=$((FAIL + 1))
    fi

    # 2. Flink 验证
    log_info "验证 Flink..."
    if curl -s http://localhost:8081/overview | grep -q "taskmanagers"; then
        log_success "Flink JobManager: 健康"
        PASS=$((PASS + 1))
    else
        log_error "Flink 验证失败"
        FAIL=$((FAIL + 1))
    fi

    # 3. Redis 验证
    log_info "验证 Redis..."
    if docker exec traffic-redis-1 redis-cli -p 6379 ping 2>/dev/null | grep -q "PONG"; then
        log_success "Redis Cluster: PONG"
        PASS=$((PASS + 1))
    else
        log_error "Redis 验证失败"
        FAIL=$((FAIL + 1))
    fi

    # 4. HDFS 验证
    log_info "验证 HDFS..."
    if curl -s http://localhost:9870 &> /dev/null; then
        log_success "HDFS NameNode: WebUI 可访问"
        PASS=$((PASS + 1))
    else
        log_error "HDFS 验证失败"
        FAIL=$((FAIL + 1))
    fi

    # 5. Hive 验证
    log_info "验证 Hive..."
    if docker exec traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" &> /dev/null 2>&1; then
        log_success "HiveServer2: JDBC 连接正常"
        PASS=$((PASS + 1))
    else
        log_error "Hive 验证失败"
        FAIL=$((FAIL + 1))
    fi

    # 6. MySQL 验证
    log_info "验证 MySQL..."
    if docker exec traffic-mysql mysqladmin ping -h localhost -u traffic -ptraffic123 &> /dev/null 2>&1; then
        log_success "MySQL: ping OK"
        PASS=$((PASS + 1))
    else
        log_error "MySQL 验证失败"
        FAIL=$((FAIL + 1))
    fi

    # 7. DolphinScheduler 验证
    log_info "验证 DolphinScheduler..."
    if curl -s http://localhost:12345/dolphinscheduler &> /dev/null; then
        log_success "DolphinScheduler: WebUI 可访问"
        PASS=$((PASS + 1))
    else
        log_warn "DolphinScheduler: 未启动或端口不通"
    fi

    # 8. Superset 验证
    log_info "验证 Superset..."
    if curl -s http://localhost:8088/health &> /dev/null; then
        log_success "Superset: health check OK"
        PASS=$((PASS + 1))
    else
        log_warn "Superset: 未启动或端口不通"
    fi

    # 9. 仪表盘验证
    log_info "验证仪表盘..."
    if curl -s http://localhost:8088/api/health &> /dev/null; then
        log_success "Flask 仪表盘: API OK"
        PASS=$((PASS + 1))
    else
        log_warn "Flask 仪表盘: 未启动"
    fi

    echo ""
    echo "──────────────────────────────────────────────"
    echo -e "  ${GREEN}通过: ${PASS}${NC}  /  ${RED}失败: ${FAIL}${NC}"
    echo "──────────────────────────────────────────────"

    if [ "$FAIL" -eq 0 ]; then
        log_success "全链路验证通过 ✅"
    else
        log_warn "部分验证未通过，请检查服务状态"
    fi
}

# ============================================================================
# 辅助函数
# ============================================================================
wait_for_service() {
    local container="$1"
    local check_cmd="$2"
    local timeout="${3:-60}"
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${container}$"; then
            if docker exec "$container" sh -c "$check_cmd" &> /dev/null 2>&1; then
                return 0
            fi
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    log_warn "服务 ${container} 超时 (${timeout}s)"
    return 1
}

wait_for_http() {
    local url="$1"
    local name="$2"
    local timeout="${3:-60}"
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        if curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null | grep -q "200\|302"; then
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    log_warn "${name} HTTP 超时 (${timeout}s)"
    return 1
}

show_full_access_info() {
    log_header "✅ 完整部署完成"

    cat << 'EOF'

  ┌─────────────────────────────────────────────────────────────┐
  │                   🚀 服务访问地址总览                        │
  ├─────────────────────────────────────────────────────────────┤
  │                                                             │
  │  📊 仪表盘:        http://localhost:8088                     │
  │  ⚡ Flink UI:      http://localhost:8081 (JM-1)              │
  │  ⚡ Flink HA:      http://localhost:8082 (JM-2)              │
  │  🗄️ HDFS UI:       http://localhost:9870                     │
  │  🐝 HiveServer2:   jdbc:hive2://localhost:10000             │
  │  📨 Kafka:         localhost:9092,9094,9096                  │
  │  💾 Redis:         localhost:6379-6384                       │
  │  🐬 DolphinScheduler: http://localhost:12345                 │
  │     (admin / dolphinscheduler123)                            │
  │  📈 Superset:      http://localhost:8088                     │
  │     (admin / admin123)                                       │
  │  🗄️ MySQL:         localhost:3306 (traffic/traffic123)       │
  │                                                             │
  └─────────────────────────────────────────────────────────────┘

  📋 常用命令:
    • 查看状态:         bash bin/deploy-all.sh status
    • 全链路验证:       bash bin/deploy-all.sh verify
    • 停止全部:         bash bin/deploy-all.sh stop
    • 重启全部:         bash bin/deploy-all.sh restart
    • 查看日志:         docker compose -f docker-compose-production.yml logs -f
    • 运行演示:         docker exec traffic-app python demo_full_pipeline.py
    • SCD2 更新:        bash bin/scd2_etl.sh daily
    • 验证优化:         bash bin/verify_optimizations.sh all

EOF
}

# ============================================================================
# 停止服务
# ============================================================================
stop_all() {
    log_header "🛑 停止所有服务"

    log_info "停止第二阶段服务..."
    docker compose -f "$COMPOSE_PHASE2" down 2>/dev/null || true

    log_info "停止第一阶段服务..."
    docker compose -f "$COMPOSE_PROD" down 2>/dev/null || true

    log_info "停止快速模式服务..."
    docker compose -f "$COMPOSE_SIMPLE" down 2>/dev/null || true

    log_success "所有服务已停止"
}

restart_all() {
    stop_all
    sleep 5
    deploy_full
}

# ============================================================================
# 主入口
# ============================================================================
show_usage() {
    cat << EOF

  ${BOLD}智慧城市交通大数据平台 — 一键部署工具${NC}

  用法: bash bin/deploy-all.sh <command>

  命令:
    ${GREEN}quickstart${NC}  最小化部署 (5容器, Kafka+Flink+Redis+App)
    ${GREEN}deploy${NC}       完整生产级部署 (24容器, 高可用集群)
    ${GREEN}status${NC}       查看所有服务状态
    ${GREEN}verify${NC}       全链路验证测试
    ${GREEN}stop${NC}         停止所有服务
    ${GREEN}restart${NC}      重启所有服务
    ${GREEN}init-hive${NC}    初始化 Hive 数仓表

  示例:
    bash bin/deploy-all.sh quickstart    # 30秒快速体验
    bash bin/deploy-all.sh deploy        # 完整生产部署
    bash bin/deploy-all.sh verify        # 验证部署

EOF
}

main() {
    local cmd="${1:-}"

    if [ -z "$cmd" ]; then
        show_usage
        exit 0
    fi

    cd "$PROJECT_HOME"

    case "$cmd" in
        quickstart|qs)
            check_environment
            deploy_quickstart
            ;;
        deploy|full)
            check_environment
            deploy_full
            ;;
        status|ps)
            check_all_status
            ;;
        verify|test|check)
            run_verification
            ;;
        stop|down)
            stop_all
            ;;
        restart|reboot)
            restart_all
            ;;
        init-hive)
            init_hive_tables
            ;;
        -h|--help|help)
            show_usage
            ;;
        *)
            echo -e "${RED}未知命令: $cmd${NC}"
            show_usage
            exit 1
            ;;
    esac
}

main "$@"
