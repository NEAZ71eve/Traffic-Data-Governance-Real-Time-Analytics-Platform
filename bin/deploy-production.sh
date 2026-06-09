#!/bin/bash
# ============================================================================
# deploy-production.sh
# 智慧城市交通大数据平台 — 生产级集群部署脚本
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_HOME}/docker-compose-production.yml"
export COMPOSE_PROJECT_NAME="traffic"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# 检查依赖
check_dependencies() {
    log_info "检查依赖环境..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker 未安装，请先安装 Docker"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose 未安装，请先安装 Docker Compose"
        exit 1
    fi
    
    # 检查 Docker 守护进程
    if ! docker info &> /dev/null; then
        log_error "Docker 守护进程未运行"
        exit 1
    fi
    
    # 检查内存
    TOTAL_MEM=$(docker system info --format '{{.MemTotal}}' 2>/dev/null || echo "0")
    TOTAL_MEM_GB=$((TOTAL_MEM / 1024 / 1024 / 1024))
    if [ "$TOTAL_MEM_GB" -lt 8 ]; then
        log_warn "系统内存 ${TOTAL_MEM_GB}GB，建议至少 8GB 内存"
        read -p "是否继续? [y/N] " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        log_info "系统内存 ${TOTAL_MEM_GB}GB [OK]"
    fi
    
    log_success "依赖检查通过"
}

# 拉取镜像
pull_images() {
    log_info "拉取 Docker 镜像..."
    docker-compose -f "$COMPOSE_FILE" pull
    log_success "镜像拉取完成"
}

# 启动集群
deploy_cluster() {
    log_info "启动生产级集群..."
    log_info "配置文件: $COMPOSE_FILE"
    
    # 创建网络和数据卷（如果不存在）
    docker-compose -f "$COMPOSE_FILE" up -d --no-deps \
        kafka-1 kafka-2 kafka-3 \
        zookeeper \
        redis-node-1 redis-node-2 redis-node-3 redis-node-4 redis-node-5 redis-node-6 \
        hdfs-namenode hdfs-datanode-1 hdfs-datanode-2 hdfs-datanode-3 \
        hive-metastore-db mysql
    
    log_info "等待基础服务就绪..."
    sleep 30
    
    # 启动初始化服务
    log_info "初始化 Kafka Topics..."
    docker-compose -f "$COMPOSE_FILE" up -d kafka-init
    
    log_info "初始化 Redis Cluster..."
    docker-compose -f "$COMPOSE_FILE" up -d redis-cluster-init
    
    log_info "等待集群初始化完成..."
    sleep 20
    
    # 启动 Flink 集群
    log_info "启动 Flink HA 集群..."
    docker-compose -f "$COMPOSE_FILE" up -d zookeeper flink-jobmanager-1 flink-jobmanager-2
    sleep 15
    docker-compose -f "$COMPOSE_FILE" up -d flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3
    
    # 启动 Hive
    log_info "启动 Hive 服务..."
    docker-compose -f "$COMPOSE_FILE" up -d hive-metastore
    sleep 20
    docker-compose -f "$COMPOSE_FILE" up -d hiveserver2
    
    # 启动业务应用
    log_info "启动业务应用..."
    docker-compose -f "$COMPOSE_FILE" up -d app
    
    log_success "集群启动完成"
}

# 初始化 Hive 表
init_hive_tables() {
    log_info "初始化 Hive 数仓表..."
    
    # 等待 HiveServer2 就绪
    for i in {1..30}; do
        if docker exec traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" &> /dev/null; then
            break
        fi
        log_info "等待 HiveServer2 就绪... ($i/30)"
        sleep 10
    done
    
    # 创建数据库
    docker exec traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -e \
        "CREATE DATABASE IF NOT EXISTS traffic_db COMMENT '智慧城市交通数仓';"
    
    # 执行建表脚本
    for sql_file in ${PROJECT_HOME}/sql/ods/*.sql ${PROJECT_HOME}/sql/dim/*.sql ${PROJECT_HOME}/sql/dwd/*.sql ${PROJECT_HOME}/sql/dws/*.sql ${PROJECT_HOME}/sql/ads/*.sql; do
        if [ -f "$sql_file" ]; then
            log_info "执行: $(basename $sql_file)"
            docker exec -i traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000/traffic_db -f - < "$sql_file" || true
        fi
    done
    
    log_success "Hive 表初始化完成"
}

# 检查集群状态
check_status() {
    log_info "检查集群状态..."
    
    echo ""
    echo "=========================================="
    echo "           集群服务状态"
    echo "=========================================="
    docker-compose -f "$COMPOSE_FILE" ps
    
    echo ""
    echo "=========================================="
    echo "           健康检查"
    echo "=========================================="
    
    # Kafka 检查
    if docker exec traffic-kafka-1 kafka-topics.sh --bootstrap-server kafka-1:9092 --list &> /dev/null; then
        log_success "Kafka 集群 [OK]"
    else
        log_error "Kafka 集群 [FAIL]"
    fi
    
    # Flink 检查
    if curl -s http://localhost:8081/overview &> /dev/null; then
        log_success "Flink JobManager [OK]"
    else
        log_error "Flink JobManager [FAIL]"
    fi
    
    # Redis 检查
    if docker exec traffic-redis-1 redis-cli -p 6379 ping | grep -q PONG; then
        log_success "Redis Cluster [OK]"
    else
        log_error "Redis Cluster [FAIL]"
    fi
    
    # HDFS 检查
    if curl -s http://localhost:9870 &> /dev/null; then
        log_success "HDFS NameNode [OK]"
    else
        log_error "HDFS NameNode [FAIL]"
    fi
    
    # Hive 检查
    if docker exec traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" &> /dev/null; then
        log_success "HiveServer2 [OK]"
    else
        log_error "HiveServer2 [FAIL]"
    fi
    
    # MySQL 检查
    if docker exec traffic-mysql mysqladmin ping -h localhost -u traffic -ptraffic123 &> /dev/null; then
        log_success "MySQL [OK]"
    else
        log_error "MySQL [FAIL]"
    fi
}

# 显示访问信息
show_access_info() {
    echo ""
    echo "=========================================="
    echo "         🚀 集群部署完成"
    echo "=========================================="
    echo ""
    echo "📊 服务访问地址:"
    echo "  • Flink Web UI:     http://localhost:8081"
    echo "  • Flink HA UI:      http://localhost:8082"
    echo "  • HDFS NameNode:    http://localhost:9870"
    echo "  • HiveServer2:      jdbc:hive2://localhost:10000"
    echo "  • MySQL:            localhost:3306 (traffic/traffic123)"
    echo "  • Redis Cluster:    localhost:6379-6384"
    echo "  • Kafka:            localhost:9092,9094,9096"
    echo "  • 业务应用:         http://localhost:8088"
    echo ""
    echo "📋 常用命令:"
    echo "  • 查看日志:   docker-compose -f docker-compose-production.yml logs -f [服务名]"
    echo "  • 停止集群:   docker-compose -f docker-compose-production.yml down"
    echo "  • 重启服务:   docker-compose -f docker-compose-production.yml restart [服务名]"
    echo ""
    echo "🔧 Kafka 管理:"
    echo "  • 查看 Topics: docker exec traffic-kafka-1 kafka-topics.sh --bootstrap-server kafka-1:9092 --list"
    echo "  • 查看消费组:  docker exec traffic-kafka-1 kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --list"
    echo ""
    echo "🔧 Redis 管理:"
    echo "  • 查看集群节点: docker exec traffic-redis-1 redis-cli -p 6379 cluster nodes"
    echo "  • 查看集群信息: docker exec traffic-redis-1 redis-cli -p 6379 cluster info"
    echo ""
    echo "🔧 HDFS 管理:"
    echo "  • 查看文件系统: docker exec traffic-hdfs-namenode hdfs dfs -ls /"
    echo "  • 创建目录:     docker exec traffic-hdfs-namenode hdfs dfs -mkdir -p /user/hive/warehouse"
    echo ""
}

# 停止集群
stop_cluster() {
    log_info "停止集群..."
    docker-compose -f "$COMPOSE_FILE" down
    log_success "集群已停止"
}

# 清理数据
cleanup() {
    log_warn "即将清理所有数据卷！"
    read -p "确认删除所有数据? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose -f "$COMPOSE_FILE" down -v
        docker volume prune -f
        log_success "数据清理完成"
    else
        log_info "取消清理"
    fi
}

# 主函数
main() {
    case "${1:-deploy}" in
        deploy)
            check_dependencies
            pull_images
            deploy_cluster
            sleep 30
            init_hive_tables
            check_status
            show_access_info
            ;;
        status)
            check_status
            ;;
        stop)
            stop_cluster
            ;;
        restart)
            stop_cluster
            sleep 5
            deploy_cluster
            check_status
            show_access_info
            ;;
        cleanup)
            cleanup
            ;;
        init-hive)
            init_hive_tables
            ;;
        *)
            echo "用法: $0 {deploy|status|stop|restart|cleanup|init-hive}"
            echo ""
            echo "命令:"
            echo "  deploy     - 部署完整集群"
            echo "  status     - 检查集群状态"
            echo "  stop       - 停止集群"
            echo "  restart    - 重启集群"
            echo "  cleanup    - 清理所有数据"
            echo "  init-hive  - 初始化 Hive 表"
            exit 1
            ;;
    esac
}

main "$@"
