# ============================================================
# 智慧城市交通数据治理平台 — Docker 一键命令
# Usage:
#   make build      构建镜像
#   make up         启动全部服务
#   make down       停止并清理
#   make status     查看状态
#   make demo       运行全流程演示
#   make test       运行伪分布式测试
#   make clean      彻底清理(含数据卷)
# ============================================================

.PHONY: build up down restart status logs demo test clean

COMPOSE = docker compose -p traffic

build:
	$(COMPOSE) build app

up:
	$(COMPOSE) up -d
	@echo ""
	@echo "等待服务启动..."
	@sleep 5
	@$(MAKE) status

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart

status:
	@echo "============================================"
	@echo "  服务状态"
	@echo "============================================"
	@$(COMPOSE) ps
	@echo ""
	@echo "端口映射:"
	@echo "  仪表盘:  http://localhost:8088"
	@echo "  Flink:   http://localhost:8081"
	@echo "  Kafka:   localhost:9092"
	@echo "  Redis:   localhost:6379"

logs:
	$(COMPOSE) logs -f --tail=100

demo:
	$(COMPOSE) exec app python demo_full_pipeline.py

test:
	@echo "运行伪分布式测试套件..."
	$(COMPOSE) exec app python pseudo_distributed/test_hive_sql.py
	$(COMPOSE) exec app python pseudo_distributed/test_hdfs.py
	$(COMPOSE) exec app python pseudo_distributed/test_scheduler.py

test-all: test
	$(COMPOSE) exec app python pseudo_distributed/test_kafka.py || true
	$(COMPOSE) exec app python pseudo_distributed/test_redis.py || true
	$(COMPOSE) exec app python pseudo_distributed/test_pipeline.py || true

shell:
	$(COMPOSE) exec app bash

clean:
	$(COMPOSE) down -v
	@echo "已清理所有容器和数据卷"
