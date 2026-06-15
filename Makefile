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

.PHONY: build up down restart status logs demo test clean \
        monitor-up monitor-down monitor-status \
        elk-up elk-down elk-status \
        alert-up alert-down \
        superset-setup \
        deploy-all status-all

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

# ========== 监控栈 ==========
monitor-up:
	docker compose -f docker-compose-monitoring.yml up -d
	@echo ""
	@echo "监控栈已启动:"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  AlertManager: http://localhost:9093"
	@echo "  Loki:       http://localhost:3100"

monitor-down:
	docker compose -f docker-compose-monitoring.yml down

monitor-status:
	docker compose -f docker-compose-monitoring.yml ps

# ========== ELK 日志栈 ==========
elk-up:
	docker compose -f docker-compose-elk.yml up -d
	@echo ""
	@echo "ELK 日志栈已启动:"
	@echo "  Kibana:        http://localhost:5601"
	@echo "  Elasticsearch:  http://localhost:9200"
	@echo "  Logstash:       tcp://localhost:5000"

elk-down:
	docker compose -f docker-compose-elk.yml down

elk-status:
	docker compose -f docker-compose-elk.yml ps

# ========== 告警 Webhook ==========
alert-up:
	@echo "启动告警 Webhook 模拟服务器..."
	python python/alert_webhook_server.py &

alert-down:
	@echo "停止告警 Webhook..."
	@pkill -f alert_webhook_server.py || true

alert-test:
	python python/alert_dispatcher.py --test

# ========== Superset 配置 ==========
superset-setup:
	python bin/setup_superset.py

superset-setup-offline:
	python bin/setup_superset.py --offline

# ========== 一键部署 ==========
deploy-all:
	bash bin/deploy-all.sh deploy

deploy-quick:
	bash bin/deploy-all.sh quickstart

status-all:
	bash bin/deploy-all.sh status

verify-all:
	bash bin/deploy-all.sh verify

# ========== 帮助 ==========
help:
	@echo "智慧城市交通数据治理平台 — 命令清单"
	@echo ""
	@echo "  核心服务:"
	@echo "    make up / down / restart / status / logs"
	@echo ""
	@echo "  一键部署:"
	@echo "    make deploy-quick    # 快速启动 (5容器)"
	@echo "    make deploy-all      # 完整部署 (24容器)"
	@echo "    make status-all      # 查看所有服务"
	@echo "    make verify-all      # 全链路验证"
	@echo ""
	@echo "  监控栈:"
	@echo "    make monitor-up      # 启动 Prometheus+Grafana"
	@echo "    make monitor-down    # 停止监控栈"
	@echo ""
	@echo "  日志栈:"
	@echo "    make elk-up          # 启动 ELK"
	@echo "    make elk-down        # 停止 ELK"
	@echo ""
	@echo "  告警系统:"
	@echo "    make alert-up        # 启动 Webhook 模拟器"
	@echo "    make alert-test      # 发送测试告警"
	@echo ""
	@echo "  可视化:"
	@echo "    make superset-setup  # 自动配置 Superset 看板"
	@echo ""
	@echo "  开发测试:"
	@echo "    make demo            # 全流程演示"
	@echo "    make test            # 伪分布式测试"
	@echo "    make shell           # 进入容器"
	@echo "    make clean           # 清理所有"
