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

.PHONY: build up down restart status logs demo test test-all shell clean \
        monitor-up monitor-down monitor-status \
        elk-up elk-down elk-status \
        alert-up alert-down alert-test \
        sim-up sim-down sim-logs \
        superset-setup superset-setup-offline \
        deploy-all deploy-quick status-all verify-all \
        flink-build flink-submit \
        help

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
	@echo "启动告警适配器容器..."
	docker compose -f docker-compose-monitoring.yml up -d alertmanager-adapter
	@echo ""
	@echo "告警适配器已启动:"
	@echo "  Health:     http://localhost:5000/health"

alert-down:
	@echo "停止告警 Webhook..."
	@pkill -f alert_webhook_server.py || true

alert-test:
	python python/alert_dispatcher.py --test

# ========== 数据模拟器 ==========
sim-up:
	@echo "启动数据模拟引擎..."
	$(COMPOSE) --profile simulators up -d
	@echo ""
	@echo "数据模拟器已启动:"
	@echo "  查看日志: make sim-logs"

sim-down:
	$(COMPOSE) --profile simulators down

sim-logs:
	$(COMPOSE) logs -f --tail=50 simulator-kafka

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

# ========== Flink 作业编译 ==========
flink-build:
	@echo "使用 Docker Maven 编译 Flink 作业..."
	docker run --rm -v "$(PWD)/flink":/workspace -w /workspace \
	  maven:3.9-eclipse-temurin-11 mvn clean package -DskipTests
	@echo ""
	@echo "Flink JAR 已生成: flink/target/traffic-flink-jobs-1.0.0.jar"
	@ls -la flink/target/traffic-flink-jobs-1.0.0.jar 2>/dev/null || echo "  [WARN] JAR 未找到，请检查编译日志"

flink-submit:
	@echo "提交 Flink 作业到集群..."
	$(COMPOSE) exec flink-jobmanager flink run -d -c com.traffic.flink.TrafficVehicleCount /workspace/traffic-flink-jobs-1.0.0.jar 2>/dev/null || \
	  echo "  [WARN] 请先运行 make flink-build 生成 JAR"

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
	@echo "  数据模拟:"
	@echo "    make sim-up         # 启动数据模拟引擎"
	@echo "    make sim-down       # 停止数据模拟"
	@echo "    make sim-logs       # 查看模拟器日志"
	@echo ""
	@echo "  告警系统:"
	@echo "    make alert-up       # 启动告警适配器容器"
	@echo "    make alert-down     # 停止告警适配器"
	@echo "    make alert-test     # 发送测试告警"
	@echo ""
	@echo "  可视化:"
	@echo "    make superset-setup # 自动配置 Superset 看板"
	@echo ""
	@echo "  Flink 作业:"
	@echo "    make flink-build    # 编译 Flink JAR"
	@echo "    make flink-submit   # 提交 Flink 作业"
	@echo ""
	@echo "  开发测试:"
	@echo "    make demo            # 全流程演示"
	@echo "    make test            # 伪分布式测试"
	@echo "    make shell           # 进入容器"
	@echo "    make clean           # 清理所有"
