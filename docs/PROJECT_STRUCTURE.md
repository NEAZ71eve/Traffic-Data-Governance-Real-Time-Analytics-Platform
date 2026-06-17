# 项目结构

> v3.0 | 2026-06-15

## 目录树

```
├── config/              # 配置中心 (7个配置)
│   ├── kafka_topics.json
│   ├── hive_config.json
│   ├── dolphinscheduler_config.json
│   ├── metrics_thresholds.json
│   ├── alert_config.json
│   ├── data_permission.json
│   └── superset/superset_config.py
├── sql/                 # 数仓 SQL (24个脚本)
│   ├── ods/ (7)         # 原始数据层 TEXTFILE
│   ├── dim/ (4)         # 维度层 SCD2 ORC+Snappy
│   ├── dwd/ (4)         # 明细清洗层 ORC+Snappy
│   ├── dws/ (4)         # 轻度汇总层 ORC+Snappy
│   └── ads/ (5)         # 应用指标层 ORC+Snappy
├── flink/               # 实时计算 Maven 项目
│   └── src/main/java/com/traffic/flink/
│       ├── TrafficVehicleCount.java      # 车流统计→Redis
│       ├── TrafficCongestionDetection.java # 拥堵检测→Kafka
│       └── DeviceStatusCEP.java          # CEP 异常检测→Kafka
├── python/              # 数据治理 & AI (8个模块)
│   ├── data_quality_monitor.py           # 四维质量监控
│   ├── data_lineage.py                   # 血缘追踪
│   ├── ai_etl_generator.py              # AI ETL 生成
│   ├── ai_anomaly_detector.py           # Isolation Forest 异常检测
│   ├── hive_optimizer.py                # 小文件治理
│   ├── nl2sql_enhanced.py              # NL2SQL
│   ├── alert_dispatcher.py             # 告警分发引擎
│   └── alert_webhook_server.py         # Webhook 模拟器
├── prometheus/          # Prometheus 配置
│   ├── prometheus.yml
│   ├── alert_rules.yml
│   └── alertmanager.yml
├── grafana/             # Grafana 配置
│   ├── dashboards/traffic-platform-overview.json
│   └── provisioning/
├── logstash/            # ELK 日志管道
├── bin/                 # 运维脚本 (11个)
├── docs/                # 文档中心 (8份)
├── pseudo_distributed/  # 伪分布式本地运行 (10个脚本)
├── datax/               # DataX 配置 (3个)
├── flume/               # Flume 配置
├── maxwell/             # Maxwell 配置
├── mysql/               # MySQL 建表
├── redis/               # Redis 配置
├── init/                # 容器初始化脚本
├── docker/              # Docker 工具
├── docker-compose.yml           # 快速模式 (5容器)
├── docker-compose-production.yml # 生产集群 (17容器)
├── docker-compose-phase2.yml    # 采集+可视化 (7容器)
├── docker-compose-monitoring.yml # 监控栈 (6容器)
├── docker-compose-elk.yml       # ELK 日志栈 (3容器)
├── Dockerfile.app
├── Makefile              # 25个快捷命令
├── dashboard_app.py      # Flask 统一仪表盘
├── demo_full_pipeline.py # 全流程演示 (零依赖)
└── README.md
```

## 文件统计

| 类别 | 数量 |
|------|------|
| 数仓 SQL | 24 |
| Python 模块 | 8 |
| Flink 作业 | 3 |
| 配置 JSON | 7 |
| Shell 脚本 | 11 |
| 伪分布式测试 | 10 |
| 文档 | 8 |
| Docker 编排 | 5 |
| DataX 配置 | 3 |

## 技术栈

| 层级 | 技术 |
|------|------|
| 数据采集 | DataX / Maxwell / Flume |
| 消息队列 | Kafka 3.7 |
| 实时计算 | Flink 1.18 |
| 数据仓库 | Hive 4.0 (ORC+Snappy) |
| 调度系统 | DolphinScheduler 2.0.5 |
| 缓存 | Redis 7 |
| 可视化 | Superset / Flask |
| 监控 | Prometheus + Grafana |
| 日志 | ELK |
| AI 辅助 | LangChain + DeepSeek |

## 四维能力域

| 域 | 核心能力 |
|----|---------|
| 交通运行 | 车流统计 / 平均车速 / 拥堵指数 / TOP10 / 高峰分析 |
| 设备运维 | 在线率 / CPU/内存/温度 / 健康评分 / MTBF/MTTR |
| 故障告警 | CEP 模式告警 / P0-P3 分级 / 多渠道推送 / 去重抑制 |
| 数据治理 | 四维质量评分 / 血缘追踪 / AI 异常检测 / NL2SQL / ETL 生成 |
