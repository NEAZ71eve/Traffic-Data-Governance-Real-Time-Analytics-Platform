# 智慧城市交通数据治理实时分析平台

> **Traffic Data Governance & Real-Time Analytics Platform**
> Kafka 实时采集 → Flink 流计算 → Hive 数仓分层 → Superset 可视化 → AI 辅助治理

[![Java](https://img.shields.io/badge/Java-8+-orange)](.) [![Python](https://img.shields.io/badge/Python-3.8+-blue)](.) [![Flink](https://img.shields.io/badge/Flink-1.18-red)](.) [![Hive](https://img.shields.io/badge/Hive-4.0-yellow)](.) [![Docker](https://img.shields.io/badge/Docker-✅-blue)](.)

## 项目定位

模拟车联网设备上报的交通时序数据，搭建 Hadoop/Hive 离线数仓 + Flink 实时流计算 + Kafka 消息队列 + DolphinScheduler 调度一体化平台。覆盖 **ODS→DIM→DWD→DWS→ADS** 五层数仓建模及全链路 ETL。

## 架构

```
终端设备 ──Flume/Maxwell──▶ Kafka (4 Topic) ──▶ Flink (3 作业) ──▶ Redis / Hive
                                                      │
                                               DolphinScheduler 调度
                                                      │
                                              Hive 数仓 ODS→DIM→DWD→DWS→ADS
                                                      │
                                              Superset 可视化 + AI 辅助
```

## 技术栈

| 层次 | 组件 | 状态 |
|------|------|------|
| 分布式存储 | Hadoop HDFS 3.2.1 | ✅ Docker |
| 离线数仓 | Apache Hive 4.0.0 | ✅ Docker |
| 消息队列 | Apache Kafka 3.7.0 | ✅ Docker |
| 实时计算 | Apache Flink 1.18.1 | ✅ Docker |
| 任务调度 | DolphinScheduler 2.0.5 | ✅ Docker |
| 数据采集 | DataX / Maxwell / Flume | ✅ 配置就绪 |
| 缓存 | Redis 7 | ✅ Docker |
| 数据治理 | Python 自研 | ✅ 可运行 |
| 可视化 | Superset / Flask | ✅ 自动配置 |
| 监控 | Prometheus + Grafana | ✅ 一键启动 |
| 日志 | ELK (ES + Logstash + Kibana) | ✅ 一键启动 |
| AI 辅助 | LangChain + DeepSeek | ✅ 6/6 模块通过 |

## 快速开始

```bash
# 零依赖演示
python demo_full_pipeline.py

# Docker 快速模式 (5 容器)
cd deploy && docker-compose up -d

# 一键生产部署
bash bin/deploy-all.sh deploy
```

| 服务 | 地址 | 凭证 |
|------|------|------|
| Flink | http://localhost:8081 | - |
| Kafka | localhost:9092 | - |
| HDFS | http://localhost:9870 | - |
| Hive | jdbc:hive2://localhost:10000 | - |
| DolphinScheduler | http://localhost:12345 | admin/dolphinscheduler123 |
| Superset | http://localhost:8088 | admin/admin123 |
| Grafana | http://localhost:3000 | admin/admin |
| Redis | localhost:6379 | - |

## 数仓分层

| 分层 | 表数 | 存储 | 保留 | 职责 |
|------|------|------|------|------|
| ODS | 7 | TEXTFILE | 90天 | 原始数据贴源 |
| DIM | 4 | ORC+Snappy | 永久(SCD2) | 维度关联 |
| DWD | 4 | ORC+Snappy | 90天 | 清洗去重 |
| DWS | 4 | ORC+Snappy | 365天 | 轻度汇总 |
| ADS | 5 | ORC+Snappy | 365天 | 应用指标 |

## 核心模块

| 模块 | 说明 |
|------|------|
| **Flink 实时计算** | TrafficVehicleCount(车流→Redis) / CongestionDetection(拥堵→Kafka) / DeviceStatusCEP(CEP异常→Kafka) |
| **数据治理** | 四维质量监控 + 血缘追踪 + 告警推送(钉钉/邮件/短信) |
| **AI 辅助** | NL2SQL / 异常检测(Isolation Forest) / ETL 脚本生成 |
| **监控告警** | Prometheus 10条告警规则 + Grafana 11面板运维大屏 |
| **日志收集** | ELK 全组件日志聚合 + Kibana 检索 |
| **BI 看板** | 4套 Superset 看板(26图表) + Flask 统一仪表盘 |

## 量化成果

| 指标 | 数值 |
|------|------|
| 数仓表 | 24 张 |
| Kafka 消息 | 250,000 条 |
| Flink 作业 | 3 个 |
| AI 模块 | 6/6 通过 |
| 端到端延迟 | < 5 秒 |
| 部署容器 | 24 服务 |

## 文档

| 文档 | 内容 |
|------|------|
| [项目介绍](docs/INTRODUCTION.md) | 业务背景 / 用例 / 数据架构 / FAQ |
| [架构设计](docs/ARCHITECTURE.md) | 数仓分层 / Flink 配置 / CEP 规则 / 血缘 / 优化 |
| [运维手册](docs/RUNBOOK.md) | 部署 / 巡检 / 故障排查 / 容灾 |
| [项目结构](docs/PROJECT_STRUCTURE.md) | 完整目录树 / 文件统计 |
| [BI 看板](docs/BI_DASHBOARDS.md) | 4套看板设计 / 24图表 SQL |
| [AI 模块](docs/AI_MODULE_DESIGN.md) | 技术选型 / 安全边界 / 评估指标 |
| [部署指南](docs/PRODUCTION_DEPLOYMENT.md) | 生产集群部署 |
| [验证报告](docs/VERIFICATION_REPORT.md) | 17项验证结果 |
| [伪分布式](pseudo_distributed/README.md) | 单机运行方案 |
| [Redis 设计](redis/redis_data_structure.md) | 缓存结构设计 |
