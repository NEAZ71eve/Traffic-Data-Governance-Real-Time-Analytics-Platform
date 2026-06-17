# 项目介绍

> 智慧城市交通数据治理实时分析平台 v3.0 | 2026-06-15

## 业务背景

城市交通管理部门面临海量实时数据处理挑战：全市 5000+ 卡口、2000+ 传感器、1000+ 智能设备，日均产生 500 万通行记录和 300 万心跳数据。传统离线报表 T+1 产出，无法支撑实时拥堵疏导和设备故障快速响应。

## 架构

```
实时链路: Kafka ──▶ Flink ──▶ Redis ──▶ Superset 实时看板 (<5s)
离线链路: 终端 ──▶ Kafka/HDFS ──▶ Hive ODS→DIM→DWD→DWS→ADS ──▶ Superset 分析看板
```

## 业务场景

| 场景 | 链路 | 端到端延迟 |
|------|------|-----------|
| 高峰期拥堵预警 | 传感器→Kafka→Flink→钉钉 | <5s |
| 设备故障自动发现 | 心跳→Flink CEP→告警→工单 | <5s |
| 数据质量全链路监控 | 完整率/唯一率/合法性/时效性 | 天级 |
| AI 辅助分析 | NL2SQL / 异常检测 / ETL 生成 | 交互式 |

## 技术选型

| 能力 | 选型 | 理由 |
|------|------|------|
| 消息队列 | Kafka | 生态成熟，Flink 原生支持 |
| 实时计算 | Flink | 真正流处理，Exactly-Once，CEP 库 |
| 离线数仓 | Hive | Hadoop 生态集成，ORC+Snappy 列存 |
| 调度 | DolphinScheduler | 分布式、可视化 DAG |
| 可视化 | Superset | SQL IDE、丰富图表、角色权限 |

## 数据流时序

```
T+0s  设备产生数据
T+1s  Flume/Maxwell → Kafka Topic
T+2s  Flink 窗口聚合/CEP 检测
T+3s  Redis 写入 (实时缓存)
T+4s  Superset 刷新实时看板
T+5s  告警推送 (端到端 <5s)
───────────────────────── 天级批处理分界线 ──
T+1d  DolphinScheduler 触发: ODS→DIM→DWD→DWS→ADS
T+1d  数据质量监控 → 血缘追踪
```

## FAQ

| 问题 | 回答 |
|------|------|
| 最小资源需求？ | 1台 16C/32G 即可跑通全部组件 |
| 实时延迟为何 <5s？ | Kafka→Flink→Redis 全程内存计算 |
| Flink 崩溃怎么办？ | Checkpoint 5min + 3次自动重启，Exactly-Once |
| 数据回溯怎么做？ | DolphinScheduler 补数功能选择日期重跑全链路 |
| SCD2 如何实现？ | dim_*_zip 表 start_date/end_date/is_current 字段 |
| 如何添加新数据源？ | kafka_topics.json 新增 Topic → ODS 建表 → 调度加任务 |
