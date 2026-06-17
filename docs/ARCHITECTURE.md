# 架构设计

## 总体架构

```
MySQL 业务库 ──Maxwell──┐
日志数据 ──Flume────────┼──▶ Kafka (4 Topic) ──┬──▶ Flink 实时计算 ──▶ Redis (实时看板)
                       │                      │
                  终端设备                     └──▶ HDFS ──▶ Hive 数仓 ──▶ DolphinScheduler
                                                          │
                                                   Superset 可视化 + AI 辅助分析
```

## 数据采集层

| 组件 | 用途 | 方式 |
|------|------|------|
| DataX | 静态维表同步 | 全量，每日凌晨 |
| Maxwell | MySQL Binlog CDC | 实时增量 |
| Flume | 日志采集 | 实时增量 |

**Kafka Topic 设计**:

| Topic | 分区 | 副本 | 保留 | 日数据量 |
|-------|------|------|------|---------|
| traffic_vehicle | 8 | 3 | 1天 | ~300万 |
| traffic_status | 4 | 3 | 1天 | ~100万 |
| device_status | 4 | 3 | 1天 | ~50万 |
| device_alarm | 4 | 3 | 7天 | ~5万 |

## 数仓建模 (Kimball)

| 分层 | 表数 | 存储 | 刷新 | 保留 | 职责 |
|------|------|------|------|------|------|
| ODS | 7 | TEXTFILE | T+1 | 90天 | 原始数据留存 |
| DIM | 4 | ORC+Snappy | T+1 SCD2 | 永久 | 维度关联 |
| DWD | 4 | ORC+Snappy | T+1 | 90天 | 清洗去重 |
| DWS | 4 | ORC+Snappy | T+1 | 365天 | 轻度汇总 |
| ADS | 5 | ORC+Snappy | T+1 | 365天 | 应用指标 |

### 维度表

| 表 | 策略 | 说明 |
|----|------|------|
| dim_road_zip | SCD2 | 道路属性变更时闭合旧版本 |
| dim_device_zip | SCD2 | 设备固件/状态变更时触发 |
| dim_time | 静态 | 2020-2030 年时间明细 |
| dim_area | 静态 | DataX 全量同步 |

### 事实表

| 表 | 类型 | 粒度 |
|----|------|------|
| dwd_vehicle_pass_di | 事务型 | 一次车辆通行 |
| dwd_device_status_di | 周期型 | 1分钟设备快照 |
| dwd_alarm_log_di | 事务型 | 一次告警事件 |
| dws_road_hour_flow | 累积快照 | 道路+小时 |
| dws_area_jam_hour | 累积快照 | 区域+小时 |

## 实时计算

| 作业 | 入口 | 源→输出 | 窗口 | Checkpoint |
|------|------|---------|------|-----------|
| 车流统计 | TrafficVehicleCount | Kafka→Redis | 5min滚动 | 5min |
| 拥堵检测 | TrafficCongestionDetection | Kafka→Kafka | 5min滚动 | 5min |
| CEP异常 | DeviceStatusCEP | Kafka→Kafka | CEP模式 | 5min |

**CEP 规则**:

| 规则 | 条件 | 窗口 | 级别 |
|------|------|------|------|
| 连续离线 | 3条 OFFLINE | 180s | CRITICAL |
| CPU 高负载 | 3条 >90% | 5min | MAJOR |
| 温度过高 | 2条 >80°C | 10min | MAJOR |
| 流量突增 | 当前>历史均值×2 | 实时 | MAJOR |

**容灾**:

| 场景 | 方案 |
|------|------|
| Kafka 故障 | Flink 切换读取 HDFS ODS |
| Flink 崩溃 | 固定延迟重启(3次)，从 Checkpoint 恢复 |
| Redis 不可用 | 结果暂存 HDFS，恢复后回灌 |
| 数据延迟 | 质量监控告警→人工介入 |

## 数据血缘

```
ODS ──▶ DWD ──▶ DWS ──▶ ADS
                  ▲
                 DIM
```

| 类型 | 工具 | 采集方式 |
|------|------|---------|
| 离线血缘 | Atlas | Hive Hook 采集 |
| 实时血缘 | Atlas Flink Connector | 运行时注册 |
| 本地血缘 | data_lineage.py | 硬编码+自动发现 |

## 业务指标与告警

| 指标 | 阈值 | 动作 |
|------|------|------|
| 拥堵等级 5 | 车速<10km/h 且 车流>2000/h | 推送管控部门 |
| 设备健康<60 | 四维加权 <60 | 一级告警→工单 |
| 数据完整率<99% | 空值率>1% | 触发重跑 |
| Kafka Lag>10000 | 消费积压 | 运维排查 |

**告警升级**: MINOR→邮件 → MAJOR→钉钉+邮件 → CRITICAL→钉钉@所有人+短信+电话+工单

## 工程优化

| 优化项 | 手段 |
|--------|------|
| 存储 | ORC + Snappy 压缩 |
| Join | MapJoin(小表自动转)、Skew Join(倾斜键打散) |
| 倾斜治理 | 随机前缀打散 + 两阶段聚合 |
| 小文件 | ORC block size=256MB、合并任务 |
| 调度 | 失败重试3次/60s、按层级依赖 ODS→DIM+DWD→DWS→ADS |
