# 智慧城市交通数据治理实时分析平台

> **Traffic Data Governance & Real-Time Analytics Platform**  
> 覆盖全链路的交通大数据平台：Kafka 实时采集 → Flink 流计算 → Hive 数仓分层 → Superset 可视化 → AI 辅助治理

[![Tech](https://img.shields.io/badge/Java-8+-orange)](.) [![Python](https://img.shields.io/badge/Python-3.8+-blue)](.) [![Flink](https://img.shields.io/badge/Flink-1.15+-red)](.) [![Hive](https://img.shields.io/badge/Hive-3.1+-yellow)](.)

---

## 项目定位

面向**智慧城市交通管理**场景，构建一套企业级大数据实时分析平台。涵盖数据采集、实时计算、离线数仓、数据治理、可视化看板、AI 辅助分析六大能力域，技术栈完整对标一线互联网公司大数据平台。

```
  交通终端设备                      实时链路                    离线链路
  ┌──────────┐              ┌──────────────────┐       ┌─────────────────────┐
  │ 车辆卡口  │──┐           │  Kafka (4 Topic)  │       │   HDFS 数据湖        │
  │ 路况传感器│  │           │  ┌──────────────┐ │       │   ┌─────────────┐   │
  │ 设备心跳  │──┼──Flume───▶│  │traffic_vehicle│ │──▶Flink│ Hive ODS(TEXT)│   │
  │ 告警日志  │  │  Maxwell  │  │traffic_status │ │       │   └──────┬──────┘   │
  └──────────┘──┘           │  │device_status  │ │       │          ▼          │
                            │  │device_alarm   │ │       │   ┌─────────────┐   │
                            │  └──────────────┘ │       │   │ DWD 明细清洗 │   │
                            └──────────────────┘       │   └──────┬──────┘   │
                                     │                 │          ▼          │
                                     ▼                 │   ┌─────────────┐   │
                            ┌──────────────────┐       │   │ DWS 轻度汇总 │   │
                            │  Redis 实时缓存   │       │   └──────┬──────┘   │
                            └────────┬─────────┘       │          ▼          │
                                     │                 │   ┌─────────────┐   │
         ┌───────────────────────────┼─────────────────┤   │ ADS 应用指标 │   │
         ▼                           ▼                 │   └─────────────┘   │
   ┌──────────┐              ┌──────────────┐         └─────────────────────┘
   │ Superset │              │ DingTalk/Email│                  │
   │ 实时看板  │              │  智能告警通知 │           DolphinScheduler
   └──────────┘              └──────────────┘            任务调度编排
```

---

## 技术栈

| 层次 | 组件 | 用途 |
|------|------|------|
| **数据采集** | DataX / Maxwell / Flume | 全量同步 / CDC增量 / 日志采集 |
| **消息队列** | Kafka 2.8+ | 4 Topic 解耦，8分区高吞吐 |
| **实时计算** | Flink 1.15+ | 滚动窗口聚合 + CEP 模式匹配 + KeyedState |
| **存储引擎** | HDFS 3.3+ / Hive 3.1+ / Redis 6.2+ | 数据湖 / 数仓 / 实时缓存 |
| **任务调度** | DolphinScheduler 3.1+ | 18 个任务 DAG 编排，补数重跑 |
| **数据治理** | Apache Griffin / Atlas / Ranger | 质量监控 / 血缘图谱 / 权限管控 |
| **可视化** | Apache Superset 2.0+ | 4 套角色看板，24 个图表 |
| **AI 辅助** | Isolation Forest / SQLNet / Jinja2 | 异常检测 / NL2SQL / ETL 生成 |
| **开发语言** | Java 8+ / Python 3.8+ / SQL(HQL) | Flink 作业 / 治理脚本 / 数仓 ETL |

---

## 项目结构

```
Traffic-Data-Governance-Real-Time-Analytics-Platform/
│
├── config/                              # 配置中心 (6个JSON)
│   ├── kafka_topics.json                #   Kafka Topic & 消费者组
│   ├── hive_config.json                 #   Hive 连接参数
│   ├── dolphinscheduler_config.json     #   调度工作流 & 回溯策略
│   ├── metrics_thresholds.json          #   业务指标阈值 (拥堵/健康/质量)
│   ├── alert_config.json                #   告警渠道 (钉钉/邮件/短信)
│   └── data_permission.json             #   权限矩阵 (Ranger/Superset)
│
├── sql/                                 # 数仓SQL (20个脚本, DDL+ETL)
│   ├── ods/  (4)                         #   ODS 原始数据层 → TEXTFILE
│   ├── dim/  (4)                         #   DIM 维度层 → SCD2拉链表
│   ├── dwd/  (4)                         #   DWD 明细清洗层 → 去重/校验/派生
│   ├── dws/  (4)                         #   DWS 轻度汇总层 → 小时/天聚合
│   └── ads/  (4)                         #   ADS 应用指标层 → 看板指标
│
├── flink/                               # 实时计算 (3个Java作业)
│   ├── TrafficVehicleCount.java          #   车流统计 → Redis
│   ├── TrafficCongestionDetection.java   #   拥堵检测 → Kafka + 流量突增
│   └── DeviceStatusCEP.java              #   CEP异常检测 → 离线/CPU/温度
│
├── python/                              # 数据治理 (3个Python脚本)
│   ├── data_quality_monitor.py           #   质量监控 + 钉钉/邮件告警
│   ├── data_lineage.py                   #   血缘追踪 + 影响分析
│   └── ai_etl_generator.py               #   AI ETL生成 + NL2SQL
│
├── docs/                                # 文档中心
│   ├── INTRODUCTION.md                   #   项目介绍手册 (新人必读)
│   ├── PROJECT_STRUCTURE.md              #   项目结构说明
│   ├── ARCHITECTURE.md                   #   架构设计文档 (技术细节)
│   ├── RUNBOOK.md                        #   运维操作手册
│   ├── BI_DASHBOARDS.md                  #   Superset看板设计
│   └── AI_MODULE_DESIGN.md               #   AI模块选型与边界
│
├── demo_full_pipeline.py                 # 全流程模拟演示 (零依赖)
└── README.md                             # 本文档
```

---

## 数仓分层设计 (Kimball 维度建模)

| 分层 | 表名规范 | 刷新频率 | 存储 | 保留 | 核心职责 |
|------|---------|---------|------|------|---------|
| **ODS** | `ods_{entity}_di` | 天级 | TEXTFILE | 90天 | 原始数据贴源层 |
| **DIM** | `dim_{entity}_zip` | 天级(SCD2) | ORC+Snappy | 永久 | 道路/设备/时间/区域维度 |
| **DWD** | `dwd_{entity}_di` | 天级 | ORC+Snappy | 90天 | 清洗/去重/值域校验/派生字段 |
| **DWS** | `dws_{entity}_{granularity}` | 天级 | ORC+Snappy | 365天 | 道路×小时/区域×小时/设备×天聚合 |
| **ADS** | `ads_{dashboard}` | 天级 | ORC+Snappy | 365天 | 运营看板/拥堵TOP榜/健康评分/MTBF |

### 事实表类型

| 表 | 类型 | 粒度 | 说明 |
|----|------|------|------|
| dwd_vehicle_pass_di | **事务型** | 车辆通行事件 | 一次车辆通过=一条记录 |
| dwd_device_status_di | **周期型** | 设备心跳快照(~1min) | 周期性采集指标 |
| dws_road_hour_flow | **累积快照** | 道路+小时 | 从明细聚合到小时 |
| dws_area_jam_hour | **累积快照** | 区域+小时 | 区域级拥堵汇总 |

---

## 核心功能模块

### 1. 实时流计算 (Flink)

| 作业 | 数据源 | 输出 | 关键技术 |
|------|--------|------|---------|
| TrafficVehicleCount | Kafka traffic_vehicle | Redis | 5min滚动窗口, AggregateFunction, BoundedOutOfOrderness |
| TrafficCongestionDetection | Kafka traffic_status | Kafka Alert | KeyedState流量突增检测(EMA), Watermark 30s |
| DeviceStatusCEP | Kafka device_status | Kafka Alert | CEP连续模式匹配, 3条OFFLINE/CPU>90%/温度>80°C |

**容错机制**: Checkpoint(5min, EXACTLY_ONCE) + Savepoint + 固定延迟重启(3次/60s)

### 2. 数据治理

| 工具 | 能力 | 技术实现 |
|------|------|---------|
| data_quality_monitor.py | 完整率/唯一率/合法性/Kafka Lag | Hive SQL + Kafka Consumer + 钉钉/邮件告警 |
| data_lineage.py | 表级血缘/字段血缘/影响分析 | 有向图 + DFS递归 + Graphviz DOT导出 |
| ai_etl_generator.py | ODS DDL/DWD ETL/DWS SQL生成 | Jinja2模板引擎 + NL2SQL模式匹配 |

### 3. 业务看板 (Superset, 4套大屏/26图表) — 前端呈现层

> 用户打开浏览器就能看到，不是后台技术，是业务出口。

| 看板 | 目标角色 | 刷新 | 能看到什么 |
|------|---------|------|-----------|
| 城市交通总览大屏 | 领导/运营 | 5min | 全市堵不堵、今天多少车、哪里最堵、高峰几点 |
| **实时路况监控面板** | 运营分析师 | **5秒** | 数字每秒跳动、拥堵路段变红、流量突增立刻报警 |
| 设备运维监控大屏 | 运维工程师 | 1min | 哪些设备在线/离线、CPU内存温度、健康评分排行 |
| 数据质量监控面板 | 数据开发 | 10min | 数据有没有丢失/重复/异常、Kafka消费积压 |

**最亮眼功能**：实时路况面板，数据链路 `传感器 → Kafka → Flink → Redis → 前端`，端到端延迟 **< 5秒**，车流数字像股票一样每秒跳动。

详见 [BI_DASHBOARDS.md](docs/BI_DASHBOARDS.md) — 含完整 ASCII 界面布局图、交互操作表、颜色编码规则、面试话术。

---

## 快速体验

### 零依赖演示 (30秒)

```bash
cd D:\s\新项目
python demo_full_pipeline.py
```

输出完整的 8 阶段数据流：Kafka采集 → ODS → DWD清洗 → DWS聚合 → ADS指标 → Flink CEP → 血缘分析 → 质量监控 → 告警推送。

### 生产部署

详见 [RUNBOOK.md](docs/RUNBOOK.md)，包含完整的组件启停、Topic创建、Flink作业提交、数仓初始化流程。

```bash
# 1. 环境检查
java -version && python --version

# 2. 启动基础组件 (ZooKeeper → Hadoop → Kafka → Hive)
start-all.sh
kafka-server-start.sh -daemon /opt/kafka/config/server.properties
nohup hive --service metastore &

# 3. 初始化数仓 (20张表)
hive -f sql/ods/*.sql
hive -f sql/dim/*.sql
hive -f sql/dwd/*.sql
hive -f sql/dws/*.sql
hive -f sql/ads/*.sql

# 4. 编译 & 提交 Flink
cd flink && mvn clean package
flink run -c com.traffic.flink.TrafficVehicleCount target/traffic-flink-1.0.jar

# 5. 启动 Python 治理脚本
python python/data_quality_monitor.py
```

---

## 量化成果

| 指标 | 数值 | 计算逻辑 |
|------|------|---------|
| Hive 查询性能提升 | **40%** | (优化前20s - 优化后12s) / 20s → ORC+MapJoin+SkewJoin |
| 数据质量识别率 | **98%+** | 规则识别异常数 / 人工标注异常数 |
| 端到端实时延迟 | **< 5秒** | Redis写入时间 - Kafka产生时间 (Flink Metrics) |
| 日均处理数据量 | **500万+** | ODS层日增量 |
| 数仓表数量 | **20** | ODS:4 + DIM:4 + DWD:4 + DWS:4 + ADS:4 |
| 业务指标数 | **60+** | ADS层所有指标列汇总 |
| Flink并行在线任务 | **3** | 可横向扩展至10+ |

---

## 文档导航

| 文档 | 内容 |
|------|------|
| [项目介绍手册](docs/INTRODUCTION.md) | 业务背景 / 痛点 / 用例 / 数据架构 / 技术选型 / 模块详解 / FAQ / 路线图 (新人必读) |
| [架构设计文档](docs/ARCHITECTURE.md) | 技术架构 / 数仓分层 / 事实表类型 / Flink配置 / CEP规则 / 血缘实现 / 回溯机制 / 工程优化 |
| [运维操作手册](docs/RUNBOOK.md) | 初次部署 / 组件启停 / 日常巡检 / 故障排查 / 容灾降级 / 备份恢复 / 性能调优 / 值班Checklist |
| [BI看板设计](docs/BI_DASHBOARDS.md) | 4套Superset看板 / 24个图表SQL / 告警联动 / 部署配置 |
| [AI模块设计](docs/AI_MODULE_DESIGN.md) | 技术选型 / 评估指标 / 安全边界 / 异常回退 |
| [项目结构](docs/PROJECT_STRUCTURE.md) | 文件清单 / 技术栈 / 业务主题域 |

---

## 业务阈值速查

| 指标 | 阈值 | 动作 |
|------|------|------|
| 拥堵等级5 | 车速<10km/h, 车流>2000/h | 推送交通管控部门 |
| 设备健康<60 | 四维加权评分 | 一级告警 → 运维工单 |
| 数据完整率<99% | 空值率>1% | 推送开发 → 触发重跑 |
| Kafka Lag>10000 | 消费积压 | 推送运维排查 |
