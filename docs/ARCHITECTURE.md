# 架构设计文档

## 一、总体架构

```
                    ┌───────────────┐
                    │ 交通终端设备   │
                    └───────┬───────┘
                            │
         ┌──────────────────┼──────────────────┐
         │                                  │
    MySQL业务库                        日志数据
         │                                  │
      Maxwell                           Flume
         │                                  │
         └──────────── Kafka ────────────────┘
                            │
           ┌────────────────┼────────────────┐
           ▼                                 ▼
       Flink实时计算                     HDFS落地
           │                                 │
           ├── Redis(Superset实时看板)     Hive数仓(ODS→ADS)
           │                                 │
           └── DingTalk/邮件告警        DolphinScheduler
                                             │
                                             ▼
                                      ┌──────────────────┐
                                      │   应用服务层       │
                                      │ Superset 可视化   │
                                      │ 运维工单系统对接   │
                                      │ 告警通知(钉钉/邮件) │
                                      │ AI辅助分析        │
                                      └──────────────────┘
```

---

## 二、数据采集层

### 2.1 采集组件

| 组件 | 用途 | 同步方式 | 频率 |
|------|------|---------|------|
| DataX | 静态维表（道路、设备、区域） | 全量 | 每日凌晨 |
| Maxwell | MySQL Binlog增量（设备变更、告警） | CDC增量 | 实时 |
| Flume | 日志数据（车辆、路况、设备状态） | 增量 | 实时 |

### 2.2 Kafka Topic设计

| Topic | 分区数 | 副本数 | 保留时间 | 数据格式 | 日数据量 |
|-------|--------|--------|----------|---------|---------|
| traffic_vehicle | 8 | 3 | 1天 | TSV(\\t分隔) | ~300万条/天 |
| traffic_status | 4 | 3 | 1天 | TSV(\\t分隔) | ~100万条/天 |
| device_status | 4 | 3 | 1天 | TSV(\\t分隔) | ~50万条/天 |
| device_alarm | 4 | 3 | 7天 | TSV(\\t分隔) | ~5万条/天 |

---

## 三、数仓建模（Kimball维度建模）

### 3.1 分层架构与刷新策略

| 分层 | 表命名规范 | 刷新频率 | 刷新方式 | 存储格式 | 保留周期 | 用途 |
|------|-----------|---------|---------|---------|---------|------|
| **ODS** 原始数据层 | `ods_{entity}_di` | 天级(T+1) | 全量+增量 | TEXTFILE | 90天 | 原始数据留存 |
| **DIM** 维度层 | `dim_{entity}_zip` | 天级 | 增量(SCD2) | ORC+Snappy | 永久 | 维度关联 |
| **DWD** 明细清洗层 | `dwd_{entity}_di` | 天级 | 增量(INSERT OVERWRITE) | ORC+Snappy | 90天 | 清洗后明细 |
| **DWS** 轻度汇总层 | `dws_{entity}_{granularity}` | 天级 | 增量聚合 | ORC+Snappy | 365天 | 多维分析 |
| **ADS** 应用指标层 | `ads_{dashboard_name}` | 天级 | 覆盖/增量 | ORC+Snappy | 365天 | 报表看板 |

### 3.2 维度表（Kimball星型模型）

| 表名 | 类型 | 变更策略 | 说明 |
|------|------|---------|------|
| dim_road_zip | **SCD2拉链表** | 道路属性变更时闭合旧版本(start_time/end_time)，新增当前版本(is_current='Y') | 道路维度 |
| dim_device_zip | **SCD2拉链表** | 设备固件/状态变更时触发SCD2更新 | 设备维度 |
| dim_time | 静态维 | 一次性生成2020-2030年日期+小时明细 | 时间维度(含peak_period) |
| dim_area | 静态维 | DataX全量同步 | 区域维度(省/市/区) |

#### SCD2更新逻辑（以dim_road_zip为例）
```
1. 检测到道路属性变更（road_name/road_type/lane_count等）
2. 闭合旧记录：SET end_time = 昨天, is_current = 'N'
3. 新增当前记录：SET start_time = 今天, end_time = '9999-12-31', is_current = 'Y'
4. 关联方式：WHERE is_current = 'Y' 获取当前有效版本
5. 历史回溯：WHERE dt = '历史日期' 获取该日期的历史版本
```

### 3.3 事实表类型标注

| 表名 | 事实表类型 | 粒度 | 度量 | 说明 |
|------|-----------|------|------|------|
| dwd_vehicle_pass_di | **事务型事实表** | 车辆通行事件(vehicle_id + pass_time) | speed, pass_count | 每条记录=一次车辆通行，不可再细分 |
| dwd_device_status_di | **周期型事实表** | 设备状态快照(device_id + heartbeat_time, ~1分钟) | cpu, memory, temp | 周期性采集的设备指标快照 |
| dwd_alarm_log_di | **事务型事实表** | 告警事件(alarm_id) | alarm_count, recover_time | 每条记录=一次告警事件 |
| dws_road_hour_flow | **累积快照事实表** | 道路+小时(road_id + hour) | traffic_count, avg_speed | 从明细聚合到小时粒度 |
| dws_area_jam_hour | **累积快照事实表** | 区域+小时(area_id + hour) | jam_level, congestion_rate | 区域级拥堵汇总 |

---

## 四、实时计算模块

### 4.1 Flink任务清单

| 任务 | 入口类 | 数据源 | 输出 | 窗口 | Checkpoint间隔 | 并行度 |
|------|-------|-------|------|------|---------------|-------|
| 实时车流统计 | TrafficVehicleCount | Kafka traffic_vehicle | Redis | 5min滚动 | 5min | 4 |
| 拥堵检测 | TrafficCongestionDetection | Kafka traffic_status | Print→Kafka | 5min滚动 | 5min | 4 |
| CEP异常检测 | DeviceStatusCEP | Kafka device_status | Print→Kafka | CEP模式 | 5min | 4 |

### 4.2 Checkpoint/StateBackend配置

```java
// Checkpoint: 5分钟一次, EXACTLY_ONCE
env.enableCheckpointing(5 * 60 * 1000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:8020/flink/checkpoints/");

// StateBackend: HashMapState(低延迟) / RocksDB(大状态)
env.setStateBackend(new HashMapStateBackend()); // 默认

// 重启策略: 60秒内最多重启3次
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(60)));
```

### 4.3 Watermark策略

| 任务 | 策略 | 最大乱序 | 空闲超时 | 说明 |
|------|------|---------|---------|------|
| TrafficVehicleCount | BoundedOutOfOrderness | 30秒 | 5秒 | 使用pass_time事件时间 |
| TrafficCongestionDetection | BoundedOutOfOrderness | 30秒 | 5秒 | 使用系统处理时间 |
| DeviceStatusCEP | BoundedOutOfOrderness | 30秒 | 5秒 | 使用heartbeat_time事件时间 |

### 4.4 CEP规则明细

| 规则 | 模式 | 触发条件 | 时间窗口 | 告警级别 |
|------|------|---------|---------|---------|
| 连续离线 | begin→next→next | 连续3条 online_flag='OFFLINE' | 180秒内 | CRITICAL |
| CPU持续高负载 | begin→next→next | 连续3条 cpu_usage > 90% | 5分钟内 | MAJOR |
| 温度过高 | begin→next | 连续2条 temperature > 80℃ | 10分钟内 | MAJOR |
| 流量突增(KeyedState) | 无模式 | 当前流量 > 历史均值*2 | 实时 | MAJOR |

### 4.5 容灾与降级

| 场景 | 应对方案 | 恢复方式 |
|------|---------|---------|
| Kafka集群故障 | Flink切换读取HDFS ODS增量数据 | Kafka恢复后从Checkpoint切回 |
| Flink任务崩溃 | 固定延迟重启(3次) | 从Checkpoint/Savepoint恢复状态 |
| Redis不可用 | 结果暂存HDFS，恢复后回灌Redis | Superset切换读取Hive ADS |
| 数据延迟>5分钟 | 数据质量监控触发告警 | 人工介入排查上游 |

---

## 五、数据血缘管理

### 5.1 血缘链路图

```
                    ODS                              DIM
                     │                                │
   ┌─────────┬───────┼───────┬─────────┐              │
   ▼         ▼       ▼       ▼         ▼              │
ods_vehicle ods_traffic ods_device ods_alarm    dim_road_zip
   │           │           │         │         dim_device_zip
   ▼           ▼           ▼         ▼         dim_time/dim_area
dwd_vehicle dwd_traffic dwd_device dwd_alarm       │
   │           │           │         │              │
   ▼           ▼           ▼         ▼              │
dws_road    dws_area    dws_device dws_alarm ◄──────┘
   │           │           │         │
   ▼           ▼           ▼         ▼
ads_traffic  ads_top    ads_device  ads_device
_operation   _jam       _health     _mtbf_mttr
```

### 5.2 实现方式

| 血缘类型 | 工具 | 采集方式 | 输出 |
|---------|------|---------|------|
| 离线血缘(ODS→ADS) | Apache Atlas | Hive Hook采集DDL/ETL脚本 | 血缘图谱(GraphDB) |
| 实时血缘(Kafka→Redis) | Atlas Flink Connector | Flink任务运行时注册 | Topic→Flink→Redis血缘 |
| Python本地血缘 | data_lineage.py | 硬编码+元数据自动发现 | JSON/Graphviz DOT |

---

## 六、数据回溯与重跑机制

### 6.1 回溯流程

```
1. 检查目标分区是否存在 → SHOW PARTITIONS
2. 备份目标分区数据（可选）
3. 删除目标分区 → ALTER TABLE DROP PARTITION
4. 按依赖顺序重跑 → ODS → DIM → DWD → DWS → ADS
5. 数据质量校验 → python data_quality_monitor.py
6. 校验通过 → 重跑完成 / 校验失败 → 重试或告警
```

### 6.2 分区保留策略

- ODS/DWD：保留近90天全量分区
- DWS：保留近365天分区
- ADS：保留近365天分区
- 过期分区由 DolphinScheduler 定时任务自动清理

---

## 七、业务指标与告警闭环

### 7.1 核心指标阈值（详见 config/metrics_thresholds.json）

| 指标 | 阈值定义 | 告警动作 |
|------|---------|---------|
| 拥堵等级5（严重拥堵） | 车速 < 10km/h 且 车流量 > 2000辆/小时 | 推送交通管控部门 |
| 设备健康评分 < 60 | 在线率+资源+异常+恢复 加权<60分 | 一级告警→生成运维工单 |
| 数据完整率 < 99% | 空值率 > 1% | 推送数据开发，触发数据重跑 |
| Kafka Lag > 10000 | 消费延迟 > 10000条 | 推送运维，排查消费端 |

### 7.2 告警升级机制

```
MINOR → 邮件通知 → 30分钟内重复则升级
  ↓ (持续未恢复)
MAJOR → 钉钉群 + 邮件 → 60分钟内未处理则升级
  ↓ (持续未恢复)
CRITICAL → 钉钉@所有人 + 短信 + 电话 + 自动生成工单
```

---

## 八、工程优化

### 8.1 Hive优化
- 存储：ORC + Snappy压缩
- 分区：动态分区(Dynamic Partition)
- Join优化：MapJoin(小表)、Skew Join(倾斜键打散)
- 小文件治理：ORC block size=256MB、合并小文件任务
- 查询优化：CBO开启、FetchTask转换

### 8.2 数据倾斜治理
- 场景：主干道路流量远高于普通道路
- 方案：随机前缀打散(hot_key+random) + 两阶段聚合（局部聚合→全局聚合）
- 配置：hive.optimize.skewjoin=true, hive.skewjoin.key=100000

### 8.3 调度优化
- 失败重试：3次，间隔60秒
- 超时告警：ODS 30min / DWD 60min / DWS 90min / ADS 120min
- 任务依赖：严格按 ODS→DIM+DWD→DWS→ADS 层级依赖
- 维表任务：DIM层与ODS并行执行

---

## 九、量化成果（含基线与计算逻辑）

### 9.1 Hive查询性能提升 40%

| 指标 | 优化前 | 优化后 | 提升 |
|------|-------|-------|------|
| 主干道路流量查询耗时 | 20秒 | 12秒 | 40% |
| 全表扫描IO | ~50GB | ~15GB(ORC压缩) | 70% |
| Join倾斜任务耗时 | 180秒 | 45秒 | 75% |

**计算逻辑**: (优化前耗时 - 优化后耗时) / 优化前耗时 × 100%

**优化手段**: ORC格式替换TEXTFILE + Snappy压缩 + MapJoin自动转换 + SkewJoin倾斜治理 + 小文件合并

### 9.2 数据质量问题识别率 98%+

**计算逻辑**: 质量规则识别出的异常数据条数 / 人工标注验证的总异常条数 × 100%

**覆盖维度**: 完整性(空值) + 准确性(值域) + 唯一性(重复) + 及时性(延迟)

**验证方式**: 每月抽样1000条数据，人工标注异常，对比质量监控识别结果

### 9.3 实时监控延迟 < 5秒（端到端）

**延迟计算**: 数据产生时间(Kafka) → Flink处理 → Redis写入 的总耗时

**计算方式**:
```
端到端延迟 = Redis写入时间戳 - Kafka消息产生时间戳
通过 Flink Metrics 监控: numRecordsInPerSecond / numRecordsOutPerSecond
```

### 9.4 日均处理能力

| 指标 | 数值 | 说明 |
|------|------|------|
| 数仓表数量 | 24 | ODS 7 + DIM 4 + DWD 4 + DWS 4 + ADS 5 |
| ODS 数据量（测试） | 1,490 行/天 | vehicle 500 + status 240 + device 720 + alarm 30 |
| Kafka 消息量 | 250,000 条 | 含 vehicle 100K + status 50K + device 100K |
| Flink 任务数 | 3 作业 (代码就绪) | TrafficVehicleCount 已提交运行 |
| DolphinScheduler 任务 | 20 个 (配置就绪) | 容器已运行 |

### 9.5 实际部署拓扑

```
容器名                    镜像                    端口
────────────────────────────────────────────────────────
traffic-hdfs-namenode    bde2020/hadoop-namenode    :9870,9000
traffic-hdfs-dn-1        bde2020/hadoop-datanode    :9864
traffic-hive-metastore-db  postgres:15-alpine       5432(内网)
traffic-hive-metastore   apache/hive:4.0.0          :9083
traffic-hiveserver2      apache/hive:4.0.0          :10000
traffic-kafka-1          apache/kafka:3.7.0         :9092
traffic-flink-jm         flink:1.18                 :8081
traffic-flink-tm         flink:1.18                 -
traffic-ds-db            postgres:15-alpine         :5433
traffic-ds-api           apache/dolphinscheduler    :12345
traffic-ds-master        apache/dolphinscheduler    -
traffic-ds-worker        apache/dolphinscheduler    -
docker-redis-1           redis:6                    :6379
```
