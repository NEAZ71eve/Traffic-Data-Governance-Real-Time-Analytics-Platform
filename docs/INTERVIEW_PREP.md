# 面试准备文档

> 基于 **智慧城市交通数据治理实时分析平台** 项目  
> 适用岗位：**数仓实习生** / **大数据开发实习生**  
> 生成日期：2026-06-17

---

## 目录

1. [项目介绍（必背）](#1-项目介绍必背)
2. [数仓建模与SQL（数仓实习生重点）](#2-数仓建模与sql数仓实习生重点)
3. [Flink 实时计算（大数据开发重点）](#3-flink-实时计算大数据开发重点)
4. [数据治理与AI模块](#4-数据治理与ai模块)
5. [大数据生态组件](#5-大数据生态组件)
6. [系统架构与设计权衡](#6-系统架构与设计权衡)
7. [场景面试题](#7-场景面试题)
8. [简历项目描述模板](#8-简历项目描述模板)

---

## 1. 项目介绍（必背）

### 一句话介绍

> 这是一个智慧城市交通数据治理与实时分析平台，模拟车联网设备上报的交通时序数据，搭建了 Hadoop/Hive 离线数仓 + Flink 实时流计算 + Kafka 消息队列 + DolphinScheduler 调度的一体化大数据平台，覆盖 ODS→DIM→DWD→DWS→ADS 五层数仓建模及全链路 ETL。

### 技术栈一句话

> 数据采集层用 DataX/Maxwell/Flume，消息队列用 Kafka，实时计算用 Flink（3个作业：车流统计、拥堵检测、CEP异常检测），离线数仓用 Hive 4.0（ORC+Snappy 列存），调度用 DolphinScheduler，可视化用 Superset + Flask，AI辅助用 Python 自研（NL2SQL/异常检测/ETL生成）。

### 项目规模

| 指标 | 数值 |
|------|------|
| 数仓表 | 24 张（ODS 7 + DIM 4 + DWD 4 + DWS 4 + ADS 5） |
| Kafka 消息 | 250,000 条（含 vehicle 100K + status 50K + device 100K） |
| Flink 作业 | 3 个（代码就绪，TrafficVehicleCount 已提交运行） |
| Docker 容器 | 24 个服务（生产集群模式） |
| AI 模块 | 6/6 测试通过（100%） |
| 端到端延迟 | < 5 秒 |

### 典型面试问题

**Q: 为什么做这个项目？**
> 模拟车联网（卡口/传感器/信号灯）上报的交通数据，搭建完整的大数据平台。核心痛点是传统离线报表 T+1 产出无法支撑实时拥堵疏导和设备故障快速响应，所以采用实时+离线双链路架构。

**Q: 项目最大的挑战是什么？**
> 数据倾斜问题——主干道流量远高于普通道路。解决方案是随机前缀打散 + 两阶段聚合，在 dws_road_hour_flow.sql 中实现：阶段1给热点 key 加 0~9 随机前缀做局部聚合，阶段2去掉前缀再做全局聚合，Shuffle 减少了 99.5%。

---

## 2. 数仓建模与SQL（数仓实习生重点）

### 2.1 数仓分层架构

```
ODS  (操作数据层)   ─── TEXTFILE   ─── 7张表 ─── 保留90天
  ↓
DIM  (维度层)      ─── ORC+Snappy ─── 4张表 ─── SCD2永久
DWD  (明细清洗层)   ─── ORC+Snappy ─── 4张表 ─── 保留90天
  ↓
DWS  (轻度汇总层)   ─── ORC+Snappy ─── 4张表 ─── 保留365天
  ↓
ADS  (应用指标层)   ─── ORC+Snappy ─── 5张表 ─── 保留365天
```

**各层职责**：

| 层 | 职责 | 表名举例 |
|----|------|---------|
| ODS | 原始数据贴源层，保持数据原样 | ods_vehicle_pass_di |
| DIM | 维度关联，SCD2缓慢变化维 | dim_road_zip |
| DWD | 数据清洗（去重、空值过滤、值域校验） | dwd_vehicle_pass_di |
| DWS | 按主题轻度汇总（道路×小时、区域×小时） | dws_road_hour_flow |
| ADS | 面向应用输出指标 | ads_traffic_operation |

### 2.2 面试高频：DWD层清洗逻辑

```sql
-- 文件: sql/dwd/dwd_vehicle_pass_di.sql
INSERT OVERWRITE TABLE traffic_db.dwd_vehicle_pass_di PARTITION (dt)
SELECT
    vehicle_id, road_id, device_id,
    CAST(pass_time AS TIMESTAMP) AS pass_time,
    CAST(speed AS INT) AS speed,
    COALESCE(direction, 'UNKNOWN') AS direction,  -- 空值填充
    CASE
        WHEN vehicle_type IN ('小型车', '中型车', '大型车') THEN vehicle_type
        ELSE '其他'
    END AS vehicle_type,
    HOUR(pass_time) AS hour,                    -- 派生字段
    CAST(ROUND(speed / 3600.0, 4) AS DECIMAL(10,4)) AS speed_km_per_s,
    dt
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id, pass_time ORDER BY pass_time) AS rn
    FROM traffic_db.ods_vehicle_pass_di
    WHERE dt = '${date}'
      AND vehicle_id IS NOT NULL                -- 非空校验
      AND road_id IS NOT NULL
      AND speed >= 0 AND speed <= 200           -- 值域校验：车速0~200km/h
) t
WHERE rn = 1;                                    -- 去重
```

**面试问题**:
- **Q: 你怎么做数据清洗的？** → 四步：①非空校验 ②值域校验(speed 0-200) ③ROW_NUMBER去重 ④COALESCE空值填充 + 派生字段
- **Q: ROW_NUMBER() OVER 的用法？** → 按 vehicle_id + pass_time 分组排序，取 rn=1 保留第一条，实现精确去重
- **Q: 为什么要 HOUR(pass_time) 作为派生字段？** → 后续 DWS 层按小时聚合时避免重复计算，属于"提前计算"的优化思想

### 2.3 面试高频：DWS层数据倾斜治理

```sql
-- 文件: sql/dws/dws_road_hour_flow.sql (两阶段聚合)
-- 阶段1: 加随机前缀打散热点，局部聚合
SELECT
    CONCAT(CAST(FLOOR(RAND() * 10) AS STRING), '_', road_id) AS skew_key,
    COUNT(1) AS cnt, SUM(speed) AS total_spd, ...
FROM traffic_db.dwd_vehicle_pass_di
WHERE dt = '${date}'
GROUP BY CONCAT(CAST(FLOOR(RAND() * 10) AS STRING), '_', road_id), hour, dt
) t1
-- 阶段2: 去掉前缀，全局聚合
SELECT
    SUBSTR(skew_key, 3) AS road_id,
    SUM(cnt) AS traffic_count, ...
GROUP BY SUBSTR(skew_key, 3), hour, dt
```

**面试问题**:
- **Q: 数据倾斜怎么处理？** → 随机前缀打散 + 两阶段聚合。阶段1在热点 key 前加 0~9 随机前缀，将热点数据分散到10个 Reducer 做局部聚合；阶段2去掉前缀做全局聚合。Shuffle 减少 99.5%。
- **Q: 还有哪些倾斜优化手段？** → Hive 侧开启 `hive.optimize.skewjoin=true` + `hive.skewjoin.key=100000`，MapJoin 自动转换小表。

### 2.4 面试高频：SCD2拉链表

```sql
-- 文件: sql/dim/dim_road_zip.sql
CREATE TABLE traffic_db.dim_road_zip (
    road_id     STRING,
    road_name   STRING,
    ...
    start_time  STRING,  -- 拉链生效时间
    end_time    STRING,  -- 拉链失效时间，9999-12-31 表示当前有效
    is_current  STRING   -- Y/N 是否为当前记录
)
PARTITIONED BY (dt STRING)
STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');
```

**增量更新逻辑**（4个UNION ALL）:

```
1. 保留未变更的当前记录（原样保留）
2. 闭合变更的旧版本：end_time = 昨天, is_current = 'N'
3. 插入变更的新版本：start_time = 今天, end_time = '9999-12-31', is_current = 'Y'
4. 插入新增道路：start_time = 今天, is_current = 'Y'
```

**面试问题**:
- **Q: SCD2 是什么？为什么用？** → 缓慢变化维类型2，保留历史版本。比如道路改名后，旧报表查历史数据关联旧名，新报表关联新名。比直接覆盖更灵活。
- **Q: 查询当前有效记录怎么查？** → `WHERE is_current = 'Y'`
- **Q: 查历史某个日期的版本呢？** → `WHERE dt = '历史日期'` 拿到该日期的全量快照
- **Q: 拉链表的分区策略是什么？** → 按 dt 日期分区，每天一个全量快照，通过 start_time/end_time 标记版本有效区间

### 2.5 事实表类型

| 表 | 类型 | 粒度 | 含义 |
|----|------|------|------|
| dwd_vehicle_pass_di | 事务型事实表 | 一次车辆通行事件 | 不可再分的最小粒度 |
| dwd_device_status_di | 周期型事实表 | 1分钟设备心跳快照 | 周期性采集指标 |
| dws_road_hour_flow | 累积快照事实表 | 道路+小时 | 从明细聚合到小时粒度 |

### 2.6 面试高频SQL题

**Q: 写SQL找出今天最拥堵的5条路**

```sql
SELECT r.road_name, f.traffic_count, f.avg_speed, f.jam_level
FROM dws_road_hour_flow f
JOIN dim_road_zip r ON f.road_id = r.road_id AND r.is_current = 'Y'
WHERE f.dt = CURRENT_DATE
ORDER BY f.jam_level DESC, f.traffic_count DESC
LIMIT 5;
```

**Q: 写SQL计算设备健康评分（四维加权）**

```sql
SELECT device_id,
    ROUND(online_rate / 100 * 40                    -- 在线率40%
        + GREATEST(0, (1 - avg_cpu/100)) * 30       -- 资源30%
        + GREATEST(0, (1 - abnormal_rate/100)) * 30 -- 故障30%
    , 2) AS health_score
FROM ads_device_health_score WHERE dt = CURRENT_DATE;
```

**Q: 窗口函数有什么实际应用？**
> 本项目 DWD 层用 `ROW_NUMBER() OVER (PARTITION BY vehicle_id, pass_time ORDER BY pass_time)` 做数据去重；ADS 层用 `MAX(CASE WHEN ...)` 做行转列；DWS 层用聚合函数做窗口聚合。

### 2.7 面试高频：SQL性能调优

| 优化项 | 本项目配置 |
|--------|-----------|
| 存储格式 | ORC + Snappy 列存（比 TEXTFILE 减少 70% IO） |
| 分区 | `hive.exec.dynamic.partition=true`，按 dt 天分区 |
| MapJoin | `hive.auto.convert.join=true`，小表自动广播 |
| 倾斜 | `hive.optimize.skewjoin=true` + 随机前缀两阶段聚合 |
| 小文件 | `hive.merge.mapfiles=true`，merge.size.per.task=256MB |
| 向量化 | `hive.vectorized.execution.enabled=true` |
| 并行执行 | `hive.exec.parallel=true`，thread.number=8 |

---

## 3. Flink 实时计算（大数据开发重点）

### 3.1 三个作业总览

| 作业 | 输入 | 输出 | 核心逻辑 | 行号参考 |
|------|------|------|---------|---------|
| TrafficVehicleCount | Kafka traffic_vehicle | Redis | 5min滚动窗口聚合车流 | TrafficVehicleCount.java |
| TrafficCongestionDetection | Kafka traffic_status | Print/Kafka | 拥堵聚合 + KeyedState流量突增检测 | TrafficCongestionDetection.java |
| DeviceStatusCEP | Kafka device_status | Kafka/Redis | CEP连续模式匹配（离线/CPU/温度） | DeviceStatusCEP.java |

### 3.2 面试高频：Checkpoint 配置

```java
// 核心配置
env.enableCheckpointing(5 * 60 * 1000);                    // 5分钟触发一次
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // Exactly-Once语义
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000); // 最小间隔1分钟
env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);    // 超时10分钟
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);          // 同时最多1个
env.getCheckpointConfig().enableExternalizedCheckpoints(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 取消时保留
env.setStateBackend(new HashMapStateBackend());               // 状态后端
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(60))); // 3次重试
```

**面试问题**:
- **Q: Checkpoint 怎么保证 Exactly-Once？** → Flink 通过分布式快照（Barrier对齐）实现。Source 端记录 Kafka Offset，Operator 端保存状态快照，Sink 端事务提交。三者原子化完成即 Exactly-Once。
- **Q: Checkpoint 和 Savepoint 区别？** → Checkpoint 是自动周期触发的，用于故障恢复；Savepoint 是手动触发的，用于作业升级/迁移。代码中配置了 `RETAIN_ON_CANCELLATION`，即取消时保留 Checkpoint，也可以当 Savepoint 用。
- **Q: 重启策略为什么用 fixedDelay 3次60秒？** → 应对临时性故障（网络抖动、资源不足）。3次内如果恢复则继续处理，3次都失败说明有根本性问题，需要人工介入。
- **Q: HashMapStateBackend 和 RocksDB 区别？** → HashMapStateBackend 基于内存，延迟低但状态大小受限；RocksDB 基于磁盘（LSM-Tree），可支持 GB 级大状态，但吞吐稍低。代码注释中写了切换方法。

### 3.3 面试高频：Watermark 机制

```java
.assignTimestampsAndWatermarks(
    WatermarkStrategy.<Tuple3<...>>forBoundedOutOfOrderness(Duration.ofSeconds(30))
        .withTimestampAssigner((element, recordTimestamp) -> element.f1)  // 取pass_time
        .withIdleness(Duration.ofSeconds(5))  // 5秒空闲检测
)
```

**面试问题**:
- **Q: Watermark 解决什么问题？** → 解决乱序数据。设备上报可能有网络延迟导致数据乱序到达，Watermark 告诉 Flink "当前时间之前的数据已经到齐了，可以触发窗口计算了"。本项目设 30 秒乱序容忍。
- **Q: 空闲超时 `.withIdleness(5s)` 有什么用？** → 如果某个 Kafka 分区长时间没有数据，Watermark 会停滞不前，导致整个窗口不触发。空闲检测让 Flink 忽略这个分区，其他分区的 Watermark 正常推进。
- **Q: 迟到数据怎么处理？** → 代码中注释了 `sideOutputLateData` 方案——迟到数据输出到侧输出流，后续写入 HDFS 做离线补录（T+1 修复 DWD/DWS 层）。

### 3.4 面试高频：CEP 模式匹配

```java
// 连续离线检测：3条OFFLINE，180秒内
Pattern.<DeviceStatus>begin("offline_1")
    .where(status -> "OFFLINE".equals(status.onlineFlag))
    .next("offline_2")
    .where(status -> "OFFLINE".equals(status.onlineFlag))
    .next("offline_3")
    .where(status -> "OFFLINE".equals(status.onlineFlag))
    .within(Time.seconds(180));
```

**4条CEP规则**:

| 规则 | 实现 | 窗口 | 级别 |
|------|------|------|------|
| 连续离线 | begin→next→next (3次) | 180s | CRITICAL |
| CPU高负载 | begin→next→next (3次 > 90%) | 5min | MAJOR |
| 温度过高 | begin→next (2次 > 80°C) | 10min | MAJOR |
| 高频告警 | timesOrMore(10).consecutive() | 5min | CRITICAL |

**面试问题**:
- **Q: CEP 和普通窗口计算的区别？** → 窗口计算关注"一段时间内的聚合值"（如 SUM/COUNT），CEP 关注"事件之间的时序关系"（如 A发生后B再发生C）。本项目 CEP 检测"连续3次离线"这种模式，普通窗口做不到。
- **Q: `.next()` 和 `.followedBy()` 区别？** → `next()` 要求严格连续（事件紧挨着），`followedBy()` 允许中间有其他事件。本项目用 `next()` 确保严格的连续异常检测。
- **Q: 为什么告警级别分 CRITICAL 和 MAJOR？** → 离线影响最严重（设备完全不可用），所以 CRITICAL；CPU/温度高是性能问题，MAJOR。

### 3.5 面试高频：KeyedState 流量突增检测

```java
// 文件: TrafficCongestionDetection.java, FlowAnomalyDetector 类
// 用 ValueState 维护历史流量均值（EMA指数移动平均）
Double historyAvg = historicalAvgFlow.value();
int currentFlow = value.f2;

// 流量突增检测：当前 > 历史均值 * 2
if (currentFlow > historyAvg * 2 && count >= 5) {
    out.collect("ANOMALY|FLOW_SPIKE|..." + currentFlow + historyAvg);
}

// 指数移动平均更新（alpha=0.2，更关注近期趋势）
double newAvg = historyAvg * 0.8 + currentFlow * 0.2;
historicalAvgFlow.update(newAvg);
```

**面试问题**:
- **Q: KeyedState 是什么？** → 按 key 分区的状态存储。本项目按 roadId 分区，每条道路维护自己的历史流量均值。
- **Q: 为什么用 EMA（指数移动平均）而不是简单平均？** → EMA 给近期数据更高权重（alpha=0.2），对流量变化更敏感。简单平均会让历史老旧数据影响当前判断。
- **Q: Timer 用来做什么？** → 24小时后自动清理过期状态，防止状态无限膨胀。

---

## 4. 数据治理与AI模块

### 4.1 数据质量四维监控

```python
# 文件: python/data_quality_monitor.py
# AlertNotifier 类：钉钉/邮件多渠道告警
```

| 维度 | 规则 | 阈值 | 动作 |
|------|------|------|------|
| 完整性 | 关键字段空值率 | < 1% | 推送开发 |
| 准确性 | 值域校验（车速0-200） | 违反 < 0.1% | 推送开发 |
| 唯一性 | 主键重复率 | < 0.1% | 推送开发 |
| 及时性 | Kafka Lag | < 10000 | 推送运维 |

**告警升级机制**:

```
MINOR → 邮件通知 → 30分钟未恢复升级
MAJOR → 钉钉群+邮件 → 60分钟未恢复升级
CRITICAL → 钉钉@所有人+短信+电话+工单
```

### 4.2 数据血缘追踪

```python
# 文件: python/data_lineage.py
# DataLineageManager 类：有向图 + DFS 递归

# 构建血缘图
ods_vehicle_pass_di → dwd_vehicle_pass_di → dws_road_hour_flow → ads_traffic_operation
                                                                  → ads_top_jam_roads
dwd_device_status_di → dws_device_health_day → ads_device_health_score
                                              → ads_device_mtbf_mttr
dim_road_zip → dws_area_jam_hour, ads_top_jam_roads

# 上游溯源：从 ads_traffic_operation 反向查找
# → dws_road_hour_flow, dws_area_jam_hour
# → dwd_vehicle_pass_di, dwd_traffic_status_di
# → ods_vehicle_pass_di, ods_traffic_status_di, dim_road_zip, dim_area, dim_time

# 影响分析：修改 ods_vehicle_pass_di 会影响哪些下游？
# → dwd_vehicle_pass_di → dws_road_hour_flow → ads_traffic_operation
```

### 4.3 AI 辅助模块

| 模块 | 文件 | 功能 |
|------|------|------|
| NL2SQL | python/nl2sql_enhanced.py | 8种查询意图识别，自然语言→Hive SQL |
| 异常检测 | python/ai_anomaly_detector.py | 自定义 Isolation Forest，3类异常检测 |
| ETL生成 | python/ai_etl_generator.py | ODS→DWD→DWS SQL模板生成 |

**NL2SQL 示例**:
> 输入："昨天早高峰最拥堵的 10 条路"
> 输出：`SELECT road_name, avg_speed FROM ads_top_jam_roads WHERE dt = '${yesterday}' AND peak_flag = 'PEAK_HOUR' ORDER BY jam_level DESC LIMIT 10`

**安全边界**: AI 模块只读查询/离线分析/开发辅助，不接入核心数仓链路，可一键关闭不影响核心业务。

---

## 5. 大数据生态组件

### 5.1 Hive

| 配置 | 本项目值 |
|------|---------|
| 版本 | 4.0.0 |
| 存储 | ORC + Snappy（列存，压缩比 ~3:1） |
| 分区 | 动态分区，按 dt 天分区 |
| 执行引擎 | Tez（优于 MapReduce） |
| 优化 | CBO、MapJoin、Vectorization |

### 5.2 Kafka

| Topic | 分区 | 副本 | 数据量/天 |
|-------|------|------|----------|
| traffic_vehicle | 8 | 3 | ~300万 |
| traffic_status | 4 | 3 | ~100万 |
| device_status | 4 | 3 | ~50万 |
| device_alarm | 4 | 3 | ~5万 |

### 5.3 DolphinScheduler

**调度 DAG 依赖**:
```
ODS层 → DIM层 (并行) → DWD层 → DWS层 → ADS层 → 数据质量检查 → 分区清理
```

### 5.4 组件选型理由

| 组件 | 为什么选这个 |
|------|------------|
| Kafka | 生态最成熟，Flink 原生支持 Kafka Connector |
| Flink | 真正流处理（非微批），Exactly-Once 语义，自带 CEP 库 |
| Hive | Hadoop 生态集成度最高，ORC+Snappy 列存适合离线分析 |
| DolphinScheduler | 分布式、可视化 DAG、补数重跑功能强 |
| Superset | 开源、SQL IDE、丰富图表、角色权限 |
| Prometheus+Grafana | 云原生监控标准，社区活跃 |

---

## 6. 系统架构与设计权衡

### 6.1 实时+离线双链路

```
实时链路: 传感器 → Kafka → Flink → Redis → Superset (延迟 <5s)
离线链路: Flume/Maxwell → Kafka → HDFS → Hive ODS→DIM→DWD→DWS→ADS (T+1)
```

**为什么双链路？**
> 实时链路满足秒级监控需求（拥堵预警、设备故障告警），但窗口计算只保留短期数据。离线链路做全量历史数据分析和复杂 ETL，两者互补。面试中经常问这种"为什么既要实时又要离线"的架构权衡。

### 6.2 数仓建模方法论

| 维度建模概念 | 本项目实践 |
|-------------|-----------|
| 星型模型 | 事实表 dwd_vehicle_pass_di 居中，dim_road_zip/dim_time 等维度表环绕 |
| 事实表类型 | 事务型（通行事件）、周期型（设备心跳）、累积快照（小时聚合） |
| 缓慢变化维 | SCD2 拉链表（dim_road_zip / dim_device_zip） |
| 退化维度 | vehicle_type、direction 直接放在事实表中 |

### 6.3 工程优化

| 问题 | 方案 | 效果 |
|------|------|------|
| 数据倾斜 | 随机前缀 + 两阶段聚合 | Shuffle 减少 99.5% |
| 小文件 | ORC block size=256MB + CONCATENATE 合并 | 文件数减少 99.2% |
| Join 慢 | MapJoin 自动转换 | 比 ReduceJoin 快 3.52x |
| 查询慢 | ORC + Snappy + 向量化 + CBO | IO 减少 70% |

---

## 7. 场景面试题

### 场景1：数据倾斜

> **面试官**：你的 DWS 层按 road_id 聚合时，长安街的车流量是普通道路的 100 倍，怎么处理？

**回答**：两阶段聚合。第一阶段给 road_id 加随机前缀（0~9），把热点数据打散到 10 个 Reducer 做局部聚合；第二阶段去掉前缀再做全局聚合。代码在 `dws_road_hour_flow.sql` 的 57-86 行实现。此外还在 Hive 侧开启了 `hive.optimize.skewjoin=true` 配合 MapJoin 自动转换小表。

### 场景2：Flink 作业崩溃

> **面试官**：你的 Flink 作业半夜崩溃了，怎么保证数据不丢？

**回答**：配置了 Checkpoint 每 5 分钟一次，Exactly-Once 语义。崩溃后 Flink 自动从最近一次成功的 Checkpoint 恢复，重启策略是固定延迟 3 次、每次间隔 60 秒。如果 3 次都失败，Prometheus 告警规则 `Flink-Checkpoint-Fail` 会触发 MAJOR 级别告警，推送钉钉群通知值班运维。

### 场景3：数据回溯

> **面试官**：发现上周的 ODS 数据有误，需要重新跑 ETL，怎么做？

**回答**：三步走。第一步删掉目标分区 `ALTER TABLE DROP PARTITION`，第二步按依赖顺序 `ODS → DIM → DWD → DWS → ADS` 逐层执行 ETL 脚本，传参 `date=上周日期`，第三步运行数据质量监控验证。DolphinScheduler 的补数功能也可以一键选择日期范围自动重跑全链路。

### 场景4：维度变更

> **面试官**：长安街改名了，报表要既能查新名也能查旧名，怎么设计？

**回答**：用 SCD2 拉链表。dim_road_zip 表有 start_time/end_time/is_current 三个字段。长安街改名时，旧记录 `end_time` 设为改名前一天、`is_current='N'`，新记录 `start_time` 设为改名当天、`end_time='9999-12-31'`、`is_current='Y'`。查当前数据用 `is_current='Y'`，查历史数据用 `dt` 分区拿到当时的快照。

### 场景5：慢查询优化

> **面试官**：有个 Hive 查询跑 20 分钟，怎么优化？

**回答**：从存储、计算、SQL 三个层面排查。存储层检查是否用了 ORC+Snappy 列存（比 TEXTFILE 减少 70% IO）；计算层检查是否开启 CBO、MapJoin、向量化、并行执行；SQL 层检查是否有数据倾斜（热点 key）、是否有不必要的全表扫描、是否合理利用分区裁剪。本项目的性能基线显示，优化后查询从 20 秒降到 12 秒（40%）。

---

## 8. 简历项目描述模板

### 简版（数仓实习生）

> **智慧城市交通数据治理实时分析平台**
> 
> 基于 Kafka + Flink + Hive + DolphinScheduler 搭建的交通大数据平台，覆盖 ODS→DIM→DWD→DWS→ADS 五层数仓。
> 
> - 设计 24 张 Hive 数仓表（ORC+Snappy），实现 SCD2 拉链表（dim_road_zip/dim_device_zip）保留历史版本
> - 编写 DWD 层 ETL（数据清洗去重/空值过滤/值域校验）和 DWS 层聚合（含随机前缀两阶段聚合解决数据倾斜）
> - 开发 ADS 层业务指标（交通运营指标/设备健康评分/MTBF/MTTR），数据质量四维监控（完整率/唯一率/合法性/时效性）
> - 使用 DolphinScheduler 编排天级 ETL 任务，实现补数回溯

### 详版（大数据开发实习生）

> **智慧城市交通数据治理实时分析平台**
> 
> 全链路大数据平台：Kafka 采集 → Flink 流计算 → Hive 数仓分层 → Superset 可视化 → AI 辅助治理
> 
> - **实时计算**：开发 3 个 Flink 作业（TrafficVehicleCount 车流统计→Redis、TrafficCongestionDetection 拥堵检测→Kafka、DeviceStatusCEP CEP异常检测→Kafka/Redis），配置 Checkpoint 5min/EXACTLY_ONCE + Watermark 30s + 3次重启策略
> - **离线数仓**：基于 Kimball 维度建模设计 24 张表（ODS 7 + DIM 4 + DWD 4 + DWS 4 + ADS 5），ORC+Snappy 列存，SCD2 拉链表（start_time/end_time/is_current 三字段版本管理）
> - **数据治理**：Python 自研四维数据质量监控（钉钉/邮件多渠道告警，30分钟去重抑制）+ 有向图 DFS 血缘追踪（ADS→DWS→DWD→ODS 反向溯源）
> - **AI 辅助**：实现 NL2SQL（8种查询意图）、自定义 Isolation Forest 异常检测（3类）、ETL SQL 模板生成，6/6 模块测试通过
> - **工程优化**：随机前缀两阶段聚合解决数据倾斜（Shuffle 减少 99.5%）、ORC+Snappy 减少 70% IO、MapJoin 加速 3.52x
> - **部署运维**：Docker Compose 编排 24 个容器（Kafka 3节点 KRaft + Flink HA 双JM + Redis 3主3从 Cluster），Prometheus+Grafana 11面板运维大屏 + ELK 日志收集

---

## 附录：关键代码索引

| 模块 | 文件 | 核心内容 |
|------|------|---------|
| Flink 车流统计 | flink/.../TrafficVehicleCount.java | Checkpoint配置、Watermark策略、5min窗口聚合、Redis Sink |
| Flink 拥堵检测 | flink/.../TrafficCongestionDetection.java | KeyedState+EMA流量突增检测、Timer状态清理 |
| Flink CEP | flink/.../DeviceStatusCEP.java | 4条CEP规则（连续离线/CPU/温度/高频）、多路输出 |
| DWD ETL | sql/dwd/dwd_vehicle_pass_di.sql | ROW_NUMBER去重、值域校验、COALESCE空值填充 |
| DWS ETL | sql/dws/dws_road_hour_flow.sql | 随机前缀两阶段聚合、数据倾斜治理 |
| ADS ETL | sql/ads/ads_device_health_score.sql | 四维加权健康评分（0.4×在线率+0.3×资源+0.3×故障） |
| DIM SCD2 | sql/dim/dim_road_zip.sql | start_time/end_time/is_current 拉链表 |
| 数据血缘 | python/data_lineage.py | 有向图构建、DFS递归上游溯源/影响分析 |
| 数据质量 | python/data_quality_monitor.py | 四维监控、钉钉/邮件多渠道告警 |
| NL2SQL | python/nl2sql_enhanced.py | 8种查询意图、实体/指标/时间映射 |
| 异常检测 | python/ai_anomaly_detector.py | 自定义 Isolation Forest |
