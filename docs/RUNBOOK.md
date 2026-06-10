# 运维操作手册 (Operations Runbook)

> 版本: v3.0 | 更新日期: 2026-06-10 | 适用于: Traffic-Data-Governance-Real-Time-Analytics-Platform

---

## 一、系统架构速查

```
traffic-kafka-1:9092 (Kafka 3.7.0 KRaft)
  │
  ├─→ traffic-flink-jm:8081 (Flink 1.18.1)
  │     └─ TrafficVehicleCount (5min滚动窗口, Watermark 30s)
  │          └─ Redis Sink
  │
  └─→ HDFS → Hive (ODS → DWD → DWS → ADS)
         ↑
  traffic-ds-api:12345 (DolphinScheduler 2.0.5)
```

| 组件 | 默认端口 | 用途 | 运行方式 |
|------|---------|------|---------|
| HDFS NameNode | 9870/9000 | HDFS 元数据 | Docker |
| HiveServer2 | 10000 | SQL 查询服务 | Docker |
| Kafka | 9092 | 消息队列 | Docker KRaft |
| Flink JobManager | 8081 | 作业管理 Web UI | Docker |
| Flink TaskManager | 随机 | 作业执行 | Docker |
| Redis | 6379 | 实时缓存 | Docker (Dify) |
| DolphinScheduler API | 12345 | 任务调度 | Docker |
| Hive Metastore/PostgreSQL | 9083/5432(内网) | Hive 元数据 | Docker |

---

## 二、初次部署

### 2.1 Docker 部署（推荐）

```bash
# 1. 拉取镜像
docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
docker pull bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
docker pull apache/hive:4.0.0
docker pull postgres:15-alpine
docker pull flink:1.18-scala_2.12
docker pull apache/kafka:3.7.0
docker pull apache/dolphinscheduler:latest

# 2. 创建网络
docker network create traffic-prod-net

# 3. 启动各组件（分步）
# → HDFS
docker compose -p traffic -f docker-compose-production.yml up -d hdfs-namenode hdfs-datanode-1
# → Hive
docker compose -p traffic -f docker-compose-production.yml up -d hive-metastore-db hive-metastore hiveserver2
# → Kafka
docker run -d --name traffic-kafka-1 --network traffic_traffic-prod-net -p 9092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e CLUSTER_ID=4L6g3nShT-eMCtK--X86sw \
  apache/kafka:3.7.0
# → Flink
docker run -d --name traffic-flink-jm --network traffic_traffic-prod-net -p 8081:8081 flink:1.18-scala_2.12 jobmanager
docker run -d --name traffic-flink-tm --network traffic_traffic-prod-net -e JOB_MANAGER_RPC_ADDRESS=flink-jobmanager flink:1.18-scala_2.12 taskmanager
# → DolphinScheduler
docker compose -p traffic -f docker-compose-phase2.yml up -d dolphinscheduler-db dolphinscheduler-api dolphinscheduler-master dolphinscheduler-worker

# 4. 验证
docker ps
```

### 2.2 配置文件初始化（Windows）

```powershell
# 所有配置文件位于 config/ 目录，按实际环境修改以下占位符：

# config/kafka_topics.json → bootstrap_servers 地址
# config/hive_config.json  → metastore_uris / hive_server2 地址
# config/alert_config.json → dingtalk.webhook_url / email.smtp_host
# config/dolphinscheduler_config.json → dolphinscheduler API 地址
```

### 2.3 创建数据库

```sql
-- 通过 beeline 或 hive CLI 执行
CREATE DATABASE IF NOT EXISTS traffic_db
COMMENT '交通数据治理仓库'
LOCATION '/user/hive/warehouse/traffic_db.db';
```

### 2.4 执行建表脚本（按依赖顺序）

```bash
# 第一步: ODS 层（不需要依赖其他层）
hive -f sql/ods/ods_vehicle_pass_di.sql
hive -f sql/ods/ods_traffic_status_di.sql
hive -f sql/ods/ods_device_status_di.sql
hive -f sql/ods/ods_alarm_log_di.sql

# 第二步: DIM 层（独立于 ODS，可并行）
hive -f sql/dim/dim_road_zip.sql
hive -f sql/dim/dim_device_zip.sql
hive -f sql/dim/dim_time.sql
hive -f sql/dim/dim_area.sql

# 第三步: DWD 层（依赖ODS）
hive -f sql/dwd/dwd_vehicle_pass_di.sql
hive -f sql/dwd/dwd_traffic_status_di.sql
hive -f sql/dwd/dwd_device_status_di.sql
hive -f sql/dwd/dwd_alarm_log_di.sql

# 第四步: DWS 层（依赖DWD+DIM）
hive -f sql/dws/dws_road_hour_flow.sql
hive -f sql/dws/dws_area_jam_hour.sql
hive -f sql/dws/dws_device_health_day.sql
hive -f sql/dws/dws_alarm_day.sql

# 第五步: ADS 层（依赖DWS+DIM）
hive -f sql/ads/ads_traffic_operation.sql
hive -f sql/ads/ads_top_jam_roads.sql
hive -f sql/ads/ads_device_health_score.sql
hive -f sql/ads/ads_device_mtbf_mttr.sql
```

### 2.5 验证建表结果

```sql
USE traffic_db;
SHOW TABLES;
-- 预期输出 20 张表:
-- ods_vehicle_pass_di / ods_traffic_status_di / ods_device_status_di / ods_alarm_log_di
-- dim_road_zip / dim_device_zip / dim_time / dim_area
-- dwd_vehicle_pass_di / dwd_traffic_status_di / dwd_device_status_di / dwd_alarm_log_di
-- dws_road_hour_flow / dws_area_jam_hour / dws_device_health_day / dws_alarm_day
-- ads_traffic_operation / ads_top_jam_roads / ads_device_health_score / ads_device_mtbf_mttr
```

---

## 三、组件启动与停止

### 3.1 启动顺序（必须按依赖关系）

```
第一步: ZooKeeper → 第二步: Hadoop(HDFS+YARN) → 第三步: Kafka
  ↓
第四步: Hive Metastore + HiveServer2
  ↓
第五步: Flink Cluster (需要 YARN)
  ↓
第六步: DolphinScheduler Master + Worker
  ↓
第七步: Redis
  ↓
第八步: Superset
```

### 3.2 启动命令

```bash
# ========== 第一步: ZooKeeper ==========
zkServer.sh start

# ========== 第二步: Hadoop ==========
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
# 验证
hdfs dfsadmin -report
jps | grep -E "NameNode|DataNode|ResourceManager|NodeManager"

# ========== 第三步: Kafka ==========
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
# 验证
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# ========== 第四步: Hive ==========
nohup hive --service metastore > /var/log/hive/metastore.log 2>&1 &
nohup hiveserver2 > /var/log/hive/hiveserver2.log 2>&1 &
# 验证
beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES"

# ========== 第五步: Flink ==========
$FLINK_HOME/bin/start-cluster.sh
# 验证: 打开 http://localhost:8081

# ========== 第六步: DolphinScheduler ==========
$DOLPHINSCHEDULER_HOME/bin/dolphinscheduler-daemon.sh start master-server
$DOLPHINSCHEDULER_HOME/bin/dolphinscheduler-daemon.sh start worker-server
# 验证: 打开 http://localhost:12345

# ========== 第七步: Redis ==========
# Docker 方式: docker run -d -p 6379:6379 redis:6.2
# 原生方式: redis-server /etc/redis/redis.conf
# 验证
redis-cli PING

# ========== 第八步: Superset ==========
superset run -h 0.0.0.0 -p 8088
# 验证: 打开 http://localhost:8088
```

### 3.3 停止命令

```bash
# 按启动的逆序停止

# 停止 Superset
pkill -f "superset run"

# 停止 Redis
redis-cli SHUTDOWN

# 停止 DolphinScheduler
$DOLPHINSCHEDULER_HOME/bin/dolphinscheduler-daemon.sh stop worker-server
$DOLPHINSCHEDULER_HOME/bin/dolphinscheduler-daemon.sh stop master-server

# 停止 Flink
$FLINK_HOME/bin/stop-cluster.sh

# 停止 Hive
pkill -f HiveServer2
pkill -f HiveMetaStore

# 停止 Kafka
$KAFKA_HOME/bin/kafka-server-stop.sh

# 停止 ZooKeeper
zkServer.sh stop

# 停止 Hadoop
$HADOOP_HOME/sbin/stop-yarn.sh
$HADOOP_HOME/sbin/stop-dfs.sh
```

### 3.4 优雅停止 Flink 作业（保留 Savepoint）

```bash
# 查看运行中的作业
flink list

# 停止作业并保留 Savepoint（用于以后恢复）
flink stop --savepointPath hdfs://namenode:8020/flink/savepoints/ <job-id>

# 取消作业并保留 Checkpoint
flink cancel -s <job-id>
```

---

## 四、Kafka Topic 管理

### 4.1 创建 Topic（初次部署）

```bash
# 车辆通行数据 - 8分区（高吞吐）
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --topic traffic_vehicle \
    --partitions 8 \
    --replication-factor 3 \
    --config retention.ms=86400000 \
    --bootstrap-server localhost:9092

# 路况监测数据 - 4分区
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --topic traffic_status \
    --partitions 4 \
    --replication-factor 3 \
    --config retention.ms=86400000 \
    --bootstrap-server localhost:9092

# 设备状态数据 - 4分区
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --topic device_status \
    --partitions 4 \
    --replication-factor 3 \
    --config retention.ms=86400000 \
    --bootstrap-server localhost:9092

# 设备告警数据 - 4分区（保留7天）
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --topic device_alarm \
    --partitions 4 \
    --replication-factor 3 \
    --config retention.ms=604800000 \
    --bootstrap-server localhost:9092
```

### 4.2 Topic 日常运维

```bash
# 查看所有 Topic
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# 查看指定 Topic 详情
$KAFKA_HOME/bin/kafka-topics.sh --describe --topic traffic_vehicle --bootstrap-server localhost:9092

# 查看消费者组 Lag
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group traffic_vehicle_group

# 重置消费者组 Offset（补数据场景）
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group traffic_vehicle_group \
    --topic traffic_vehicle \
    --reset-offsets --to-earliest --execute

# 增加分区数（只能增不能减）
$KAFKA_HOME/bin/kafka-topics.sh --alter \
    --topic traffic_vehicle \
    --partitions 16 \
    --bootstrap-server localhost:9092

# 删除 Topic
$KAFKA_HOME/bin/kafka-topics.sh --delete \
    --topic test_topic \
    --bootstrap-server localhost:9092
```

---

## 五、Flink 作业运维

### 5.1 编译打包

```bash
cd D:\s\新项目\flink
mvn clean package -DskipTests
# 产物: target/traffic-flink-1.0.jar
```

### 5.2 提交作业

```bash
# ---- 实时车流统计 ----
# 数据源: Kafka traffic_vehicle
# 输出: Redis (每5分钟滚动窗口)
# 并行度: 4
flink run -d \
    -c com.traffic.flink.TrafficVehicleCount \
    -p 4 \
    target/traffic-flink-1.0.jar

# ---- 路况拥堵检测 ----
# 数据源: Kafka traffic_status
# 输出: Kafka traffic_alert + 控制台
# 功能: 5分钟窗口聚合 + 流量异常检测(KeyedState)
flink run -d \
    -c com.traffic.flink.TrafficCongestionDetection \
    -p 4 \
    target/traffic-flink-1.0.jar

# ---- CEP 设备异常检测 ----
# 数据源: Kafka device_status
# 输出: Kafka device_alert
# CEP规则: 连续离线(3次)/CPU高负载(3次)/温度过高(2次)
flink run -d \
    -c com.traffic.flink.DeviceStatusCEP \
    -p 4 \
    target/traffic-flink-1.0.jar
```

### 5.3 Checkpoint 管理

```bash
# 查看 Checkpoint 历史
# 打开 Flink Web UI: http://localhost:8081 → 点击作业 → Checkpoints

# 查看 HDFS 上 Checkpoint 文件
hdfs dfs -ls -R /flink/checkpoints/

# Checkpoint 目录大小监控（超过 10GB 需清理旧版本）
hdfs dfs -du -h /flink/checkpoints/

# 手动清理 7 天前的旧 Checkpoint
hdfs dfs -rm -r /flink/checkpoints/$(date -d '7 days ago' +%Y%m%d)*
```

### 5.4 Savepoint 操作

```bash
# 场景1: 作业升级（修改代码后从 Savepoint 恢复）
flink savepoint <job-id> hdfs://namenode:8020/flink/savepoints/
flink cancel <job-id>
flink run -s hdfs://namenode:8020/flink/savepoints/savepoint-xx \
    -c com.traffic.flink.TrafficVehicleCount \
    target/traffic-flink-2.0.jar

# 场景2: 集群迁移
flink stop --savepointPath hdfs://namenode:8020/flink/savepoints/ <job-id>
# 在新集群提交
flink run -s hdfs://namenode:8020/flink/savepoints/savepoint-xx \
    -c com.traffic.flink.TrafficVehicleCount \
    target/traffic-flink-1.0.jar

# 场景3: 查看 Savepoint 内容
flink savepoint --dispose hdfs://namenode:8020/flink/savepoints/savepoint-xx
```

### 5.5 作业监控

```bash
# 查看运行中的作业
flink list

# 查看作业详细信息
flink list -a  # 包含已完成/取消的作业

# 查看作业异常信息
curl http://localhost:8081/jobs/<job-id>/exceptions

# 查看作业指标（Prometheus 格式）
curl http://localhost:8081/jobs/<job-id>/metrics
```

### 5.6 Web UI 监控指标

| 指标 | 正常范围 | 异常阈值 | 说明 |
|------|---------|---------|------|
| numRecordsInPerSecond | > 0 | = 0 持续5分钟 | 上游 Kafka 无数据 |
| numRecordsOutPerSecond | ≈ numRecordsIn | 偏差 > 30% | 数据处理可能存在丢数 |
| currentInputWatermark | 持续递增 | 停滞 > 60秒 | 上游数据延迟 |
| numLateRecordsDropped | < 100/分钟 | > 1000/分钟 | Watermark 配置过小 |
| checkpointSize | < 100MB | > 500MB | 状态膨胀，需优化或扩容 |
| checkpointDuration | < 30秒 | > 60秒 | 状态过大或 HDFS 吞吐不足 |

---

## 六、DolphinScheduler 调度管理

### 6.1 项目初始化

```bash
# 创建项目（通过 API）
curl -X POST http://localhost:12345/dolphinscheduler/projects \
    -H "Content-Type: application/json" \
    -H "token: YOUR_TOKEN" \
    -d '{
        "projectName": "traffic-data-platform",
        "description": "交通数据治理实时分析平台"
    }'

# 上传资源文件（SQL 脚本复制到 DolphinScheduler 资源中心）
# 将所有 sql/ 目录文件上传至 /dolphinscheduler/resources/sql/

# 创建租户
curl -X POST http://localhost:12345/dolphinscheduler/tenants \
    -H "Content-Type: application/json" \
    -H "token: YOUR_TOKEN" \
    -d '{"tenantCode": "traffic_tenant", "queueId": 1}'
```

### 6.2 工作流依赖关系

```
                  ┌─ ods_vehicle_pass_di ───── dwd_vehicle_pass_di ───── dws_road_hour_flow ──┐
                  │                                                                           │
                  ├─ ods_traffic_status_di ──── dwd_traffic_status_di ─── dws_area_jam_hour ──┤
                  │                                                                           │
                  ├─ ods_device_status_di ──── dwd_device_status_di ── dws_device_health_day ─┤
dim_road_zip ─────┤                                                                           ├── ads_*
dim_device_zip ───┤                                                                           │
dim_time ─────────┤                                                                           │
dim_area ─────────┤                                                                           │
                  ├─ ods_alarm_log_di ──────── dwd_alarm_log_di ────── dws_alarm_day ─────────┤
                  │                                                                           │
                  └─ [所有任务成功] ─────────── data_quality_check ──── partition_cleanup ─────┘
```

### 6.3 手动触发实例

```bash
# 补跑指定日期的数据
curl -X POST http://localhost:12345/dolphinscheduler/projects/traffic-data-platform/executors/start-process-instance \
    -H "Content-Type: application/json" \
    -H "token: YOUR_TOKEN" \
    -d '{
        "processDefinitionCode": 1,
        "scheduleTime": "2024-01-15 00:00:00",
        "failureStrategy": "END",
        "warningType": "FAILURE",
        "warningGroupId": 1,
        "processInstancePriority": "HIGHEST",
        "startNodeList": "",
        "taskDependType": "TASK_POST"
    }'
```

### 6.4 查看实例状态

```bash
# 查看实例列表
curl http://localhost:12345/dolphinscheduler/projects/traffic-data-platform/process-instances \
    -H "token: YOUR_TOKEN"

# 重跑失败实例
curl -X POST http://localhost:12345/dolphinscheduler/projects/traffic-data-platform/executors/restart-process-instance \
    -H "Content-Type: application/json" \
    -H "token: YOUR_TOKEN" \
    -d '{"processInstanceId": 123}'
```

---

## 七、日常运维巡检清单

### 7.1 每日巡检（09:00）

| 检查项 | 命令/方式 | 正常标准 |
|--------|----------|---------|
| YARN 节点状态 | `yarn node -list -all` | 全部 RUNNING，无 LOST/UNHEALTHY |
| HDFS 存储使用率 | `hdfs dfsadmin -report` | < 80% |
| Kafka 消费者 Lag | `kafka-consumer-groups.sh --describe` | < 1000 |
| Flink 作业状态 | Web UI: http://localhost:8081 | 全部 RUNNING |
| Flink Checkpoint | Web UI → Checkpoints 页 | 最近一次 Completed |
| DolphinScheduler 昨日任务 | Web UI → 工作流实例 | 全部 SUCCESS |
| Hive 最新分区 | `SHOW PARTITIONS ods_vehicle_pass_di` | 包含昨天的分区 |
| Redis 实时数据 | `redis-cli HGETALL traffic:vehicle` | 有最新值 |
| Superset 看板 | 浏览器打开 http://localhost:8088 | 看板正常加载 |
| 数据质量评分 | `python python/data_quality_monitor.py` | > 95% |

### 7.2 每周巡检（周一）

| 检查项 | 内容 |
|--------|------|
| HDFS 小文件检查 | `hdfs fsck /user/hive/warehouse/ -files -blocks -locations` |
| 分区清理确认 | 确认 `partition_cleanup` 任务正常执行，90天前分区已删除 |
| Flink 状态大小 | 检查 Checkpoint 大小趋势，评估是否需要扩容 |
| Dim 拉链表更新 | 检查 `dim_road_zip` / `dim_device_zip` 的 `is_current='Y'` 记录数 |
| 磁盘空间 | `df -h` 各节点数据盘使用率 < 80% |

### 7.3 每月巡检

| 检查项 | 内容 |
|--------|------|
| 数据质量人工抽检 | 每月抽样 1000 条数据人工标注异常，验证自动识别率 ≥ 98% |
| 告警规则有效性 | 审查上月告警，剔除误报率 > 20% 的规则 |
| 查询性能基线 | 执行标准查询集，对比基线（详见 九、量化成果） |
| 备份验证 | 从备份恢复一个分区数据，验证完整性 |
| 权限审计 | 审查 Hive Ranger 和 Superset 权限，清理离职人员 |

---

## 八、数据回溯与重跑

### 8.1 单表分区重跑

```bash
# ===== 场景：某天 ODS 数据错误，需要从 ODS 到 ADS 全量重跑 =====

# Step 1: 备份原分区（可选）
hive -e "
    CREATE TABLE traffic_db.ods_vehicle_pass_di_backup LIKE traffic_db.ods_vehicle_pass_di;
    INSERT OVERWRITE TABLE traffic_db.ods_vehicle_pass_di_backup PARTITION(dt='2024-01-15')
    SELECT * FROM traffic_db.ods_vehicle_pass_di WHERE dt='2024-01-15';
"

# Step 2: 删除需要重跑的各级分区
hive -e "ALTER TABLE traffic_db.ods_vehicle_pass_di  DROP IF EXISTS PARTITION(dt='2024-01-15')"
hive -e "ALTER TABLE traffic_db.dwd_vehicle_pass_di  DROP IF EXISTS PARTITION(dt='2024-01-15')"
hive -e "ALTER TABLE traffic_db.dws_road_hour_flow   DROP IF EXISTS PARTITION(dt='2024-01-15')"
hive -e "ALTER TABLE traffic_db.ads_traffic_operation DROP IF EXISTS PARTITION(dt='2024-01-15')"
hive -e "ALTER TABLE traffic_db.ads_top_jam_roads     DROP IF EXISTS PARTITION(dt='2024-01-15')"

# Step 3: 按依赖顺序重新执行（这里以 ${date} 变量替换日期）
hive -hiveconf date=2024-01-15 -f sql/ods/ods_vehicle_pass_di.sql
hive -hiveconf date=2024-01-15 -f sql/dwd/dwd_vehicle_pass_di.sql
hive -hiveconf date=2024-01-15 -f sql/dws/dws_road_hour_flow.sql
hive -hiveconf date=2024-01-15 -f sql/ads/ads_traffic_operation.sql
hive -hiveconf date=2024-01-15 -f sql/ads/ads_top_jam_roads.sql

# Step 4: 验证
python python/data_quality_monitor.py --date 2024-01-15

# Step 5: 确认无误后清理备份表
hive -e "DROP TABLE IF EXISTS traffic_db.ods_vehicle_pass_di_backup"
```

### 8.2 批量重跑（连续多天）

```bash
# 批量重跑最近 7 天数据
for d in $(seq 7); do
    date_str=$(date -d "$d days ago" +%Y-%m-%d)
    echo "=== 开始重跑 $date_str ==="
    
    # ODS
    hive -hiveconf date=$date_str -f sql/ods/ods_vehicle_pass_di.sql
    
    # DWD
    hive -hiveconf date=$date_str -f sql/dwd/dwd_vehicle_pass_di.sql
    
    # DWS
    hive -hiveconf date=$date_str -f sql/dws/dws_road_hour_flow.sql
    
    # ADS
    hive -hiveconf date=$date_str -f sql/ads/ads_traffic_operation.sql
    
    echo "=== $date_str 重跑完成 ==="
done

# 全部重跑完成后执行质量校验
python python/data_quality_monitor.py
```

### 8.3 全链路重跑（从 Kafka 重新消费）

```bash
# 场景：Kafka 数据未丢失，但下游计算全部错误，需要从 Kafka 重新消费
# 注意：此操作会丢失当前 Flink 状态，请先手动保存 Savepoint！

# Step 1: 停止 Flink 作业并保留 Savepoint
flink stop --savepointPath hdfs://namenode:8020/flink/savepoints/ $(flink list -r)

# Step 2: 重置 Kafka 消费者偏移量（回到最早位置）
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group traffic_vehicle_group \
    --topic traffic_vehicle \
    --reset-offsets --to-earliest --execute

# Step 3: 清理 Hive 中受影响的分区（同上节）
# Step 4: 重新提交 Flink 作业（从 Kafka 从头消费）
flink run -d -c com.traffic.flink.TrafficVehicleCount target/traffic-flink-1.0.jar

# Step 5: 等待 Flink 处理完积压数据后，手动触发 DolphinScheduler 补跑 ETL
```

---

## 九、故障诊断与排查

### 9.1 故障分级与响应时间

| 等级 | 定义 | 响应SLA | 通知范围 |
|------|------|--------|---------|
| P0-CRITICAL | 全链路不可用，影响交通数据采集 | 5分钟内响应 | CTO + 运维全员 + 业务方 |
| P1-MAJOR | 部分组件故障，部分数据延迟 | 15分钟内响应 | 运维组 |
| P2-MINOR | 单个任务失败，不影响核心链路 | 1小时内响应 | 值班运维 |
| P3-WARNING | 非紧急问题（如文档显示异常） | 24小时内处理 | 责任开发 |

### 9.2 常见故障排查表

| 故障现象 | 可能原因 | 排查步骤 | 解决方案 |
|---------|---------|---------|---------|
| **Flink 作业 FAILED** | Watermark 停滞、状态溢出、OOM | 1.查看 Flink UI → Exceptions 2.查看 TaskManager 日志: `tail -500 /var/log/flink/taskmanager.log` 3.检查 JVM GC 日志 | 1.增大 TaskManager 堆内存 2.调整 Watermark 空闲超时 3.从 Savepoint 恢复 |
| **Flink Checkpoint 失败** | HDFS NameNode 不可达、磁盘满 | 1.`hdfs dfsadmin -report` 2.检查 Checkpoint 目录权限 3.查看 Checkpoint 日志 | 1.修复 HDFS 连通性 2.清理旧 Checkpoint 3.增大 Checkpoint 超时时间 |
| **Kafka 消费 Lag 积压** | 消费者性能不足、下游写入慢 | 1.`kafka-consumer-groups --describe` 2.检查消费者 CPU/网络 | 1.增加消费者并行度 2.Kafka 扩容分区 3.检查下游是否存在慢查询 |
| **Kafka 消费者无数据** | Topic 不存在、生产者未发送 | 1.`kafka-topics --list` 2.`kafka-console-consumer --from-beginning` 3.检查生产者日志 | 1.创建 Topic 2.重启生产者服务 |
| **Hive 任务执行超时** | 数据倾斜、小文件过多、YARN 资源不足 | 1.查看 YARN 应用日志 2.检查 `INSERT` 是否有大量 Reduce skew 3.`hdfs dfs -count` 统计文件数 | 1.启用 Skew Join 2.合并小文件 3.增加 Reducer 数量 |
| **Hive 分区不存在** | 上游任务未执行、分区名拼写错误 | 1.`SHOW PARTITIONS <table>` 2.检查 DolphinScheduler 上游任务状态 | 1.手动创建分区 2.补跑上游任务 |
| **Redis 无数据** | Flink Redis Sink 异常、Redis 内存满 | 1.`redis-cli INFO` 2.检查 Redis 内存使用: `used_memory_human` 3.检查 Flink 作业日志 | 1.重启 Flink 作业 2.Redis 扩容/设置 maxmemory-policy |
| **Superset 看板无数据** | 数据源连不上、SQL 超时 | 1.检查 Superset 数据源连接 2.手动执行看板 SQL 验证 3.检查 ADS 表最新分区 | 1.修复数据源 2.重建看板缓存 |
| **DolphinScheduler 任务失败** | 依赖任务未完成、超时、资源不足 | 1.查看任务日志 2.检查依赖链 3.检查 YARN 队列资源 | 1.手动重跑 2.调整超时配置 3.增加 YARN 资源 |
| **数据质量告警** | 空值率/重复率/合法率不达标 | 1.`python python/data_quality_monitor.py` 2.查看报告文件 `/tmp/data_quality_report.json` 3.对比上游数据源 | 1.检查上游数据质量 2.调整清洗规则 3.触发数据重跑 |

### 9.3 日志位置

| 组件 | 日志路径 | 查看命令 |
|------|---------|---------|
| Flink JobManager | `$FLINK_HOME/log/flink-*-jobmanager-*.log` | `tail -500` |
| Flink TaskManager | `$FLINK_HOME/log/flink-*-taskmanager-*.log` | `tail -500` |
| Kafka Server | `$KAFKA_HOME/logs/server.log` | `tail -500` |
| Kafka Controller | `$KAFKA_HOME/logs/controller.log` | `tail -200` |
| Hive Metastore | `/var/log/hive/hive-metastore.log` | `tail -500` |
| HiveServer2 | `/var/log/hive/hive-server2.log` | `tail -500` |
| DolphinScheduler | `$DOLPHINSCHEDULER_HOME/logs/` | 通过 Web UI 查看 |
| YARN ResourceManager | `$HADOOP_HOME/logs/yarn-*-resourcemanager-*.log` | `tail -500` |
| Python 脚本 | `/tmp/data_quality_monitor.log` | `cat` |
| Superset | `/var/log/superset/superset.log` | `tail -500` |

### 9.4 应急联系与告警升级路径

```
P3/WARNING: 值班运维（根据排班表）
    ↓ 30分钟内未响应
P2/MINOR: 值班运维 + 运维组长
    ↓ 15分钟内未响应
P1/MAJOR: 运维组长 + 运维经理 + 数据开发负责人
    ↓ 5分钟内未响应
P0/CRITICAL: 运维经理 + CTO + 业务方负责人（交通管控部门）
```

---

## 十、容灾与降级预案

### 10.1 场景：Kafka 集群故障

```bash
# 影响: Flink 无法消费实时数据
# 降级: Flink 切换为读取 HDFS 离线数据

# Step 1: 停止当前 Flink 作业
flink cancel <job-id>

# Step 2: 以离线模式提交（需要提前在代码中实现 fallback 逻辑）
flink run -d \
    -c com.traffic.flink.TrafficVehicleCount \
    --kafka.down.mode true \
    --hdfs.input.path /user/hive/warehouse/traffic_db.db/ods_vehicle_pass_di/ \
    target/traffic-flink-1.0.jar

# Step 3: 恢复方式（Kafka 恢复后）
# 先停止离线模式作业，再用正常模式提交（从 Checkpoint 恢复）
```

### 10.2 场景：Redis 不可用

```bash
# 影响: Superset 实时看板无数据
# 降级: Superset 切换为读取 Hive ADS 层（延迟从秒级降为天级）

# Step 1: 在 Superset 中将实时看板数据源从 Redis 切换为 Hive
# Superset → Data → Databases → 修改看板数据源

# Step 2: 恢复后回灌数据
# Redis 恢复后，重启 Flink 作业，从 Savepoint 恢复
flink run -s hdfs://namenode:8020/flink/savepoints/savepoint-xxx \
    -c com.traffic.flink.TrafficVehicleCount \
    target/traffic-flink-1.0.jar
```

### 10.3 场景：HDFS 磁盘空间不足

```bash
# 紧急处理流程

# Step 1: 立即检查磁盘使用
df -h /data
hdfs dfsadmin -report | grep "DFS Used%"

# Step 2: 扩容（如果能操作）
# 增加 DataNode 节点或扩展现有磁盘

# Step 3: 紧急清理（如果不能扩容）
# 删除过期分区（从最老的分区开始）
hive -e "ALTER TABLE traffic_db.ods_vehicle_pass_di DROP IF EXISTS PARTITION(dt < '$(date -d '7 days ago' +%Y-%m-%d)')"

# Step 4: 删除旧 Checkpoint
hdfs dfs -rm -r /flink/checkpoints/$(date -d '30 days ago' +%Y%m%d)*

# Step 5: 临时降低 Kafka 保留时间
$KAFKA_HOME/bin/kafka-configs.sh --alter \
    --entity-type topics \
    --entity-name traffic_vehicle \
    --add-config retention.ms=43200000 \
    --bootstrap-server localhost:9092
```

---

## 十一、备份与恢复

### 11.1 备份策略

| 备份对象 | 频率 | 保留周期 | 备份位置 |
|---------|------|---------|---------|
| Hive 建表 DDL | 每日 | 永久 | Git + 文件服务器 |
| ODS 原始数据 | 每日 | 90天 | HDFS 分区自动保留 |
| DWD/DWS/ADS | 每日 | 365天 | HDFS 分区自动保留 |
| Flink Checkpoint | 持续 | 7天 | HDFS `/flink/checkpoints/` |
| Flink Savepoint | 手动触发 | 30天 | HDFS `/flink/savepoints/` |
| 配置文件 | 每次变更 | 永久 | Git + `config_backup/` 目录 |

### 11.2 Hive 表结构备份

```bash
# 导出所有建表语句
hive -e "SHOW TABLES IN traffic_db" | while read table; do
    hive -e "SHOW CREATE TABLE traffic_db.$table" > "config_backup/${table}_ddl_$(date +%Y%m%d).sql"
done

# 或使用 mysqldump 备份 Hive Metastore 元数据库
mysqldump -h metastore_host -u hive -p hive_metastore > metastore_backup_$(date +%Y%m%d).sql
```

### 11.3 配置文件备份

```bash
# 备份整个 config 目录
tar -czf config_backup_$(date +%Y%m%d).tar.gz config/

# 建议将配置文件纳入 Git 管理
cd D:\s\新项目
git add config/
git commit -m "[backup] 导出生产配置 $(date +%Y-%m-%d)"
```

### 11.4 灾难恢复流程

```bash
# ===== 全量灾难恢复（Hive 数据全部丢失） =====

# Step 1: 恢复 Hive 表结构
for f in config_backup/*_ddl_*.sql; do
    hive -f "$f"
done

# Step 2: 从 Kafka 重新消费数据（前提：Kafka 数据未过期）
# 重置所有消费者组到 earliest
for group in traffic_vehicle_group traffic_status_group device_status_group device_alarm_group; do
    $KAFKA_HOME/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 \
        --group $group \
        --reset-offsets --to-earliest --execute --all-topics
done

# Step 3: 重启 Flink 作业（从 Kafka 从头消费）
# Step 4: 等待数据落地后，触发 DolphinScheduler 全量补跑 ETL
```

---

## 十二、性能监控与调优

### 12.1 Hive 查询性能调优

```sql
-- 查询优化参数（在 DolphinScheduler 任务中配置）

-- 1. 启用 CBO 优化器
SET hive.cbo.enable = true;

-- 2. MapJoin 自动转换（小表自动放入内存）
SET hive.auto.convert.join = true;
SET hive.auto.convert.join.noconditionaltask.size = 256000000;

-- 3. 数据倾斜治理
SET hive.optimize.skewjoin = true;
SET hive.skewjoin.key = 100000;

-- 4. 动态分区
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- 5. 并行执行
SET hive.exec.parallel = true;
SET hive.exec.parallel.thread.number = 8;

-- 6. 小文件合并
SET hive.merge.mapfiles = true;
SET hive.merge.mapredfiles = true;
SET hive.merge.size.per.task = 256000000;
SET hive.merge.smallfiles.avgsize = 128000000;

-- 7. 向量化查询
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;
```

### 12.2 Kafka 性能优化

```bash
# Producer 端优化参数
# compression.type=snappy       # 压缩传输
# batch.size=16384              # 批量发送
# linger.ms=5                   # 等待5ms凑批次

# Consumer 端优化参数
# fetch.min.bytes=1048576       # 最少拉取1MB
# fetch.max.wait.ms=500         # 最多等500ms
# max.partition.fetch.bytes=10485760  # 单分区最多拉取10MB
```

### 12.3 Flink 性能优化

```bash
# 作业提交时指定 JVM 参数
flink run -d \
    -Dtaskmanager.memory.process.size=4096m \
    -Dtaskmanager.numberOfTaskSlots=4 \
    -Dparallelism.default=4 \
    -c com.traffic.flink.TrafficVehicleCount \
    target/traffic-flink-1.0.jar

# 或者修改 flink-conf.yaml:
# taskmanager.memory.process.size: 4096m
# taskmanager.numberOfTaskSlots: 4
# parallelism.default: 4
# state.backend: hashmap
# execution.checkpointing.interval: 300s
```

---

## 十三、告警管理

### 13.1 告警渠道配置

修改 [alert_config.json](file:///D:/s/新项目/config/alert_config.json):

```json
{
  "dingtalk": {
    "webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=YOUR_REAL_TOKEN",
    "secret": "YOUR_REAL_SECRET"
  },
  "email": {
    "smtp_host": "your-smtp.company.com",
    "smtp_port": 587,
    "sender": "data-platform@your-company.com",
    "password": "YOUR_EMAIL_PASSWORD"
  }
}
```

### 13.2 告警抑制规则

| 规则 | 配置项 | 说明 |
|------|--------|------|
| 重复抑制 | `duplicate_suppression_minutes: 30` | 相同告警30分钟内不重复推送 |
| 维护窗口 | `maintenance_window: 凌晨2-3点` | 运维窗口期非CRITICAL告警静默 |
| 每日汇总 | `daily_summary.send_time: 09:00` | 每天早上9点发送质量日报 |

### 13.3 手动测试告警

```bash
python -c "
from data_quality_monitor import AlertNotifier
notifier = AlertNotifier('config/alert_config.json')
notifier.notify('测试告警', '这是一条测试告警，请忽略。', severity='MINOR')
"
```

---

## 十四、数据权限管理

### 14.1 Hive 表权限（Ranger）

```bash
# 权限模型配置见 config/data_permission.json

# 数据开发工程师: ODS 只读 + DWD/DWS/ADS 读写
# 运维工程师: 仅设备相关表只读
# 交通运营分析师: ADS + 道路维度只读
# 管理层决策: ADS 全部只读
```

### 14.2 Superset 看板权限

| 看板 | 可见角色 | 数据源 | 刷新间隔 |
|------|---------|--------|---------|
| 交通运营实时看板 | 运营分析师+开发+决策 | Redis → Hive ADS | 60秒 |
| 设备运维监控看板 | 运维工程师+开发+决策 | Hive ADS | 5分钟 |
| 交通治理决策看板 | 决策层+开发 | Hive ADS | 5分钟 |
| 数据质量监控看板 | 数据开发 | Python报告 → CSV | 10分钟 |

---

## 十五、Python 运维脚本

### 15.1 数据质量监控

```bash
# 运行完整质量检查
python python/data_quality_monitor.py

# 指定日期检查
python python/data_quality_monitor.py --date 2024-01-15

# 查看报告
cat /tmp/data_quality_report.json
```

### 15.2 数据血缘导出

```bash
# 导出血缘关系 JSON
python python/data_lineage.py

# 生成可视化图
python -c "
from data_lineage import DataLineageManager
mgr = DataLineageManager()
print(mgr.visualize_lineage())
" > /tmp/lineage.dot
dot -Tpng /tmp/lineage.dot -o /tmp/lineage.png
```

### 15.3 AI ETL 生成器

```bash
# 生成自定义 ODS 建表语句
python -c "
from ai_etl_generator import ETLScriptGenerator
gen = ETLScriptGenerator()
ddl = gen.generate_ods_ddl('ods_custom_table', 'vehicle', u'自定义车辆数据表')
print(ddl)
"

# NL2SQL 查询转换
python -c "
from ai_etl_generator import NL2SQLConverter
conv = NL2SQLConverter()
sql = conv.convert(u'今天最拥堵的5条道路', date='2024-01-15', limit=5)
print(sql)
"
```

---

## 十六、附录

### 16.1 端口速查

| 组件 | 端口 | 协议 |
|------|------|------|
| ZooKeeper | 2181 | TCP |
| Kafka | 9092 | TCP |
| HDFS NameNode RPC | 8020 | TCP |
| HDFS NameNode Web UI | 9870 | HTTP |
| YARN ResourceManager Web UI | 8088 | HTTP |
| HiveServer2 | 10000 | TCP |
| Hive Metastore | 9083 | TCP |
| Flink JobManager Web UI | 8081 | HTTP |
| Redis | 6379 | TCP |
| DolphinScheduler Web UI | 12345 | HTTP |
| Superset Web UI | 8088 | HTTP |

### 16.2 关键目录

| 路径 | 说明 |
|------|------|
| `/user/hive/warehouse/traffic_db.db/` | Hive 数仓数据 |
| `/flink/checkpoints/` | Flink Checkpoint |
| `/flink/savepoints/` | Flink Savepoint |
| `/tmp/data_quality_report.json` | 数据质量报告 |
| `$KAFKA_HOME/logs/` | Kafka 日志 |
| `$FLINK_HOME/log/` | Flink 日志 |
| `$DOLPHINSCHEDULER_HOME/logs/` | DolphinScheduler 日志 |

### 16.3 运维值班 Checklist（每日 09:00）


- [ ] YARN 节点全部 RUNNING（`yarn node -list`）
- [ ] HDFS 使用率 < 80%（`hdfs dfsadmin -report`）
- [ ] Kafka 消费者 Lag < 1000（`kafka-consumer-groups --describe`）
- [ ] Flink 作业全部 RUNNING（http://localhost:8081）
- [ ] Flink Checkpoint 最近一次 Completed（Web UI）
- [ ] DolphinScheduler 昨日全部 SUCCESS（Web UI）
- [ ] Hive 最新分区包含昨天日期
- [ ] Redis 有最新数据（`redis-cli HGETALL traffic:vehicle`）
- [ ] Superset 看板正常加载（http://localhost:8088）
- [ ] 数据质量评分 > 95%（`python python/data_quality_monitor.py`）
- [ ] 无 P0/P1 未处理告警
- [ ] 检查邮件/钉钉是否有新告警
- [ ] 日志无 ERROR 级别异常（抽查关键组件最近1h日志）

### 16.4 运维值班记录模板

```
值班日期: YYYY-MM-DD
值班人员: [姓名]

[每日巡检]
- YARN 节点: [正常/异常]
- HDFS 使用率: [XX%]
- Kafka Lag: [正常/异常]
- Flink 作业: [全部 RUNNING / 有失败]
- 数据质量评分: [XX%]
- 告警统计: [P0: 0, P1: 0, P2: 0, P3: 0]

[异常处理]
- 故障描述: [简要描述]
- 处理措施: [做了什么]
- 恢复时间: [HH:MM]
- 是否需跟进: [是/否]

[遗留问题]

[交接备注]
```
