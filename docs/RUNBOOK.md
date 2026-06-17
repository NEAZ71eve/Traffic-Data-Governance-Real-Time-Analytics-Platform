# 运维操作手册

> v3.0 | 2026-06-10

## 架构速查

| 组件 | 端口 | 运行方式 |
|------|------|---------|
| HDFS NameNode | 9870/9000 | Docker |
| HiveServer2 | 10000 | Docker |
| Kafka | 9092 | Docker KRaft |
| Flink JobManager | 8081 | Docker |
| Redis | 6379 | Docker |
| DolphinScheduler | 12345 | Docker |
| Superset | 8088 | Docker |
| Prometheus | 9090 | Docker |
| Grafana | 3000 | Docker |
| AlertManager | 9093 | Docker |

## 部署

```bash
# Docker 部署（推荐）
docker compose -f docker-compose-production.yml up -d

# 分步启动顺序: ZooKeeper → Hadoop → Kafka → Hive → Flink → DolphinScheduler → Redis → Superset

# 一键部署
bash bin/deploy-all.sh deploy

# Windows PowerShell
.\bin\deploy-all.ps1 deploy
```

## 建表

```bash
# 按依赖顺序执行
hive -f sql/ods/*.sql    # ODS 层 (7张)
hive -f sql/dim/*.sql    # DIM 层 (4张)
hive -f sql/dwd/*.sql   # DWD 层 (4张)
hive -f sql/dws/*.sql   # DWS 层 (4张)
hive -f sql/ads/*.sql   # ADS 层 (5张)
```

## Kafka 管理

```bash
# 创建 Topic
kafka-topics.sh --create --topic traffic_vehicle --partitions 8 --replication-factor 3 --bootstrap-server localhost:9092

# 查看 Lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group traffic_vehicle_group

# 重置 Offset（补数据）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group traffic_vehicle_group --topic traffic_vehicle --reset-offsets --to-earliest --execute
```

## Flink 作业

```bash
# 编译
cd flink && mvn clean package -DskipTests

# 提交
flink run -d -c com.traffic.flink.TrafficVehicleCount -p 4 target/traffic-flink-1.0.jar

# Savepoint
flink stop --savepointPath hdfs://namenode:8020/flink/savepoints/ <job-id>

# 从 Savepoint 恢复
flink run -s hdfs://namenode:8020/flink/savepoints/savepoint-xx -c com.traffic.flink.TrafficVehicleCount target/traffic-flink-2.0.jar
```

## 日常巡检

### 每日 (09:00)

- [ ] YARN 节点全部 RUNNING
- [ ] HDFS 使用率 < 80%
- [ ] Kafka Lag < 1000
- [ ] Flink 作业全部 RUNNING
- [ ] Checkpoint 最近一次 Completed
- [ ] DolphinScheduler 昨日全部 SUCCESS
- [ ] Hive 最新分区包含昨天
- [ ] Redis 有最新数据
- [ ] 数据质量评分 > 95%

### 每周

- [ ] HDFS 小文件检查 (`hdfs fsck`)
- [ ] 分区清理确认 (90天前分区已删除)
- [ ] Flink 状态大小趋势
- [ ] SCD2 拉链表 is_current 记录数
- [ ] 磁盘使用率

### 每月

- [ ] 数据质量人工抽检 1000 条
- [ ] 告警规则有效性审查
- [ ] 查询性能基线对比
- [ ] 备份恢复验证
- [ ] 权限审计

## 故障排查

| 现象 | 排查 | 解决 |
|------|------|------|
| Flink 作业 FAILED | UI→Exceptions, TaskManager 日志 | 增大堆内存, 调整 Watermark, 从 Savepoint 恢复 |
| Checkpoint 失败 | hdfs dfsadmin, 目录权限, 日志 | 修复 HDFS, 清理旧 Checkpoint, 增大超时 |
| Kafka Lag 积压 | kafka-consumer-groups --describe | 增加并行度, 扩容分区, 检查下游慢查询 |
| Hive 任务超时 | YARN 日志, 数据分布, 文件数 | 启用 Skew Join, 合并小文件, 增加 Reducer |
| Redis 无数据 | redis-cli INFO, 内存使用, Flink 日志 | 重启 Flink 作业, 扩容/设置 maxmemory-policy |
| Superset 无数据 | 数据源连接, SQL 手动执行, ADS 分区 | 修复数据源, 重建缓存 |
| HDFS 磁盘满 | df -h, dfsadmin report | 删除过期分区, 旧 Checkpoint, 降低 Kafka 保留时间 |

### 日志位置

| 组件 | 路径 |
|------|------|
| Flink | $FLINK_HOME/log/flink-*.log |
| Kafka | $KAFKA_HOME/logs/server.log |
| Hive | /var/log/hive/hive-*.log |
| Python | /tmp/data_quality_monitor.log |

## 数据回溯

```bash
# 单表分区重跑
hive -e "ALTER TABLE traffic_db.ods_vehicle_pass_di DROP IF EXISTS PARTITION(dt='2024-01-15')"
hive -hiveconf date=2024-01-15 -f sql/ods/ods_vehicle_pass_di.sql
hive -hiveconf date=2024-01-15 -f sql/dwd/dwd_vehicle_pass_di.sql
# ... 按依赖链逐层重跑

# 批量重跑最近7天
for d in $(seq 7); do
  date_str=$(date -d "$d days ago" +%Y-%m-%d)
  hive -hiveconf date=$date_str -f sql/ods/ods_vehicle_pass_di.sql
  # ... 逐层执行
done
```

## Hive 性能调优

```sql
SET hive.cbo.enable = true;
SET hive.auto.convert.join = true;
SET hive.auto.convert.join.noconditionaltask.size = 256000000;
SET hive.optimize.skewjoin = true;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.parallel = true;
SET hive.merge.mapfiles = true;
SET hive.merge.size.per.task = 256000000;
SET hive.vectorized.execution.enabled = true;
```

## 监控与告警

```bash
# 启动监控栈
docker compose -f docker-compose-monitoring.yml up -d

# 启动 ELK 日志栈
docker compose -f docker-compose-elk.yml up -d

# 启动 Webhook 模拟器
python python/alert_webhook_server.py

# 发送测试告警
python python/alert_dispatcher.py --test
```

### Prometheus 告警规则

| 规则 | 条件 | 级别 |
|------|------|------|
| 服务不可用 | up==0 持续2min | CRITICAL |
| Flink JM 宕机 | JM 不可达1min | CRITICAL |
| Kafka Lag 高 | >10000 | MAJOR |
| Redis 不可用 | down 2min | MAJOR |
| NameNode 宕机 | NN down 2min | CRITICAL |
| Checkpoint 超时 | >5min | MAJOR |
| CPU 高 | >90% 10min | WARNING |

## Python 运维脚本

```bash
# 数据质量监控
python python/data_quality_monitor.py
python python/data_quality_monitor.py --date 2024-01-15

# 数据血缘导出
python python/data_lineage.py

# AI ETL 生成
python -c "from ai_etl_generator import ETLScriptGenerator; gen = ETLScriptGenerator(); print(gen.generate_ods_ddl('ods_custom', 'vehicle', '自定义表'))"
```
