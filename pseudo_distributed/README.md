# 伪分布式部署

单机运行方案 — WSL Kafka + Redis, Windows Flink, SQLite 替代 Hive, 本地文件替代 HDFS。

## 架构

```
Windows: Flink (8081) / Python 脚本 / Flask 仪表盘 (8088)
WSL: Kafka (9092) / Redis (6379)
SQLite: traffic_data.db (Hive 替代)
本地文件: data/hdfs/ (HDFS 替代)
```

## 组件对照

| 生产组件 | 伪分布式方案 |
|---------|------------|
| Kafka | WSL Kafka 3.7 (KRaft) |
| Flink | Windows Flink 1.18 Standalone |
| Redis | WSL Redis 7.0 |
| HDFS | 本地 data/hdfs/ 分区目录 |
| Hive | SQLite traffic_data.db |
| DolphinScheduler | Python APScheduler |
| Superset | Flask 仪表盘 |

## 使用

```bash
# 安装
python setup_all.py

# 启动
python start_all.py

# 测试组件
python test_kafka.py   # Kafka 生产30条→消费验证
python test_flink.py   # Flink 集群状态+WebUI
python test_redis.py   # Redis HSET/Pipeline/PubSub
python test_hive_sql.py # SQLite 执行20+ SQL
python test_hdfs.py    # 本地FS模拟5层分区

# 全链路验证 (7/7 PASS)
python test_pipeline.py

# 停止
python stop_all.py
```
