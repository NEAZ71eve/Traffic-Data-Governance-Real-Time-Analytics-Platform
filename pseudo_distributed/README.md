# 伪分布式部署方案

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                    Windows 单机                          │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐  │
│  │  Flink   │  │  Python  │  │  WSL Ubuntu           │  │
│  │JobManager│  │ 脚本/Kafka│  │  ┌────────┐┌───────┐ │  │
│  │+TaskMgr  │  │ Consumer │  │  │ Kafka  ││ Redis │ │  │
│  │ localhost│  │ producer │──▶  │ Broker ││Server │ │  │
│  │ :8081    │  │          │  │  │ :9092  ││:6379  │ │  │
│  └──────────┘  └──────────┘  │  └────────┘└───────┘ │  │
│                               └──────────────────────┘  │
│  ┌──────────────────────────────────────────────────┐   │
│  │              SQLite (Hive 替代)                    │   │
│  │         traffic_data.db (HDFS 替代)                │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │          Flask 仪表盘 (Superset 替代)              │   │
│  │              http://127.0.0.1:8088                │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## 组件对照

| 生产组件 | 伪分布式方案 | 验证方式 |
|---------|------------|---------|
| Kafka | WSL Kafka 3.7.0 (KRaft单节点) | produce→consume 消息 |
| Flink | Windows Flink 1.18 Standalone | submit Job → 查看WebUI |
| Redis | WSL Redis 7.0 | Python redis-py 读写 |
| HDFS | 本地文件系统 data/ 目录 | Python 文件读写模拟 |
| Hive | SQLite traffic_data.db | 执行 20 个 SQL 脚本 |
| DolphinScheduler | task_scheduler.py | Python APScheduler 模拟 |
| Superset | Flask 仪表盘 | http://127.0.0.1:8088 |

## 快速开始

```bash
# 1. 一键安装所有组件
python setup_all.py

# 2. 启动所有服务
python start_all.py

# 3. 测试每个组件
python test_kafka.py
python test_flink.py
python test_redis.py
python test_hive_sql.py
python test_hdfs.py

# 4. 端到端全链路测试
python test_pipeline.py

# 5. 停止所有服务
python stop_all.py
```
