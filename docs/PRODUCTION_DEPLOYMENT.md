# 生产级集群部署

> 目标: 伪分布式/模拟 → 生产级真实集群部署

## 集群架构

```
Kafka 3节点 KRaft (9092/9094/9096)
Flink HA: 双 JM + 3 TM (8081/8082), ZooKeeper 协调
Redis Cluster: 3主3从 (6379-6384)
HDFS: NameNode + 3 DataNode (9870)
Hive: HiveServer2 + Metastore + PostgreSQL (10000)
MySQL 8.0: 业务库 (3306)
```

## 环境要求

| 资源 | 最低 | 推荐 |
|------|------|------|
| CPU | 4核 | 8核 |
| 内存 | 8GB | 16GB |
| 磁盘 | 50GB SSD | 100GB SSD |

软件: Docker 20.10+, Docker Compose 2.20+

## 部署

```bash
# 一键部署
bash bin/deploy-production.sh deploy

# 分步部署
docker compose -f docker-compose-production.yml pull
docker compose -f docker-compose-production.yml up -d kafka-1 kafka-2 kafka-3 zookeeper redis-node-1~6 hdfs-namenode hdfs-datanode-1~3 hive-metastore-db mysql
sleep 30
docker compose -f docker-compose-production.yml up -d kafka-init redis-cluster-init
docker compose -f docker-compose-production.yml up -d flink-jobmanager-1 flink-jobmanager-2 flink-taskmanager-1~3
docker compose -f docker-compose-production.yml up -d hive-metastore hiveserver2 app
```

## 服务访问

| 服务 | 地址 |
|------|------|
| Flink Web UI | http://localhost:8081 |
| Flink HA UI | http://localhost:8082 |
| HDFS NameNode | http://localhost:9870 |
| HiveServer2 | jdbc:hive2://localhost:10000 |
| MySQL | localhost:3306 (traffic/traffic123) |
| Redis Cluster | localhost:6379-6384 |
| Kafka | localhost:9092,9094,9096 |
| 业务应用 | http://localhost:8088 |

## 集群管理

```bash
# 查看状态
docker compose -f docker-compose-production.yml ps

# 查看日志
docker compose -f docker-compose-production.yml logs -f kafka-1

# 重启服务
docker compose -f docker-compose-production.yml restart redis-node-1

# 停止集群
docker compose -f docker-compose-production.yml down
```

## 高可用

| 组件 | 配置 | 容错 |
|------|------|------|
| Kafka | 3 Controller, 3副本, min.insync.replicas=2 | 容忍1节点 |
| Flink | 双 JM + ZooKeeper + RocksDB | 主备切换 |
| Redis | 3主3从, cluster-node-timeout=5000 | 自动故障转移 |
| HDFS | 3副本, 3 DataNode | 容忍2节点 |

## 从伪分布式迁移

| 配置项 | 伪分布式 | 生产集群 |
|--------|---------|---------|
| Kafka | localhost:9092 | kafka-1:9092,kafka-2:9092,kafka-3:9092 |
| Flink | localhost:8081 | flink-jobmanager-1:8081 |
| Redis | localhost:6379 | redis-node-1:6379 (Cluster) |
| HDFS | 本地文件系统 | hdfs://hdfs-namenode:9000 |
| Hive | SQLite | jdbc:hive2://hiveserver2:10000 |

## 安全建议

1. 启用 Kafka SASL/SSL、Redis ACL、Hive Kerberos
2. 使用独立 Docker 网络，限制端口暴露
3. 定期备份 Kafka 数据卷、HDFS 元数据、Hive Metastore

## 扩展

```bash
# 水平扩展 TaskManager
docker compose -f docker-compose-production.yml up -d --scale flink-taskmanager=5

# 迁移到 Kubernetes (推荐生产)
# Kafka: Strimzi Operator / Flink: Flink K8s Operator / Redis: Redis Operator
```
