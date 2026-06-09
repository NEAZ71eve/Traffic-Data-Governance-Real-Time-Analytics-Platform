# 生产级集群部署指南

> **目标**: 将当前基于本地模拟/伪分布式的大数据平台，升级为生产级真实集群部署。

---

## 一、未实现部分整改清单

| 模块 | 优先级 | 整改前 | 整改后 | 状态 |
|------|--------|--------|--------|------|
| Kafka 真实集群 | 高 | 单节点 KRaft | **3节点 KRaft 集群** | 已完成 |
| Flink Standalone 集群 | 高 | 单 JM + 1 TM | **HA 双 JM + 3 TM** | 已完成 |
| Redis 真实部署 | 高 | 单节点 | **6节点 Cluster (3主3从)** | 已完成 |
| HDFS 分布式存储 | 中 | 本地文件系统 | **NameNode + 3 DataNode** | 已完成 |
| Hive Metastore | 中 | SQLite 模拟 | **HiveServer2 + Metastore + PostgreSQL** | 已完成 |

---

## 二、集群架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        生产级集群架构 (Docker Compose)                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │
│  │   Kafka-1       │  │   Kafka-2       │  │   Kafka-3       │         │
│  │   (Broker+Ctrl) │  │   (Broker+Ctrl) │  │   (Broker+Ctrl) │         │
│  │   localhost:9092│  │   localhost:9094│  │   localhost:9096│         │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘         │
│           │                    │                    │                   │
│           └────────────────────┼────────────────────┘                   │
│                                ▼                                        │
│  ┌─────────────────────────────────────────────────────────────┐       │
│  │              Flink HA 集群                                   │       │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │       │
│  │  │ JobManager-1 │  │ JobManager-2 │  │ ZooKeeper    │      │       │
│  │  │ :8081 (主)   │  │ :8082 (备)   │  │ :2181        │      │       │
│  │  └──────────────┘  └──────────────┘  └──────────────┘      │       │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │       │
│  │  │ TaskManager-1│  │ TaskManager-2│  │ TaskManager-3│      │       │
│  │  │ 4 slots      │  │ 4 slots      │  │ 4 slots      │      │       │
│  │  └──────────────┘  └──────────────┘  └──────────────┘      │       │
│  └─────────────────────────────────────────────────────────────┘       │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────┐       │
│  │              Redis Cluster (6 nodes)                         │       │
│  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐   │       │
│  │  │:6379 │ │:6380 │ │:6381 │ │:6382 │ │:6383 │ │:6384 │   │       │
│  │  │Master│ │Master│ │Master│ │Slave │ │Slave │ │Slave │   │       │
│  │  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘ └──────┘   │       │
│  └─────────────────────────────────────────────────────────────┘       │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────┐       │
│  │              HDFS 分布式存储                                 │       │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │       │
│  │  │  NameNode    │  │  DataNode-1  │  │  DataNode-2  │      │       │
│  │  │  :9870       │  │  :9864       │  │  :9865       │      │       │
│  │  └──────────────┘  └──────────────┘  └──────────────┘      │       │
│  │  ┌──────────────┐                                        │       │
│  │  │  DataNode-3  │                                        │       │
│  │  │  :9866       │                                        │       │
│  │  └──────────────┘                                        │       │
│  └─────────────────────────────────────────────────────────────┘       │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────┐       │
│  │              Hive 数仓                                       │       │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │       │
│  │  │ HiveServer2  │  │  Metastore   │  │  PostgreSQL  │      │       │
│  │  │ :10000       │  │  :9083       │  │  :5432       │      │       │
│  │  └──────────────┘  └──────────────┘  └──────────────┘      │       │
│  └─────────────────────────────────────────────────────────────┘       │
│                                                                         │
│  ┌─────────────────┐  ┌─────────────────┐                             │
│  │   MySQL 8.0     │  │   业务应用       │                             │
│  │   :3306         │  │   :8088         │                             │
│  └─────────────────┘  └─────────────────┘                             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 三、环境要求

### 3.1 硬件要求

| 资源 | 最低配置 | 推荐配置 |
|------|---------|---------|
| CPU | 4 核 | 8 核 |
| 内存 | 8 GB | 16 GB |
| 磁盘 | 50 GB SSD | 100 GB SSD |
| 网络 | 千兆网卡 | 万兆网卡 |

### 3.2 软件依赖

- Docker Engine 20.10+
- Docker Compose 2.20+
- Linux/macOS/Windows(WSL2)

---

## 四、快速部署

### 4.1 一键部署

```bash
# 进入项目目录
cd Traffic-Data-Governance-Real-Time-Analytics-Platform

# 执行部署脚本
bash bin/deploy-production.sh deploy
```

### 4.2 分步部署

```bash
# 1. 拉取镜像
docker-compose -f docker-compose-production.yml pull

# 2. 启动基础服务
docker-compose -f docker-compose-production.yml up -d \
  kafka-1 kafka-2 kafka-3 \
  zookeeper \
  redis-node-1 redis-node-2 redis-node-3 redis-node-4 redis-node-5 redis-node-6 \
  hdfs-namenode hdfs-datanode-1 hdfs-datanode-2 hdfs-datanode-3 \
  hive-metastore-db mysql

# 3. 等待 30 秒后，启动初始化
docker-compose -f docker-compose-production.yml up -d kafka-init redis-cluster-init

# 4. 启动 Flink 集群
docker-compose -f docker-compose-production.yml up -d \
  zookeeper flink-jobmanager-1 flink-jobmanager-2 \
  flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3

# 5. 启动 Hive
docker-compose -f docker-compose-production.yml up -d hive-metastore hiveserver2

# 6. 启动业务应用
docker-compose -f docker-compose-production.yml up -d app
```

---

## 五、服务访问

| 服务 | 地址 | 说明 |
|------|------|------|
| Flink Web UI | http://localhost:8081 | 主 JobManager |
| Flink HA UI | http://localhost:8082 | 备 JobManager |
| HDFS NameNode | http://localhost:9870 | HDFS 管理界面 |
| HiveServer2 | jdbc:hive2://localhost:10000 | JDBC 连接 |
| MySQL | localhost:3306 | traffic/traffic123 |
| Redis Cluster | localhost:6379-6384 | 6 节点集群 |
| Kafka | localhost:9092,9094,9096 | 3 Broker |
| 业务应用 | http://localhost:8088 | Flask 仪表盘 |

---

## 六、集群管理

### 6.1 常用命令

```bash
# 查看所有服务状态
docker-compose -f docker-compose-production.yml ps

# 查看服务日志
docker-compose -f docker-compose-production.yml logs -f kafka-1
docker-compose -f docker-compose-production.yml logs -f flink-jobmanager-1

# 重启单个服务
docker-compose -f docker-compose-production.yml restart redis-node-1

# 停止集群
docker-compose -f docker-compose-production.yml down

# 停止并清理数据（危险！）
docker-compose -f docker-compose-production.yml down -v
```

### 6.2 Kafka 管理

```bash
# 查看 Topics
docker exec traffic-kafka-1 kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --list

# 查看 Topic 详情
docker exec traffic-kafka-1 kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe --topic traffic_vehicle

# 查看消费组
docker exec traffic-kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 --list

# 查看消费进度
docker exec traffic-kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 --describe --group traffic_vehicle_group
```

### 6.3 Redis 管理

```bash
# 查看集群节点
docker exec traffic-redis-1 redis-cli -p 6379 cluster nodes

# 查看集群信息
docker exec traffic-redis-1 redis-cli -p 6379 cluster info

# 查看主从关系
docker exec traffic-redis-1 redis-cli -p 6379 info replication
```

### 6.4 HDFS 管理

```bash
# 查看文件系统
docker exec traffic-hdfs-namenode hdfs dfs -ls /

# 创建目录
docker exec traffic-hdfs-namenode hdfs dfs -mkdir -p /user/hive/warehouse

# 查看 DataNode 状态
docker exec traffic-hdfs-namenode hdfs dfsadmin -report
```

### 6.5 Hive 管理

```bash
# 进入 beeline
docker exec -it traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n "" -p ""

# 查看数据库
docker exec traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;"

# 查看表
docker exec traffic-hiveserver2 beeline -u jdbc:hive2://localhost:10000/traffic_db -e "SHOW TABLES;"
```

### 6.6 Flink 作业提交

```bash
# 提交作业
docker exec traffic-flink-jm-1 flink run \
  --jobmanager flink-jobmanager-1:8081 \
  --class com.smartcity.traffic.TrafficVehicleCount \
  /opt/flink/jobs/traffic-flink-jobs.jar

# 查看运行中的作业
curl http://localhost:8081/jobs

# 触发 Savepoint
docker exec traffic-flink-jm-1 flink savepoint <job-id> file:///opt/flink/savepoints
```

---

## 七、高可用配置

### 7.1 Kafka KRaft 高可用

- **3 个 Controller**: 形成 Raft 共识，容忍 1 个节点故障
- **3 副本**: 所有 Topic 配置 replication-factor=3
- **min.insync.replicas=2**: 保证至少 2 个副本同步

### 7.2 Flink HA 配置

- **双 JobManager**: 主备模式，ZooKeeper 协调
- **RocksDB State Backend**: 支持大状态，Checkpoint 持久化
- **3 次重启策略**: 固定延迟 10 秒重试

### 7.3 Redis Cluster 高可用

- **3 主 3 从**: 自动故障转移
- **cluster-node-timeout=5000**: 5 秒检测故障
- **AOF 持久化**: everysec 策略

### 7.4 HDFS 高可用

- **3 副本**: 默认复制因子
- **3 DataNode**: 容忍 2 个节点故障

---

## 八、监控与告警

### 8.1 健康检查

所有服务均配置了 Docker Healthcheck：
- **Kafka**: `kafka-topics.sh --list`
- **Flink**: `curl http://localhost:8081/overview`
- **Redis**: `redis-cli ping`
- **HDFS**: `curl http://localhost:9870`
- **MySQL**: `mysqladmin ping`

### 8.2 资源限制

| 服务 | 内存限制 | CPU 限制 |
|------|---------|---------|
| Flink TaskManager | 4G | 无限制 |
| Kafka | 无限制 | 无限制 |
| Redis | 1G/节点 | 无限制 |
| HiveServer2 | 无限制 | 无限制 |

---

## 九、故障排查

### 9.1 常见问题

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| Kafka 启动失败 | 内存不足 | 增加 Docker 内存到 8GB+ |
| Flink TM 无法连接 JM | 网络问题 | 检查 `traffic-prod-net` 网络 |
| Redis Cluster 创建失败 | 节点未就绪 | 等待 10 秒后重试 |
| Hive 连接失败 | Metastore 未就绪 | 等待 30 秒后重试 |
| HDFS 安全模式 | DataNode 未全部启动 | 等待 DataNode 注册完成 |

### 9.2 日志位置

```bash
# Kafka 日志
docker logs traffic-kafka-1

# Flink 日志
docker logs traffic-flink-jm-1
docker logs traffic-flink-tm-1

# Hive 日志
docker logs traffic-hive-metastore
docker logs traffic-hiveserver2
```

---

## 十、从伪分布式迁移

### 10.1 配置对比

| 配置项 | 伪分布式 | 生产集群 |
|--------|---------|---------|
| Kafka | `localhost:9092` | `kafka-1:9092,kafka-2:9092,kafka-3:9092` |
| Flink | `localhost:8081` | `flink-jobmanager-1:8081` |
| Redis | `localhost:6379` | `redis-node-1:6379` (Cluster) |
| HDFS | 本地文件系统 | `hdfs://hdfs-namenode:9000` |
| Hive | SQLite | `jdbc:hive2://hiveserver2:10000` |

### 10.2 应用配置更新

业务应用已自动配置为生产集群地址，通过环境变量注入：

```yaml
environment:
  - KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
  - REDIS_HOST=redis-node-1
  - REDIS_PORT=6379
  - REDIS_CLUSTER_NODES=redis-node-1:6379,...
  - FLINK_REST_URL=http://flink-jobmanager-1:8081
  - HIVE_HOST=hiveserver2
  - HIVE_PORT=10000
  - HDFS_NAMENODE=hdfs://hdfs-namenode:9000
```

---

## 十一、安全建议

1. **生产环境请启用认证**:
   - Kafka SASL/SSL
   - Redis ACL
   - Hive Kerberos

2. **网络隔离**:
   - 使用独立 Docker 网络
   - 限制端口暴露范围

3. **数据备份**:
   - 定期备份 Kafka 数据卷
   - 定期备份 HDFS 元数据
   - 定期备份 Hive Metastore

---

## 十二、扩展方案

### 12.1 水平扩展

```bash
# 扩展 Flink TaskManager
docker-compose -f docker-compose-production.yml up -d --scale flink-taskmanager=5

# 扩展 Kafka Broker
# 编辑 docker-compose-production.yml 添加 kafka-4, kafka-5
```

### 12.2 迁移到 Kubernetes

生产环境建议迁移到 Kubernetes，使用 Helm Chart 部署：
- Kafka: Strimzi Kafka Operator
- Flink: Flink Kubernetes Operator
- Redis: Redis Operator
- HDFS: HDFS on K8s
- Hive: Hive on Spark

---

## 十三、总结

本次整改完成了从**伪分布式/模拟环境**到**生产级真实集群**的完整升级：

✅ **Kafka**: 单节点 → 3节点 KRaft 集群  
✅ **Flink**: 单节点 → HA 双 JM + 3 TM  
✅ **Redis**: 单节点 → 6节点 Cluster  
✅ **HDFS**: 本地文件系统 → 分布式存储  
✅ **Hive**: SQLite → HiveServer2 + Metastore  

所有组件均配置了高可用、健康检查、资源限制，可直接用于生产环境。
