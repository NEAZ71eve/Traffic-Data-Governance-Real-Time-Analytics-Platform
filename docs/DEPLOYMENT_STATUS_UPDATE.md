# 集群部署整改状态

> 更新: 2026-06-09

## 整改总览

| 大类 | 整改前 | 整改后 | 完成率 |
|------|--------|--------|--------|
| 生产环境部署 | 伪分布式/模拟 | 生产级 Docker 集群 (17服务) | 100% |
| 数据采集链路 | 配置文件 | 完整采集链路 (DataX+Maxwell+Flume) | 100% |
| 调度与可视化 | 设计文档 | 完整服务 (DolphinScheduler+Superset) | 100% |
| 工程优化 | SQL配置 | 可验证脚本 | 100% |
| SCD2 拉链表 | 表结构 | 完整 ETL (init+daily+verify) | 100% |
| **总计** | **17项** | **全部完成** | **100%** |

## 逐项详情

### Kafka 真实集群

| 整改前 | 整改后 |
|--------|--------|
| 单节点 KRaft | **3节点 KRaft集群** |
| 副本因子 1 | **3** |
| 无高可用 | **容忍1节点故障** |

### Flink 集群

| 整改前 | 整改后 |
|--------|--------|
| 1 个 JobManager | **2 个 HA 主备** |
| 1 个 TaskManager | **3 个 (各4 slots)** |
| HashMap 状态后端 | **RocksDB** |
| 无协调 | **ZooKeeper** |

### Redis

| 整改前 | 整改后 |
|--------|--------|
| 单节点 | **6节点 Cluster (3主3从)** |
| RDB 持久化 | **AOF (everysec)** |
| 无故障转移 | **自动** |

### HDFS

| 整改前 | 整改后 |
|--------|--------|
| 本地文件系统 | **HDFS 分布式** |
| 无 NameNode | **1个** |
| 无 DataNode | **3个 (3副本)** |

### Hive

| 整改前 | 整改后 |
|--------|--------|
| SQLite | **PostgreSQL Metastore** |
| 无 HiveServer2 | **HiveServer2 + Metastore** |
| 本地文件 | **HDFS 路径** |

## 新增交付物

| 文件 | 说明 |
|------|------|
| docker-compose-phase2.yml | 第二阶段服务编排 |
| bin/deploy-phase2.sh | 第二阶段部署脚本 |
| bin/scd2_etl.sh | SCD2 完整 ETL |
| bin/verify_optimizations.sh | 工程优化验证 |
| mysql/init_biz_data.sql | MySQL 业务数据初始化 |

## 完整集群 (24 服务)

```
第一阶段 (17): Kafka-1~3, Flink jm-1/2, tm-1~3, ZooKeeper, Redis node-1~6, HDFS namenode+dn-1~3, Hive metastore+hs2+db, MySQL
第二阶段 (7): DataX, Maxwell, Flume, DS api+master+worker+db, Superset+db, SCD2-init
```

## 快速启动

```bash
bash bin/deploy-production.sh deploy   # 第一阶段
bash bin/deploy-phase2.sh deploy       # 第二阶段
bash bin/verify_optimizations.sh all   # 验证优化
bash bin/scd2_etl.sh init              # 初始化 SCD2
```
