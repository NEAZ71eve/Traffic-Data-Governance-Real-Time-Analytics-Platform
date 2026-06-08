# 运维手册

## 一、环境准备

### 1.1 依赖安装

```bash
# Python依赖
pip install pyhive kafka-python

# Java依赖（Flink）
# 需要Java 8+
# 需要Flink 1.14+
```

### 1.2 配置文件

```bash
# 复制配置文件模板
cp config/hive_config.json.example config/hive_config.json
cp config/kafka_topics.json.example config/kafka_topics.json
```

## 二、表结构初始化

### 2.1 创建数据库

```sql
CREATE DATABASE IF NOT EXISTS traffic_db;
USE traffic_db;
```

### 2.2 创建ODS层表

```bash
hive -f sql/ods/ods_vehicle_pass_di.sql
hive -f sql/ods/ods_traffic_status_di.sql
hive -f sql/ods/ods_device_status_di.sql
hive -f sql/ods/ods_alarm_log_di.sql
```

### 2.3 创建DIM层表

```bash
hive -f sql/dim/dim_road_zip.sql
hive -f sql/dim/dim_device_zip.sql
hive -f sql/dim/dim_time.sql
hive -f sql/dim/dim_area.sql
```

### 2.4 创建DWD/DWS/ADS层表

```bash
# 创建DWD层
hive -f sql/dwd/dwd_vehicle_pass_di.sql
# ... 其他DWD表

# 创建DWS层
hive -f sql/dws/dws_road_hour_flow.sql
# ... 其他DWS表

# 创建ADS层
hive -f sql/ads/ads_traffic_operation.sql
# ... 其他ADS表
```

## 三、Kafka Topic创建

```bash
kafka-topics.sh --create \
    --topic traffic_vehicle \
    --partitions 8 \
    --replication-factor 3 \
    --bootstrap-server kafka:9092

kafka-topics.sh --create \
    --topic traffic_status \
    --partitions 4 \
    --replication-factor 3 \
    --bootstrap-server kafka:9092

kafka-topics.sh --create \
    --topic device_status \
    --partitions 4 \
    --replication-factor 3 \
    --bootstrap-server kafka:9092

kafka-topics.sh --create \
    --topic device_alarm \
    --partitions 4 \
    --replication-factor 3 \
    --bootstrap-server kafka:9092
```

## 四、Flink任务部署

### 4.1 编译打包

```bash
cd flink
mvn clean package
```

### 4.2 提交任务

```bash
# 实时车流统计
flink run -c com.traffic.flink.TrafficVehicleCount \
    target/traffic-flink-1.0.jar

# 拥堵检测
flink run -c com.traffic.flink.TrafficCongestionDetection \
    target/traffic-flink-1.0.jar

# CEP异常检测
flink run -c com.traffic.flink.DeviceStatusCEP \
    target/traffic-flink-1.0.jar
```

### 4.3 Savepoint管理

```bash
# 手动触发Savepoint（用于升级/迁移）
flink savepoint <jobId> hdfs://namenode:8020/flink/savepoints/

# 从Savepoint恢复
flink run -s hdfs://namenode:8020/flink/savepoints/savepoint-xxx \
    -c com.traffic.flink.TrafficVehicleCount \
    target/traffic-flink-1.0.jar

# 取消任务并保留Savepoint
flink cancel -s <jobId>
```

### 4.4 Checkpoint恢复

```bash
# 任务崩溃后从Checkpoint自动恢复
# 配置：RETAIN_ON_CANCELLATION + fixedDelayRestart(3, 60s)

# 查看Checkpoint列表
hdfs dfs -ls /flink/checkpoints/<jobId>/
```

## 五、DolphinScheduler配置

### 5.1 创建项目

```bash
curl -X POST http://dolphinscheduler:12345/dolphinscheduler/projects/create \
    -H "Content-Type: application/json" \
    -d '{
        "name": "traffic-data-platform",
        "description": "智慧城市交通数据平台"
    }'
```

### 5.2 导入工作流

```bash
curl -X POST http://dolphinscheduler:12345/dolphinscheduler/workflow/import \
    -H "Content-Type: application/json" \
    -d @config/dolphinscheduler_config.json
```

## 六、数据质量监控

### 6.1 运行监控脚本

```bash
python python/data_quality_monitor.py
```

### 6.2 查看报告

```bash
cat /tmp/data_quality_report.json
```

## 七、数据血缘管理

### 7.1 导出血缘

```bash
python python/data_lineage.py
```

### 7.2 生成可视化图

```bash
python -c "
from data_lineage import DataLineageManager
manager = DataLineageManager()
print(manager.visualize_lineage())
" > /tmp/lineage.dot

dot -Tpng /tmp/lineage.dot -o /tmp/lineage.png
```

## 八、AI辅助工具

### 8.1 ETL脚本生成

```bash
python -c "
from ai_etl_generator import ETLScriptGenerator
generator = ETLScriptGenerator()
ddl = generator.generate_ods_ddl('ods_custom_table', 'vehicle', '自定义表')
print(ddl)
"
```

### 8.2 NL2SQL转换

```bash
python -c "
from ai_etl_generator import NL2SQLConverter
converter = NL2SQLConverter()
sql = converter.convert('今天最拥堵的5条道路', date='2024-01-15', limit=5)
print(sql)
"
```

## 九、日常运维

### 9.1 检查Kafka Lag

```bash
kafka-consumer-groups.sh --describe \
    --group traffic_vehicle_group \
    --bootstrap-server kafka:9092
```

### 9.2 检查Flink任务状态

```bash
flink list
```

### 9.3 检查Hive分区

```bash
hive -e "SHOW PARTITIONS dwd_vehicle_pass_di;"
```

### 9.4 重启失败任务

```bash
# DolphinScheduler
curl -X POST http://dolphinscheduler:12345/dolphinscheduler/workflow/executor/restart \
    -H "Content-Type: application/json" \
    -d '{"workflowInstanceId": 123}'
```

## 十、数据回溯与重跑

### 10.1 手动重跑指定日期分区

```bash
# 步骤1：检查目标分区是否存在
hive -e "SHOW PARTITIONS traffic_db.ods_vehicle_pass_di;"

# 步骤2：删除目标分区
hive -e "ALTER TABLE traffic_db.ods_vehicle_pass_di DROP IF EXISTS PARTITION(dt='2024-01-15');"

# 步骤3：按依赖顺序重新执行
# ODS → DIM → DWD → DWS → ADS
hive -hiveconf date=2024-01-15 -f sql/dwd/dwd_vehicle_pass_di.sql
hive -hiveconf date=2024-01-15 -f sql/dws/dws_road_hour_flow.sql
hive -hiveconf date=2024-01-15 -f sql/ads/ads_traffic_operation.sql

# 步骤4：数据质量校验
python python/data_quality_monitor.py --date 2024-01-15
```

### 10.2 DolphinScheduler 补数重跑

```bash
# 通过API补跑指定日期
curl -X POST http://dolphinscheduler:12345/dolphinscheduler/projects/traffic-data-platform/executor/execute \
    -H "Content-Type: application/json" \
    -d '{
        "scheduleTime": "2024-01-15 00:00:00",
        "failureStrategy": "END",
        "warningType": "FAILURE",
        "processInstancePriority": "HIGHEST"
    }'
```

### 10.3 Flink从Kafka故障降级

```bash
# Kafka集群故障时，切换读取HDFS离线数据
# 修改启动脚本的环境变量
export KAFKA_DOWN=true
flink run -c com.traffic.flink.TrafficVehicleCount \
    --kafka.down.mode true \
    target/traffic-flink-1.0.jar
```

## 十一、故障排查

### 11.1 日志位置

| 组件 | 日志路径 |
|------|----------|
| Flink | /var/log/flink/ |
| Kafka | /var/log/kafka/ |
| Hive | /var/log/hive/ |
| DolphinScheduler | /opt/dolphinscheduler/logs/ |

### 11.2 常见问题

| 问题 | 解决方案 |
|------|----------|
| Kafka消费者无数据 | 检查Topic是否存在、分区是否正确、`kafka-consumer-groups.sh --describe` |
| Hive任务失败 | 检查分区是否存在、权限是否正确、查看YARN日志 |
| Flink任务失败 | 检查Watermark配置、StateBackend配置、查看TaskManager日志 |
| Flink Checkpoint失败 | 检查HDFS路径权限、磁盘空间、调整Checkpoint超时时间 |
| 数据质量告警 | 检查数据源、清洗规则、通过`data_quality_monitor.py`定位具体异常指标 |
| Kafka Lag积压 | 增加消费者并行度、检查是否有慢查询阻塞、考虑扩容分区 |
| DolphinScheduler调度失败 | 检查依赖任务状态、查看任务日志、确认参数正确传递 |
| 数据回溯失败 | 确保目标分区已删除、按依赖顺序执行、检查HDFS空间 |