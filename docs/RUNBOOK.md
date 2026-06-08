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

## 十、故障排查

### 10.1 日志位置

| 组件 | 日志路径 |
|------|----------|
| Flink | /var/log/flink/ |
| Kafka | /var/log/kafka/ |
| Hive | /var/log/hive/ |
| DolphinScheduler | /opt/dolphinscheduler/logs/ |

### 10.2 常见问题

| 问题 | 解决方案 |
|------|----------|
| Kafka消费者无数据 | 检查Topic是否存在、分区是否正确 |
| Hive任务失败 | 检查分区是否存在、权限是否正确 |
| Flink任务失败 | 检查Watermark配置、状态后端配置 |
| 数据质量告警 | 检查数据源、清洗规则 |