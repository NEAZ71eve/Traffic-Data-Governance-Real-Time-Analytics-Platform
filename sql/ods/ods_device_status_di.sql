-- ============================================
-- ODS层：设备状态快照表（增量日分区）
-- 数据来源：Kafka device_status -> Flume -> HDFS -> Hive外部表
-- 存储格式：TEXTFILE（保留原始格式）
-- 分区字段：dt（日期分区 yyyy-MM-dd）
-- ============================================

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.ods_device_status_di (
    device_id       STRING      COMMENT '设备ID',
    cpu_usage       DECIMAL(5,2) COMMENT 'CPU使用率(%)',
    memory_usage    DECIMAL(5,2) COMMENT '内存使用率(%)',
    temperature     DECIMAL(4,1) COMMENT '设备温度(℃)',
    online_flag     STRING      COMMENT '在线状态(ONLINE/OFFLINE/UNKNOWN)',
    heartbeat_time  STRING      COMMENT '心跳时间(yyyy-MM-dd HH:mm:ss)',
    signal_strength INT         COMMENT '信号强度(dBm)',
    device_type     STRING      COMMENT '设备类型(CAMERA/SENSOR/RADAR/GATE)'
)
COMMENT '设备状态ODS原始快照表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_device_status_di'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai',
    'skip.header.line.count' = '0'
);
