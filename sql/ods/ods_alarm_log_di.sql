-- ============================================
-- ODS层：设备故障告警记录表（增量日分区）
-- 数据来源：Kafka device_alarm -> Flume -> HDFS -> Hive外部表
-- 存储格式：TEXTFILE（保留原始格式）
-- 分区字段：dt（日期分区 yyyy-MM-dd）
-- ============================================

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.ods_alarm_log_di (
    alarm_id        STRING  COMMENT '告警ID',
    device_id       STRING  COMMENT '设备ID(关联dim_device_zip)',
    alarm_type      STRING  COMMENT '告警类型(OFFLINE/CPU_HIGH/MEMORY_HIGH/TEMP_HIGH/SIGNAL_WEAK/HARDWARE_FAULT)',
    alarm_level     STRING  COMMENT '告警级别(CRITICAL/MAJOR/MINOR/WARNING)',
    alarm_content   STRING  COMMENT '告警内容描述',
    alarm_time      STRING  COMMENT '告警发生时间(yyyy-MM-dd HH:mm:ss)',
    recover_time    STRING  COMMENT '恢复时间(yyyy-MM-dd HH:mm:ss，未恢复为NULL)',
    recover_status  STRING  COMMENT '恢复状态(RECOVERED/UNRECOVERED)'
)
COMMENT '设备故障告警ODS原始记录表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_alarm_log_di'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai',
    'skip.header.line.count' = '0'
);
