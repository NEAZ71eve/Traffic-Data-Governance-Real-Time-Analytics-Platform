CREATE EXTERNAL TABLE IF NOT EXISTS ods_device_status_di (
    device_id STRING COMMENT '设备ID',
    cpu_usage DECIMAL(5,2) COMMENT 'CPU使用率(%)',
    memory_usage DECIMAL(5,2) COMMENT '内存使用率(%)',
    temperature DECIMAL(4,1) COMMENT '设备温度(℃)',
    online_flag STRING COMMENT '在线状态(ONLINE/OFFLINE)',
    heartbeat_time STRING COMMENT '心跳时间',
    signal_strength INT COMMENT '信号强度',
    device_type STRING COMMENT '设备类型',
    dt STRING COMMENT '分区日期'
) COMMENT '设备状态原始数据表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_device_status_di'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai'
);