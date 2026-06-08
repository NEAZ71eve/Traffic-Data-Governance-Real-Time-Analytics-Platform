CREATE EXTERNAL TABLE IF NOT EXISTS ods_alarm_log_di (
    alarm_id STRING COMMENT '告警ID',
    device_id STRING COMMENT '设备ID',
    alarm_type STRING COMMENT '告警类型',
    alarm_level STRING COMMENT '告警级别(CRITICAL/WARNING/INFO)',
    alarm_time STRING COMMENT '告警时间',
    recover_time STRING COMMENT '恢复时间',
    alarm_desc STRING COMMENT '告警描述',
    status STRING COMMENT '告警状态(ACTIVE/RESOLVED)',
    dt STRING COMMENT '分区日期'
) COMMENT '故障告警原始数据表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_alarm_log_di'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai'
);