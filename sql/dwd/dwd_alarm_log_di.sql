-- ============================================
-- DWD层：告警日志明细表（建表DDL + ETL）
-- 数据来源：ODS层 ods_alarm_log_di
-- 清洗逻辑：告警时间解析、恢复状态标准化、派生字段
-- 派生字段：recover_duration_min（恢复耗时/分钟）、is_recovered
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dwd_alarm_log_di (
    alarm_id            STRING      COMMENT '告警ID',
    device_id           STRING      COMMENT '设备ID',
    alarm_type          STRING      COMMENT '告警类型',
    alarm_level         STRING      COMMENT '告警级别',
    alarm_content       STRING      COMMENT '告警内容描述',
    alarm_time          TIMESTAMP   COMMENT '告警发生时间',
    recover_time        TIMESTAMP   COMMENT '恢复时间',
    recover_status      STRING      COMMENT '恢复状态',
    recover_duration_min BIGINT     COMMENT '恢复耗时(分钟)',
    is_recovered        STRING      COMMENT '当日是否恢复(Y/N)'
)
COMMENT '告警日志明细清洗表(DWD)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dwd_alarm_log_di'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'device_id,alarm_type'
);

-- ============================================
-- 每日ETL
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE traffic_db.dwd_alarm_log_di PARTITION (dt)
SELECT
    alarm_id,
    device_id,
    CASE
        WHEN alarm_type IN ('OFFLINE', 'CPU_HIGH', 'MEMORY_HIGH', 'TEMP_HIGH', 'SIGNAL_WEAK', 'HARDWARE_FAULT')
        THEN alarm_type
        ELSE 'OTHER'
    END                                                                         AS alarm_type,
    CASE
        WHEN alarm_level IN ('CRITICAL', 'MAJOR', 'MINOR', 'WARNING') THEN alarm_level
        ELSE 'WARNING'
    END                                                                         AS alarm_level,
    COALESCE(alarm_content, '')                                                 AS alarm_content,
    CAST(alarm_time AS TIMESTAMP)                                               AS alarm_time,
    CAST(recover_time AS TIMESTAMP)                                             AS recover_time,
    CASE
        WHEN recover_status IN ('RECOVERED', 'UNRECOVERED') THEN recover_status
        WHEN recover_time IS NOT NULL THEN 'RECOVERED'
        ELSE 'UNRECOVERED'
    END                                                                         AS recover_status,
    CASE
        WHEN recover_time IS NOT NULL
        THEN (UNIX_TIMESTAMP(recover_time) - UNIX_TIMESTAMP(alarm_time)) / 60
        ELSE NULL
    END                                                                         AS recover_duration_min,
    CASE WHEN recover_time IS NOT NULL THEN 'Y' ELSE 'N' END                   AS is_recovered,
    dt
FROM traffic_db.ods_alarm_log_di
WHERE dt = '${date}'
  AND alarm_id IS NOT NULL
  AND device_id IS NOT NULL;

ALTER TABLE traffic_db.dwd_alarm_log_di ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.dwd_alarm_log_di;
