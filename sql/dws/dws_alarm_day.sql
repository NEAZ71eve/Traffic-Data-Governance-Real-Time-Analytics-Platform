-- ============================================
-- DWS层：告警日汇总表（建表DDL + ETL）
-- 数据来源：DWD层 dwd_alarm_log_di
-- 聚合粒度：alarm_type + alarm_level + day
-- 指标：告警数量、恢复数量、平均恢复时长
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dws_alarm_day (
    alarm_type              STRING      COMMENT '告警类型',
    alarm_level             STRING      COMMENT '告警级别',
    total_alarm_count       BIGINT      COMMENT '告警总数',
    recovered_count         BIGINT      COMMENT '已恢复数',
    unrecovered_count       BIGINT      COMMENT '未恢复数',
    recovery_rate           DECIMAL(5,2) COMMENT '恢复率(%)',
    avg_recover_minutes     DECIMAL(10,2) COMMENT '平均恢复时长(分钟)',
    max_recover_minutes     BIGINT      COMMENT '最长恢复时长(分钟)',
    min_recover_minutes     BIGINT      COMMENT '最短恢复时长(分钟)',
    affected_device_count   BIGINT      COMMENT '受影响设备数',
    critical_count          BIGINT      COMMENT 'CRITICAL级别数',
    major_count             BIGINT      COMMENT 'MAJOR级别数',
    minor_count             BIGINT      COMMENT 'MINOR级别数',
    warning_count           BIGINT      COMMENT 'WARNING级别数'
)
COMMENT '告警日汇总表(DWS)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dws_alarm_day'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'alarm_type'
);

-- ============================================
-- 每日ETL
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE traffic_db.dws_alarm_day PARTITION (dt)
SELECT
    alarm_type,
    alarm_level,
    COUNT(1)                                                                    AS total_alarm_count,
    SUM(CASE WHEN is_recovered = 'Y' THEN 1 ELSE 0 END)                       AS recovered_count,
    SUM(CASE WHEN is_recovered = 'N' THEN 1 ELSE 0 END)                       AS unrecovered_count,
    ROUND(SUM(CASE WHEN is_recovered = 'Y' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(1), 0), 2) AS recovery_rate,
    ROUND(AVG(recover_duration_min), 2)                                         AS avg_recover_minutes,
    MAX(recover_duration_min)                                                   AS max_recover_minutes,
    MIN(recover_duration_min)                                                   AS min_recover_minutes,
    COUNT(DISTINCT device_id)                                                   AS affected_device_count,
    SUM(CASE WHEN alarm_level = 'CRITICAL' THEN 1 ELSE 0 END)                 AS critical_count,
    SUM(CASE WHEN alarm_level = 'MAJOR'    THEN 1 ELSE 0 END)                 AS major_count,
    SUM(CASE WHEN alarm_level = 'MINOR'    THEN 1 ELSE 0 END)                 AS minor_count,
    SUM(CASE WHEN alarm_level = 'WARNING'  THEN 1 ELSE 0 END)                 AS warning_count,
    dt
FROM traffic_db.dwd_alarm_log_di
WHERE dt = '${date}'
GROUP BY alarm_type, alarm_level, dt;

ALTER TABLE traffic_db.dws_alarm_day ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.dws_alarm_day;
