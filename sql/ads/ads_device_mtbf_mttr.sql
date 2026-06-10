-- ============================================
-- ADS层：设备MTBF/MTTR可靠性指标（建表DDL + ETL）
-- 数据来源：DWS层 dws_alarm_day + dim_device_zip
-- MTBF = 运行时间 / 故障次数（Mean Time Between Failures）
-- MTTR = 总恢复时间 / 恢复次数（Mean Time To Repair）
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.ads_device_mtbf_mttr (
    device_id             STRING      COMMENT '设备ID',
    device_name           STRING      COMMENT '设备名称',
    device_type           STRING      COMMENT '设备类型',
    manufacturer          STRING      COMMENT '生产厂商',
    road_id               STRING      COMMENT '所属道路ID',
    total_alarm_count     BIGINT      COMMENT '告警总次数',
    recovered_count       BIGINT      COMMENT '已恢复次数',
    avg_recover_minutes   DECIMAL(10,2) COMMENT '平均恢复时长(分钟)',
    total_runtime_minutes BIGINT      COMMENT '总运行时长(分钟/天=1440)',
    mtbf_minutes          DECIMAL(12,2) COMMENT 'MTBF(分钟)',
    mttr_minutes          DECIMAL(10,2) COMMENT 'MTTR(分钟)',
    mtbf_hours            DECIMAL(10,2) COMMENT 'MTBF(小时)',
    reliability_rank      STRING      COMMENT '可靠性等级(HIGH/MEDIUM/LOW)'
)
COMMENT '设备MTBF/MTTR指标表(ADS)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/ads_device_mtbf_mttr'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'device_id'
);

-- ============================================
-- 每日ETL
-- MTBF = 1440 / 故障次数（一天分钟数 / 故障数）
-- MTTR = 总恢复时间 / 恢复次数
-- 可靠性等级：MTBF > 720(12h) 高 / 144~720 中 / <144 低
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = true;

INSERT OVERWRITE TABLE traffic_db.ads_device_mtbf_mttr PARTITION (dt)
SELECT
    d.device_id,
    d.device_name,
    d.device_type,
    d.manufacturer,
    d.road_id,
    COALESCE(alarm.total_alarm, 0)                                              AS total_alarm_count,
    COALESCE(alarm.recovered, 0)                                                AS recovered_count,
    ROUND(COALESCE(alarm.avg_recover, 0), 2)                                    AS avg_recover_minutes,
    1440                                                                        AS total_runtime_minutes,
    CASE
        WHEN COALESCE(alarm.total_alarm, 0) = 0 THEN 1440
        ELSE ROUND(1440.0 / COALESCE(alarm.total_alarm, 1), 2)
    END                                                                         AS mtbf_minutes,
    ROUND(COALESCE(alarm.avg_recover, 0), 2)                                    AS mttr_minutes,
    CASE
        WHEN COALESCE(alarm.total_alarm, 0) = 0 THEN 24.0
        ELSE ROUND(1440.0 / COALESCE(alarm.total_alarm, 1) / 60, 2)
    END                                                                         AS mtbf_hours,
    CASE
        WHEN COALESCE(alarm.total_alarm, 0) = 0 THEN 'HIGH'
        WHEN (1440.0 / COALESCE(alarm.total_alarm, 1)) >= 720 THEN 'HIGH'
        WHEN (1440.0 / COALESCE(alarm.total_alarm, 1)) >= 144 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                                         AS reliability_rank,
    '${date}'                                                                   AS dt
FROM traffic_db.dim_device_zip d
LEFT JOIN (
    SELECT
        device_id,
        SUM(total_alarm_count)                                                  AS total_alarm,
        SUM(recovered_count)                                                    AS recovered,
        ROUND(AVG(avg_recover_minutes), 2)                                      AS avg_recover,
        dt
    FROM (
        SELECT a.device_id, al.total_alarm_count, al.recovered_count,
               al.avg_recover_minutes, al.dt
        FROM traffic_db.dws_alarm_day al
        JOIN traffic_db.dim_device_zip a
          ON a.device_id = al.device_id
        WHERE al.dt = '${date}'
          AND a.is_current = 'Y'
          AND a.dt = '${date}'
    ) sub
    GROUP BY device_id, dt
) alarm
    ON d.device_id = alarm.device_id
WHERE d.is_current = 'Y'
  AND d.dt = '${date}';

ALTER TABLE traffic_db.ads_device_mtbf_mttr ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.ads_device_mtbf_mttr;
