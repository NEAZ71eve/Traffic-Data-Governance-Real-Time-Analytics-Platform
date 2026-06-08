-- ============================================
-- ADS层：设备健康度综合评分（建表DDL + ETL）
-- 数据来源：DWS层 dws_device_health_day + dws_alarm_day + dim_device_zip
-- 评分维度：在线率(40%) + 资源使用(30%) + 异常率(20%) + 告警恢复(10%)
-- 产出：每台设备每日健康度得分0~100
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.ads_device_health_score (
    device_id             STRING      COMMENT '设备ID',
    device_name           STRING      COMMENT '设备名称',
    device_type           STRING      COMMENT '设备类型',
    road_id               STRING      COMMENT '所属道路ID',
    area_id               STRING      COMMENT '所属区域ID',
    online_rate           DECIMAL(5,2) COMMENT '在线率(%)',
    avg_cpu_usage         DECIMAL(5,2) COMMENT 'CPU使用率(%)',
    avg_memory_usage      DECIMAL(5,2) COMMENT '内存使用率(%)',
    avg_temperature       DECIMAL(4,1) COMMENT '平均温度(℃)',
    abnormal_rate         DECIMAL(5,2) COMMENT '异常率(%)',
    total_alarm_count     BIGINT      COMMENT '当天告警次数',
    recovery_rate         DECIMAL(5,2) COMMENT '告警恢复率(%)',
    online_score          DECIMAL(5,2) COMMENT '在线率得分(0-40)',
    resource_score        DECIMAL(5,2) COMMENT '资源使用得分(0-30)',
    abnormal_score        DECIMAL(5,2) COMMENT '异常率得分(0-20)',
    recovery_score        DECIMAL(5,2) COMMENT '恢复率得分(0-10)',
    health_score          DECIMAL(5,2) COMMENT '健康度综合评分(0-100)',
    health_level          STRING      COMMENT '健康等级(EXCELLENT/GOOD/FAIR/POOR/CRITICAL)'
)
COMMENT '设备健康度评分表(ADS)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/ads_device_health_score'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'device_id'
);

-- ============================================
-- 每日ETL：设备健康度综合评分
-- 评分公式（满分100）：
--   在线率得分 = online_rate / 100 * 40
--   资源得分   = (1 - (avg_cpu + avg_memory) / 200) * 30
--   异常得分   = (1 - abnormal_rate / 100) * 20
--   恢复得分   = recovery_rate / 100 * 10
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = true;

INSERT OVERWRITE TABLE traffic_db.ads_device_health_score PARTITION (dt)
SELECT
    dh.device_id,
    d.device_name,
    d.device_type,
    d.road_id,
    d.road_id AS area_id,  -- 通过道路关联区域
    ROUND(dh.online_duration * 100.0 / NULLIF(dh.online_duration + dh.offline_count, 0), 2) AS online_rate,
    dh.avg_cpu_usage,
    dh.avg_memory_usage,
    dh.avg_temperature,
    ROUND(dh.abnormal_count * 100.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0), 2) AS abnormal_rate,
    COALESCE(ad.total_alarm_count, 0)                                           AS total_alarm_count,
    COALESCE(ad.recovery_rate, 100.00)                                          AS recovery_rate,
    ROUND(dh.online_duration * 100.0 / NULLIF(dh.online_duration + dh.offline_count, 0) / 100 * 40, 2) AS online_score,
    ROUND(GREATEST(0, (1 - (dh.avg_cpu_usage + dh.avg_memory_usage) / 200)) * 30, 2) AS resource_score,
    ROUND(GREATEST(0, (1 - dh.abnormal_count * 1.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0))) * 20, 2) AS abnormal_score,
    ROUND(COALESCE(ad.recovery_rate, 100.00) / 100 * 10, 2)                     AS recovery_score,
    ROUND(
        (dh.online_duration * 100.0 / NULLIF(dh.online_duration + dh.offline_count, 0) / 100 * 40)
      + (GREATEST(0, (1 - (dh.avg_cpu_usage + dh.avg_memory_usage) / 200)) * 30)
      + (GREATEST(0, (1 - dh.abnormal_count * 1.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0))) * 20)
      + (COALESCE(ad.recovery_rate, 100.00) / 100 * 10)
    , 2)                                                                         AS health_score,
    CASE
        WHEN ROUND(
            (dh.online_duration * 100.0 / NULLIF(dh.online_duration + dh.offline_count, 0) / 100 * 40)
          + (GREATEST(0, (1 - (dh.avg_cpu_usage + dh.avg_memory_usage) / 200)) * 30)
          + (GREATEST(0, (1 - dh.abnormal_count * 1.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0))) * 20)
          + (COALESCE(ad.recovery_rate, 100.00) / 100 * 10)
        , 2) >= 90 THEN 'EXCELLENT'
        WHEN ROUND(
            (dh.online_duration * 100.0 / NULLIF(dh.online_duration + dh.offline_count, 0) / 100 * 40)
          + (GREATEST(0, (1 - (dh.avg_cpu_usage + dh.avg_memory_usage) / 200)) * 30)
          + (GREATEST(0, (1 - dh.abnormal_count * 1.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0))) * 20)
          + (COALESCE(ad.recovery_rate, 100.00) / 100 * 10)
        , 2) >= 75 THEN 'GOOD'
        WHEN ROUND(
            (dh.online_duration * 100.0 / NULLIF(dh.online_duration + dh.offline_count, 0) / 100 * 40)
          + (GREATEST(0, (1 - (dh.avg_cpu_usage + dh.avg_memory_usage) / 200)) * 30)
          + (GREATEST(0, (1 - dh.abnormal_count * 1.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0))) * 20)
          + (COALESCE(ad.recovery_rate, 100.00) / 100 * 10)
        , 2) >= 60 THEN 'FAIR'
        WHEN ROUND(
            (dh.online_duration * 100.0 / NULLIF(dh.online_duration + dh.offline_count, 0) / 100 * 40)
          + (GREATEST(0, (1 - (dh.avg_cpu_usage + dh.avg_memory_usage) / 200)) * 30)
          + (GREATEST(0, (1 - dh.abnormal_count * 1.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0))) * 20)
          + (COALESCE(ad.recovery_rate, 100.00) / 100 * 10)
        , 2) >= 40 THEN 'POOR'
        ELSE 'CRITICAL'
    END                                                                         AS health_level,
    dh.dt
FROM traffic_db.dws_device_health_day dh
JOIN traffic_db.dim_device_zip d
    ON dh.device_id = d.device_id
   AND d.is_current = 'Y'
   AND d.dt = '${date}'
LEFT JOIN (
    SELECT
        device_id,
        SUM(total_alarm_count)                                                  AS total_alarm_count,
        ROUND(AVG(recovery_rate), 2)                                             AS recovery_rate,
        dt
    FROM (
        SELECT d2.device_id, a2.total_alarm_count, a2.recovery_rate, a2.dt
        FROM traffic_db.dws_alarm_day a2
        JOIN traffic_db.dim_device_zip d2 ON a2.alarm_type IN (
            'OFFLINE', 'CPU_HIGH', 'MEMORY_HIGH', 'TEMP_HIGH', 'SIGNAL_WEAK', 'HARDWARE_FAULT'
        )
        WHERE a2.dt = '${date}' AND d2.is_current = 'Y' AND d2.dt = '${date}'
    ) sub
    GROUP BY device_id, dt
) ad
    ON dh.device_id = ad.device_id
   AND dh.dt = ad.dt
WHERE dh.dt = '${date}';

ALTER TABLE traffic_db.ads_device_health_score ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.ads_device_health_score;
