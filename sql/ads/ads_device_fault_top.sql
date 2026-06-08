-- ============================================
-- ADS层：故障率TOP设备榜（建表DDL + ETL）
-- 数据来源：DWS层 dws_device_health_day + dim_device_zip
-- 用途：展示故障率最高的TOP N设备排行
-- 指标：故障率、异常次数、在线率
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.ads_device_fault_top (
    rank_num              INT         COMMENT '排名',
    device_id             STRING      COMMENT '设备ID',
    device_name           STRING      COMMENT '设备名称',
    device_type           STRING      COMMENT '设备类型',
    manufacturer          STRING      COMMENT '生产厂商',
    road_id               STRING      COMMENT '所属道路ID',
    area_id               STRING      COMMENT '所属区域ID',
    area_name             STRING      COMMENT '区域名称',
    fault_rate            DECIMAL(5,2) COMMENT '故障率(%)',
    abnormal_count        BIGINT      COMMENT '异常次数',
    warning_count         BIGINT      COMMENT '预警次数',
    offline_count         BIGINT      COMMENT '离线次数',
    online_rate           DECIMAL(5,2) COMMENT '在线率(%)',
    avg_cpu_usage         DECIMAL(5,2) COMMENT '平均CPU(%)',
    avg_memory_usage      DECIMAL(5,2) COMMENT '平均内存(%)',
    avg_temperature       DECIMAL(4,1) COMMENT '平均温度(℃)',
    health_level          STRING      COMMENT '健康等级'
)
COMMENT '故障率TOP设备榜(ADS)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/ads_device_fault_top'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'device_id'
);

-- ============================================
-- 每日ETL：产出故障率最高的TOP 20设备
-- 故障率 = abnormal_count / (abnormal + warning + normal) × 100
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = true;

INSERT OVERWRITE TABLE traffic_db.ads_device_fault_top PARTITION (dt)
SELECT
    ROW_NUMBER() OVER (ORDER BY fault_rate DESC, abnormal_count DESC)           AS rank_num,
    dh.device_id,
    d.device_name,
    d.device_type,
    d.manufacturer,
    d.road_id,
    r.area_id,
    MAX(a.area_name)                                                            AS area_name,
    ROUND(dh.abnormal_count * 100.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0), 2) AS fault_rate,
    dh.abnormal_count,
    dh.warning_count,
    dh.offline_count,
    ROUND(dh.online_duration * 100.0 / NULLIF(dh.online_duration + dh.offline_count, 0), 2) AS online_rate,
    dh.avg_cpu_usage,
    dh.avg_memory_usage,
    dh.avg_temperature,
    CASE
        WHEN dh.abnormal_count * 100.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0) >= 50 THEN 'CRITICAL'
        WHEN dh.abnormal_count * 100.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0) >= 20 THEN 'POOR'
        WHEN dh.abnormal_count * 100.0 / NULLIF(dh.abnormal_count + dh.warning_count + dh.normal_count, 0) >= 10 THEN 'WARNING'
        ELSE 'NORMAL'
    END                                                                         AS health_level,
    dh.dt
FROM traffic_db.dws_device_health_day dh
JOIN traffic_db.dim_device_zip d
    ON dh.device_id = d.device_id
   AND d.is_current = 'Y'
   AND d.dt = '${date}'
JOIN traffic_db.dim_road_zip r
    ON d.road_id = r.road_id
   AND r.is_current = 'Y'
   AND r.dt = '${date}'
JOIN traffic_db.dim_area a
    ON r.area_id = a.area_id
WHERE dh.dt = '${date}'
ORDER BY fault_rate DESC, abnormal_count DESC
LIMIT 20;

ALTER TABLE traffic_db.ads_device_fault_top ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.ads_device_fault_top;
