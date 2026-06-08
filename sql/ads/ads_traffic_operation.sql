-- ============================================
-- ADS层：交通运营综合仪表盘（建表DDL + ETL）
-- 数据来源：DWS层 dws_road_hour_flow + dws_area_jam_hour
-- 用途：交通运营看板的综合指标（城市级/区域级）
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.ads_traffic_operation (
    city_id             STRING      COMMENT '城市ID',
    city_name           STRING      COMMENT '城市名称',
    area_id             STRING      COMMENT '区域ID',
    area_name           STRING      COMMENT '区域名称',
    peak_period         STRING      COMMENT '时段',
    total_vehicle_flow  BIGINT      COMMENT '总车流量',
    avg_speed_all       DECIMAL(8,2) COMMENT '全市平均车速(km/h)',
    peak_flow           BIGINT      COMMENT '高峰车流量',
    peak_hour           INT         COMMENT '车流高峰小时',
    jam_road_count      BIGINT      COMMENT '拥堵道路数',
    jam_road_ratio      DECIMAL(5,2) COMMENT '拥堵道路占比(%)',
    severe_jam_count    BIGINT      COMMENT '严重拥堵道路数',
    avg_congestion_rate DECIMAL(5,2) COMMENT '平均拥堵率(%)',
    total_road_count    BIGINT      COMMENT '道路总数'
)
COMMENT '交通运营综合指标表(ADS)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/ads_traffic_operation'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'city_id,area_id'
);

-- ============================================
-- 每日ETL
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = true;

INSERT OVERWRITE TABLE traffic_db.ads_traffic_operation PARTITION (dt)
SELECT
    a.city_id,
    MAX(a.city_name)                                                            AS city_name,
    a.area_id,
    MAX(a.area_name)                                                            AS area_name,
    dh.peak_period,
    SUM(dh.traffic_count)                                                       AS total_vehicle_flow,
    ROUND(AVG(dh.avg_speed), 2)                                                AS avg_speed_all,
    MAX(dh.traffic_count)                                                       AS peak_flow,
    MAX(CASE WHEN dh.traffic_count = max_flow.max_f THEN dh.hour END)          AS peak_hour,
    SUM(CASE WHEN jh.jam_level >= 3 THEN 1 ELSE 0 END)                        AS jam_road_count,
    ROUND(SUM(CASE WHEN jh.jam_level >= 3 THEN 1 ELSE 0 END) * 100.0
          / NULLIF(COUNT(DISTINCT jh.hour), 0), 2)                             AS jam_road_ratio,
    SUM(CASE WHEN jh.jam_level = 5 THEN 1 ELSE 0 END)                         AS severe_jam_count,
    ROUND(AVG(jh.avg_congestion_rate), 2)                                       AS avg_congestion_rate,
    COUNT(DISTINCT dh.road_id)                                                  AS total_road_count,
    dh.dt
FROM traffic_db.dws_road_hour_flow dh
JOIN traffic_db.dim_road_zip r
    ON dh.road_id = r.road_id
   AND r.is_current = 'Y'
   AND r.dt = '${date}'
JOIN traffic_db.dim_area a
    ON r.area_id = a.area_id
LEFT JOIN (
    SELECT road_id, dt, MAX(traffic_count) AS max_f
    FROM traffic_db.dws_road_hour_flow
    WHERE dt = '${date}'
    GROUP BY road_id, dt
) max_flow
    ON dh.road_id = max_flow.road_id
   AND dh.dt = max_flow.dt
LEFT JOIN traffic_db.dws_area_jam_hour jh
    ON a.area_id = jh.area_id
   AND dh.dt = jh.dt
   AND dh.hour = jh.hour
WHERE dh.dt = '${date}'
GROUP BY a.city_id, a.area_id, dh.hour, dh.peak_period, dh.dt;

ALTER TABLE traffic_db.ads_traffic_operation ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.ads_traffic_operation;
