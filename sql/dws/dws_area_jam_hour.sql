-- ============================================
-- DWS层：区域小时拥堵汇总表（建表DDL + ETL）
-- 数据来源：DWD层 dwd_traffic_status_di + dim_road_zip（关联道路维度获取area_id）
-- 聚合粒度：area_id + jam_level + peak_period
-- 指标：拥堵道路数、各等级拥堵数、平均拥堵率
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dws_area_jam_hour (
    area_id                STRING      COMMENT '区域ID',
    hour                   INT         COMMENT '小时(0-23)',
    peak_period            STRING      COMMENT '时段',
    jam_level              INT         COMMENT '拥堵等级(1-5)',
    affected_road_count    BIGINT      COMMENT '受影响道路数',
    jam_level1_cnt         BIGINT      COMMENT '畅通道路数',
    jam_level2_cnt         BIGINT      COMMENT '基本畅通道路数',
    jam_level3_cnt         BIGINT      COMMENT '轻度拥堵道路数',
    jam_level4_cnt         BIGINT      COMMENT '中度拥堵道路数',
    jam_level5_cnt         BIGINT      COMMENT '严重拥堵道路数',
    avg_congestion_rate    DECIMAL(5,2) COMMENT '平均拥堵率(%)',
    total_traffic_flow     BIGINT      COMMENT '总车流量'
)
COMMENT '区域小时拥堵汇总表(DWS)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dws_area_jam_hour'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'area_id'
);

-- ============================================
-- 每日ETL
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = true;
SET hive.optimize.skewjoin = true;

INSERT OVERWRITE TABLE traffic_db.dws_area_jam_hour PARTITION (dt)
SELECT
    a.area_id,
    HOUR(ts.sample_time)                                                        AS hour,
    t.peak_period,
    ts.jam_level,
    COUNT(DISTINCT ts.road_id)                                                  AS affected_road_count,
    SUM(CASE WHEN ts.jam_level = 1 THEN 1 ELSE 0 END)                          AS jam_level1_cnt,
    SUM(CASE WHEN ts.jam_level = 2 THEN 1 ELSE 0 END)                          AS jam_level2_cnt,
    SUM(CASE WHEN ts.jam_level = 3 THEN 1 ELSE 0 END)                          AS jam_level3_cnt,
    SUM(CASE WHEN ts.jam_level = 4 THEN 1 ELSE 0 END)                          AS jam_level4_cnt,
    SUM(CASE WHEN ts.jam_level = 5 THEN 1 ELSE 0 END)                          AS jam_level5_cnt,
    ROUND(AVG(ts.congestion_rate), 2)                                           AS avg_congestion_rate,
    SUM(ts.traffic_flow)                                                        AS total_traffic_flow,
    ts.dt
FROM traffic_db.dwd_traffic_status_di ts
JOIN traffic_db.dim_road_zip r
    ON ts.road_id = r.road_id
   AND r.is_current = 'Y'
   AND r.dt = '${date}'
JOIN traffic_db.dim_area a
    ON r.area_id = a.area_id
JOIN traffic_db.dim_time t
    ON t.date_key = '${date}'
   AND t.hour = HOUR(ts.sample_time)
WHERE ts.dt = '${date}'
GROUP BY a.area_id, HOUR(ts.sample_time), t.peak_period, ts.jam_level, ts.dt;

ALTER TABLE traffic_db.dws_area_jam_hour ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.dws_area_jam_hour;
