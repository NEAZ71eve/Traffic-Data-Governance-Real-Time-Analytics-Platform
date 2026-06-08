-- ============================================
-- ADS层：拥堵道路TOP榜单（建表DDL + ETL）
-- 数据来源：DWS层 dws_area_jam_hour + dim_road_zip
-- 用途：展示全市拥堵最严重的道路排行
-- 产出：TOP N拥堵道路及拥堵详情
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.ads_top_jam_roads (
    rank_num            INT         COMMENT '排名',
    road_id             STRING      COMMENT '道路ID',
    road_name           STRING      COMMENT '道路名称',
    road_type           STRING      COMMENT '道路类型',
    area_id             STRING      COMMENT '区域ID',
    area_name           STRING      COMMENT '区域名称',
    peak_period         STRING      COMMENT '最拥堵时段',
    avg_jam_level       DECIMAL(4,2) COMMENT '平均拥堵等级',
    max_jam_level       INT         COMMENT '最高拥堵等级',
    avg_congestion_rate DECIMAL(5,2) COMMENT '平均拥堵率(%)',
    total_traffic_flow  BIGINT      COMMENT '车流量',
    peak_time           INT         COMMENT '拥堵高峰小时(0-23)'
)
COMMENT '拥堵道路TOP榜(ADS)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/ads_top_jam_roads'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'road_id'
);

-- ============================================
-- 每日ETL：产出拥堵TOP 20道路
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = true;

INSERT OVERWRITE TABLE traffic_db.ads_top_jam_roads PARTITION (dt)
SELECT
    ROW_NUMBER() OVER (ORDER BY avg_jam_level DESC, avg_congestion_rate DESC) AS rank_num,
    road_id,
    road_name,
    road_type,
    area_id,
    area_name,
    peak_period,
    avg_jam_level,
    max_jam_level,
    avg_congestion_rate,
    total_traffic_flow,
    peak_time,
    dt
FROM (
    SELECT
        r.road_id,
        MAX(r.road_name)                                                        AS road_name,
        MAX(r.road_type)                                                        AS road_type,
        MAX(r.area_id)                                                          AS area_id,
        MAX(a.area_name)                                                        AS area_name,
        FIRST_VALUE(jh.peak_period) OVER (PARTITION BY r.road_id
            ORDER BY jh.avg_congestion_rate DESC)                               AS peak_period,
        ROUND(AVG(jh.jam_level), 2)                                             AS avg_jam_level,
        MAX(jh.jam_level)                                                       AS max_jam_level,
        ROUND(AVG(jh.avg_congestion_rate), 2)                                   AS avg_congestion_rate,
        SUM(jh.total_traffic_flow)                                              AS total_traffic_flow,
        FIRST_VALUE(jh.hour) OVER (PARTITION BY r.road_id
            ORDER BY jh.avg_congestion_rate DESC)                               AS peak_time,
        jh.dt
    FROM traffic_db.dws_area_jam_hour jh
    JOIN traffic_db.dim_road_zip r
        ON jh.area_id = r.area_id
       AND r.is_current = 'Y'
       AND r.dt = '${date}'
    JOIN traffic_db.dim_area a
        ON r.area_id = a.area_id
    WHERE jh.dt = '${date}'
    GROUP BY r.road_id, jh.dt
    ORDER BY avg_jam_level DESC, avg_congestion_rate DESC
    LIMIT 20
) t;

ALTER TABLE traffic_db.ads_top_jam_roads ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.ads_top_jam_roads;
