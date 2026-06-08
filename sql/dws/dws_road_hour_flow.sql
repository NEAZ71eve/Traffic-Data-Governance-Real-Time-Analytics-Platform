-- ============================================
-- DWS层：道路小时流量汇总表（建表DDL + ETL）
-- 数据来源：DWD层 dwd_vehicle_pass_di
-- 聚合粒度：road_id + hour
-- 指标：车流量、平均车速、各类型车辆数
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dws_road_hour_flow (
    road_id        STRING  COMMENT '道路ID',
    hour           INT     COMMENT '小时(0-23)',
    traffic_count  BIGINT  COMMENT '车流量',
    total_speed    BIGINT  COMMENT '总车速(用于计算均值)',
    avg_speed      DECIMAL(8,2) COMMENT '平均车速(km/h)',
    small_car_cnt  BIGINT  COMMENT '小型车数量',
    medium_car_cnt BIGINT  COMMENT '中型车数量',
    large_car_cnt  BIGINT  COMMENT '大型车数量',
    other_car_cnt  BIGINT  COMMENT '其他车辆数量'
)
COMMENT '道路小时流量汇总表(DWS)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dws_road_hour_flow'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'road_id'
);

-- ============================================
-- 每日ETL：从DWD聚合到DWS
-- 处理数据倾斜：热点道路使用随机前缀+MOD打散后两阶段聚合
-- 阶段1: 加 0~9 随机前缀局部聚合 → 打散热点key
-- 阶段2: 去掉前缀再次聚合 → 最终结果
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = true;
SET hive.optimize.skewjoin = true;
SET hive.skewjoin.key = 100000;
SET mapred.reduce.tasks = 16;

INSERT OVERWRITE TABLE traffic_db.dws_road_hour_flow PARTITION (dt)
SELECT
    road_id,
    hour,
    SUM(traffic_count)                                                          AS traffic_count,
    SUM(total_speed)                                                            AS total_speed,
    ROUND(SUM(total_speed) / NULLIF(SUM(traffic_count), 0), 2)                 AS avg_speed,
    SUM(small_car_cnt)                                                          AS small_car_cnt,
    SUM(medium_car_cnt)                                                         AS medium_car_cnt,
    SUM(large_car_cnt)                                                          AS large_car_cnt,
    SUM(other_car_cnt)                                                          AS other_car_cnt,
    dt
FROM (
    -- 阶段2: 去掉随机前缀，再次聚合（去掉前缀用SUBSTR从第3位开始）
    SELECT
        SUBSTR(skew_key, 3)                                                     AS road_id,
        hour,
        SUM(cnt)                                                                AS traffic_count,
        SUM(total_spd)                                                          AS total_speed,
        SUM(sc)                                                                 AS small_car_cnt,
        SUM(mc)                                                                 AS medium_car_cnt,
        SUM(lc)                                                                 AS large_car_cnt,
        SUM(oc)                                                                 AS other_car_cnt,
        dt
    FROM (
        -- 阶段1: 加0~9随机前缀打散，局部聚合
        SELECT
            CONCAT(CAST(FLOOR(RAND() * 10) AS STRING), '_', road_id)           AS skew_key,
            hour,
            COUNT(1)                                                            AS cnt,
            SUM(speed)                                                          AS total_spd,
            SUM(CASE WHEN vehicle_type = '小型车' THEN 1 ELSE 0 END)           AS sc,
            SUM(CASE WHEN vehicle_type = '中型车' THEN 1 ELSE 0 END)           AS mc,
            SUM(CASE WHEN vehicle_type = '大型车' THEN 1 ELSE 0 END)           AS lc,
            SUM(CASE WHEN vehicle_type NOT IN ('小型车','中型车','大型车') THEN 1 ELSE 0 END) AS oc,
            dt
        FROM traffic_db.dwd_vehicle_pass_di
        WHERE dt = '${date}'
        GROUP BY CONCAT(CAST(FLOOR(RAND() * 10) AS STRING), '_', road_id), hour, dt
    ) t1
    GROUP BY SUBSTR(skew_key, 3), hour, dt
) t2
GROUP BY road_id, hour, dt;

ALTER TABLE traffic_db.dws_road_hour_flow ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.dws_road_hour_flow;
