-- ============================================
-- DIM层：时间维度表（静态维，一次性生成）
-- 用途：为各层级提供时间属性关联（小时/星期/季度/是否高峰等）
-- 数据范围：2020-01-01 ~ 2030-12-31（可扩展）
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dim_time (
    date_key        STRING  COMMENT '日期主键(yyyy-MM-dd)',
    year            INT     COMMENT '年',
    quarter         INT     COMMENT '季度(1-4)',
    month           INT     COMMENT '月(1-12)',
    day_of_month    INT     COMMENT '日(1-31)',
    week_of_year    INT     COMMENT '年中第几周(1-53)',
    day_of_week     INT     COMMENT '周几(1=周一,7=周日)',
    day_name        STRING  COMMENT '星期名称(星期一/星期二...)',
    is_weekend      STRING  COMMENT '是否周末(Y/N)',
    is_holiday      STRING  COMMENT '是否节假日(Y/N)',
    holiday_name    STRING  COMMENT '节假日名称',
    hour            INT     COMMENT '小时(0-23)',
    peak_period     STRING  COMMENT '时段(MORNING_PEEK/EVE_PEEK/DAY_NORMAL/NIGHT_NORMAL)',
    quarter_name    STRING  COMMENT '季度名称(Q1/Q2/Q3/Q4)'
)
COMMENT '时间维度静态表'
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dim_time'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'date_key,peak_period'
);

-- ============================================
-- 时间维度生成脚本（一次性执行）
-- ============================================
-- SET hivevar:start_date = '2020-01-01';
-- SET hivevar:end_date   = '2030-12-31';
-- 
-- INSERT OVERWRITE TABLE traffic_db.dim_time
-- SELECT
--     dt                                                                          AS date_key,
--     YEAR(dt)                                                                     AS year,
--     QUARTER(dt)                                                                  AS quarter,
--     MONTH(dt)                                                                    AS month,
--     DAY(dt)                                                                      AS day_of_month,
--     WEEKOFYEAR(dt)                                                               AS week_of_year,
--     CASE WHEN DAYOFWEEK(dt) = 1 THEN 7 ELSE DAYOFWEEK(dt) - 1 END              AS day_of_week,
--     FROM_UNIXTIME(UNIX_TIMESTAMP(dt, 'yyyy-MM-dd'), 'EEEE')                     AS day_name,
--     CASE WHEN DAYOFWEEK(dt) IN (1, 7) THEN 'Y' ELSE 'N' END                    AS is_weekend,
--     'N'                                                                          AS is_holiday,
--     NULL                                                                         AS holiday_name,
--     h                                                                           AS hour,
--     CASE
--         WHEN h BETWEEN 7 AND 9   THEN 'MORNING_PEAK'
--         WHEN h BETWEEN 17 AND 19 THEN 'EVENING_PEAK'
--         WHEN h BETWEEN 6 AND 22  THEN 'DAY_NORMAL'
--         ELSE 'NIGHT_NORMAL'
--     END                                                                          AS peak_period,
--     CONCAT('Q', QUARTER(dt))                                                    AS quarter_name
-- FROM (
--     SELECT DATE_ADD('${start_date}', pos) AS dt
--     FROM (SELECT posexplode(split(space(DATEDIFF('${end_date}', '${start_date}')), ''))) t
-- ) dates
-- CROSS JOIN (SELECT 0 AS h UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
--              UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
--              UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
--              UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
--              UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19
--              UNION ALL SELECT 20 UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23) hours;
