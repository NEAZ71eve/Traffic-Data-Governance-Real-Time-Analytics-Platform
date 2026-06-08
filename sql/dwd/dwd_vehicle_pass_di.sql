-- ============================================
-- DWD层：车辆通行明细表（建表DDL + ETL）
-- 数据来源：ODS层 ods_vehicle_pass_di
-- 清洗逻辑：数据类型转换、异常值过滤(车速0~200)、去重
-- 派生字段：hour、speed_km_per_s
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dwd_vehicle_pass_di (
    vehicle_id      STRING          COMMENT '车辆ID',
    road_id         STRING          COMMENT '道路ID',
    device_id       STRING          COMMENT '采集设备ID',
    pass_time       TIMESTAMP       COMMENT '通行时间',
    speed           INT             COMMENT '车速(km/h)',
    direction       STRING          COMMENT '行驶方向',
    plate_number    STRING          COMMENT '车牌号',
    vehicle_type    STRING          COMMENT '车辆类型',
    lane            INT             COMMENT '车道号',
    hour            INT             COMMENT '通行小时(0-23)',
    speed_km_per_s  DECIMAL(10,4)   COMMENT '速度(km/s)'
)
COMMENT '车辆通行明细清洗表(DWD)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dwd_vehicle_pass_di'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'vehicle_id,road_id'
);

-- ============================================
-- 每日ETL
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = true;
SET hive.exec.orc.default.block.size = 268435456;

INSERT OVERWRITE TABLE traffic_db.dwd_vehicle_pass_di PARTITION (dt)
SELECT
    vehicle_id,
    road_id,
    device_id,
    CAST(pass_time AS TIMESTAMP)                                                AS pass_time,
    CAST(speed AS INT)                                                          AS speed,
    COALESCE(direction, 'UNKNOWN')                                              AS direction,
    plate_number,
    CASE
        WHEN vehicle_type IN ('小型车', '中型车', '大型车') THEN vehicle_type
        ELSE '其他'
    END                                                                         AS vehicle_type,
    CAST(lane AS INT)                                                           AS lane,
    HOUR(pass_time)                                                             AS hour,
    CAST(ROUND(speed / 1000.0, 4)                                              AS DECIMAL(10,4)) AS speed_km_per_s,
    dt
FROM (
    SELECT
        vehicle_id,
        road_id,
        device_id,
        pass_time,
        speed,
        direction,
        plate_number,
        vehicle_type,
        lane,
        dt,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id, pass_time ORDER BY pass_time) AS rn
    FROM traffic_db.ods_vehicle_pass_di
    WHERE dt = '${date}'
      AND vehicle_id IS NOT NULL
      AND road_id IS NOT NULL
      AND speed >= 0 AND speed <= 200
) t
WHERE rn = 1;

ALTER TABLE traffic_db.dwd_vehicle_pass_di ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.dwd_vehicle_pass_di;
