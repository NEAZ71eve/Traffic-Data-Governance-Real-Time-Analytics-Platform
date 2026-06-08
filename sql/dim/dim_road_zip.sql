-- ============================================
-- DIM层：道路维度拉链表（SCD2缓慢变化维）
-- 变更策略：道路属性变更时闭合旧记录、新增当前记录
-- 分区字段：dt（日期分区 yyyy-MM-dd）
-- 使用方式：WHERE is_current = 'Y' 获取当前有效记录
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dim_road_zip (
    road_id         STRING      COMMENT '道路ID(主键)',
    road_name       STRING      COMMENT '道路名称',
    road_type       STRING      COMMENT '道路类型(主干道/次干道/支路/快速路/高速)',
    road_length     DECIMAL(8,2) COMMENT '道路长度(公里)',
    lane_count      INT         COMMENT '车道数',
    speed_limit     INT         COMMENT '限速(km/h)',
    area_id         STRING      COMMENT '所属区域ID',
    direction       STRING      COMMENT '行驶方向(N/S/E/W/BIDIRECTIONAL)',
    start_time      STRING      COMMENT '拉链生效时间(yyyy-MM-dd)',
    end_time        STRING      COMMENT '拉链失效时间(yyyy-MM-dd)，9999-12-31表示当前有效',
    is_current      STRING      COMMENT '是否为当前记录(Y/N)'
)
COMMENT '道路维度拉链表(SCD2)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dim_road_zip'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'road_id'
);

-- ============================================
-- 拉链表初始化：首次全量加载（仅首次执行）
-- ============================================
-- INSERT OVERWRITE TABLE traffic_db.dim_road_zip PARTITION (dt = '${date}')
-- SELECT
--     road_id,
--     road_name,
--     road_type,
--     road_length,
--     lane_count,
--     speed_limit,
--     area_id,
--     direction,
--     '1970-01-01' AS start_time,
--     '9999-12-31' AS end_time,
--     'Y'           AS is_current
-- FROM traffic_db.ods_road_info;  -- 假设存在道路基础信息表

-- ============================================
-- 拉链表增量更新：每日对比变更
-- 步骤1：闭合已变更的旧记录（end_time设为昨天）
-- 步骤2：插入新记录（start_time设为今天，is_current='Y'）
-- ============================================
-- SET hive.exec.dynamic.partition = true;
-- SET hive.exec.dynamic.partition.mode = nonstrict;
-- 
-- INSERT OVERWRITE TABLE traffic_db.dim_road_zip PARTITION (dt)
-- SELECT
--     road_id, road_name, road_type, road_length, lane_count, speed_limit, area_id, direction,
--     start_time, end_time, is_current, '${date}' AS dt
-- FROM (
--     -- 历史未变更记录保持原样
--     SELECT road_id, road_name, road_type, road_length, lane_count, speed_limit, area_id, direction,
--            start_time, end_time, is_current
--     FROM traffic_db.dim_road_zip
--     WHERE dt = '${yesterday}' AND is_current = 'Y'
--       AND road_id NOT IN (
--           SELECT old.road_id
--           FROM traffic_db.dim_road_zip old
--           JOIN traffic_db.ods_road_info new ON old.road_id = new.road_id
--           WHERE old.dt = '${yesterday}' AND old.is_current = 'Y'
--             AND (old.road_name <> new.road_name OR old.road_type <> new.road_type
--               OR old.lane_count <> new.lane_count OR old.speed_limit <> new.speed_limit)
--       )
--     UNION ALL
--     -- 变更记录：闭合旧版本
--     SELECT old.road_id, old.road_name, old.road_type, old.road_length, old.lane_count,
--            old.speed_limit, old.area_id, old.direction,
--            old.start_time, '${yesterday}' AS end_time, 'N' AS is_current
--     FROM traffic_db.dim_road_zip old
--     JOIN traffic_db.ods_road_info new ON old.road_id = new.road_id
--     WHERE old.dt = '${yesterday}' AND old.is_current = 'Y'
--       AND (old.road_name <> new.road_name OR old.road_type <> new.road_type
--         OR old.lane_count <> new.lane_count OR old.speed_limit <> new.speed_limit)
--     UNION ALL
--     -- 变更记录：新增当前版本
--     SELECT new.road_id, new.road_name, new.road_type, new.road_length, new.lane_count,
--            new.speed_limit, new.area_id, new.direction,
--            '${date}' AS start_time, '9999-12-31' AS end_time, 'Y' AS is_current
--     FROM traffic_db.ods_road_info new
--     JOIN traffic_db.dim_road_zip old ON old.road_id = new.road_id
--     WHERE old.dt = '${yesterday}' AND old.is_current = 'Y'
--       AND (old.road_name <> new.road_name OR old.road_type <> new.road_type
--         OR old.lane_count <> new.lane_count OR old.speed_limit <> new.speed_limit)
--     UNION ALL
--     -- 新增的道路
--     SELECT road_id, road_name, road_type, road_length, lane_count, speed_limit, area_id, direction,
--            '${date}' AS start_time, '9999-12-31' AS end_time, 'Y' AS is_current
--     FROM traffic_db.ods_road_info new
--     WHERE new.road_id NOT IN (
--         SELECT road_id FROM traffic_db.dim_road_zip WHERE dt = '${yesterday}'
--     )
-- ) t;

ALTER TABLE traffic_db.dim_road_zip ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.dim_road_zip;
