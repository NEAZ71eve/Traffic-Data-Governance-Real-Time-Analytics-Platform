-- ============================================
-- ODS层：道路基础信息表（DataX全量/增量同步）
-- 数据来源：MySQL traffic_business.t_road → DataX → HDFS
-- 供 dim_road_zip 拉链表初始化使用
-- ============================================

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.ods_road_info (
    road_id         STRING      COMMENT '道路ID(主键)',
    road_name       STRING      COMMENT '道路名称',
    road_type       STRING      COMMENT '道路类型(主干道/次干道/支路/快速路/高速)',
    road_length     DECIMAL(8,2) COMMENT '道路长度(公里)',
    lane_count      INT         COMMENT '车道数',
    speed_limit     INT         COMMENT '限速(km/h)',
    area_id         STRING      COMMENT '所属区域ID',
    direction       STRING      COMMENT '行驶方向(N/S/E/W/BIDIRECTIONAL)',
    status          STRING      COMMENT '状态(ACTIVE/CLOSED/MAINTENANCE)',
    updated_at      STRING      COMMENT '更新时间'
)
COMMENT '道路基础信息ODS表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_road_info';
