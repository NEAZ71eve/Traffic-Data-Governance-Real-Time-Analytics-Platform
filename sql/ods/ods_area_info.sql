-- ============================================
-- ODS层：区域基础信息表（DataX全量同步）
-- 数据来源：MySQL traffic_business.t_area → DataX → HDFS
-- 供 dim_area 维度表初始化使用
-- ============================================

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.ods_area_info (
    area_id         STRING      COMMENT '区域ID(主键)',
    area_name       STRING      COMMENT '区域名称',
    area_code       STRING      COMMENT '行政区划代码',
    city_id         STRING      COMMENT '所属城市ID',
    city_name       STRING      COMMENT '所属城市名称',
    province_id     STRING      COMMENT '所属省份ID',
    province_name   STRING      COMMENT '所属省份名称',
    area_level      STRING      COMMENT '区域级别(DISTRICT/COUNTY)',
    area_type       STRING      COMMENT '区域类型(URBAN/SUBURBAN/RURAL)',
    updated_at      STRING      COMMENT '更新时间'
)
COMMENT '区域基础信息ODS表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_area_info';
