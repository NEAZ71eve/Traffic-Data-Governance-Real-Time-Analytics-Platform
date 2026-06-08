CREATE TABLE IF NOT EXISTS dim_area (
    area_id STRING COMMENT '区域ID',
    area_name STRING COMMENT '区域名称',
    parent_area_id STRING COMMENT '上级区域ID',
    area_level INT COMMENT '区域层级(1-省/2-市/3-区/4-街道)',
    city_id STRING COMMENT '城市ID',
    city_name STRING COMMENT '城市名称',
    longitude DECIMAL(10,6) COMMENT '经度',
    latitude DECIMAL(10,6) COMMENT '纬度',
    area_code STRING COMMENT '行政区划代码'
) COMMENT '区域维度表'
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);