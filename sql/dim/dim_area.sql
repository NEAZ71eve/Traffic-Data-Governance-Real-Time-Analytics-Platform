-- ============================================
-- DIM层：区域维度表（静态维）
-- 用途：关联道路到行政区划，支持按区域聚合分析
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dim_area (
    area_id         STRING  COMMENT '区域ID(主键)',
    area_name       STRING  COMMENT '区域名称(如：朝阳区)',
    area_code       STRING  COMMENT '行政区划代码',
    city_id         STRING  COMMENT '所属城市ID',
    city_name       STRING  COMMENT '所属城市名称',
    province_id     STRING  COMMENT '所属省份ID',
    province_name   STRING  COMMENT '所属省份名称',
    area_level      STRING  COMMENT '区域级别(DISTRICT/COUNTY)',
    area_type       STRING  COMMENT '区域类型(URBAN/SUBURBAN/RURAL)'
)
COMMENT '区域维度静态表'
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dim_area'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'area_id,city_id'
);

-- ============================================
-- 区域数据初始化（示例）
-- ============================================
-- INSERT INTO traffic_db.dim_area VALUES
-- ('A001', '朝阳区', '110105', 'BJ', '北京市', '11', '北京市', 'DISTRICT', 'URBAN'),
-- ('A002', '海淀区', '110108', 'BJ', '北京市', '11', '北京市', 'DISTRICT', 'URBAN'),
-- ('A003', '浦东新区', '310115', 'SH', '上海市', '31', '上海市', 'DISTRICT', 'URBAN');
