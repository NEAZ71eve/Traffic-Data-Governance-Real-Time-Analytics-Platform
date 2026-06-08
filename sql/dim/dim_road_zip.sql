CREATE TABLE IF NOT EXISTS dim_road_zip (
    road_id STRING COMMENT '道路ID',
    road_name STRING COMMENT '道路名称',
    area STRING COMMENT '所属区域',
    road_level STRING COMMENT '道路等级(高速/快速/主干道/次干道/支路)',
    length DECIMAL(8,2) COMMENT '道路长度(km)',
    lanes INT COMMENT '车道数',
    direction_type STRING COMMENT '方向类型(单向/双向)',
    start_node STRING COMMENT '起点节点',
    end_node STRING COMMENT '终点节点',
    status STRING COMMENT '道路状态(正常/施工/封闭)',
    start_date STRING COMMENT '生效开始日期',
    end_date STRING COMMENT '生效结束日期',
    is_current STRING COMMENT '是否当前版本(Y/N)'
) COMMENT '道路维度拉链表'
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transactional' = 'true'
);