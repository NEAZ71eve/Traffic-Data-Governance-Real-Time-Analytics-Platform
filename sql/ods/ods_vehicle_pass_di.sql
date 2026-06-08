CREATE EXTERNAL TABLE IF NOT EXISTS ods_vehicle_pass_di (
    vehicle_id STRING COMMENT '车辆ID',
    road_id STRING COMMENT '道路ID',
    device_id STRING COMMENT '设备ID',
    pass_time STRING COMMENT '通行时间',
    speed INT COMMENT '车速(km/h)',
    direction STRING COMMENT '行驶方向',
    plate_number STRING COMMENT '车牌号',
    vehicle_type STRING COMMENT '车辆类型',
    lane INT COMMENT '车道号',
    dt STRING COMMENT '分区日期'
) COMMENT '车辆通行原始数据表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_vehicle_pass_di'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai'
);