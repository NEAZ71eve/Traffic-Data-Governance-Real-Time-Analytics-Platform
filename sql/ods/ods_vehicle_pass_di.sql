-- ============================================
-- ODS层：车辆通行记录表（增量日分区）
-- 数据来源：Kafka traffic_vehicle -> Flume -> HDFS -> Hive外部表
-- 存储格式：TEXTFILE（保留原始格式）
-- 分区字段：dt（日期分区 yyyy-MM-dd）
-- ============================================

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.ods_vehicle_pass_di (
    vehicle_id    STRING  COMMENT '车辆ID',
    road_id       STRING  COMMENT '道路ID',
    device_id     STRING  COMMENT '采集设备ID',
    pass_time     STRING  COMMENT '通行时间(yyyy-MM-dd HH:mm:ss)',
    speed         INT     COMMENT '车速(km/h)',
    direction     STRING  COMMENT '行驶方向(N/S/E/W)',
    plate_number  STRING  COMMENT '车牌号',
    vehicle_type  STRING  COMMENT '车辆类型(小型车/中型车/大型车)',
    lane          INT     COMMENT '车道号(1-N)'
)
COMMENT '车辆通行ODS原始记录表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_vehicle_pass_di'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai',
    'skip.header.line.count' = '0'
);
