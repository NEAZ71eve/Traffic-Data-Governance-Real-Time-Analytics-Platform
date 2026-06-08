CREATE EXTERNAL TABLE IF NOT EXISTS ods_traffic_status_di (
    road_id STRING COMMENT '道路ID',
    avg_speed INT COMMENT '平均车速(km/h)',
    traffic_flow INT COMMENT '车流量',
    jam_level INT COMMENT '拥堵等级(1-5)',
    congestion_rate DECIMAL(5,2) COMMENT '拥堵率',
    peak_flag STRING COMMENT '高峰标识',
    sample_time STRING COMMENT '采样时间',
    dt STRING COMMENT '分区日期'
) COMMENT '路况监测原始数据表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_traffic_status_di'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai'
);