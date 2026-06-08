-- ============================================
-- ODS层：路况监测数据表（增量日分区）
-- 数据来源：Kafka traffic_status -> Flume -> HDFS -> Hive外部表
-- 存储格式：TEXTFILE（保留原始格式）
-- 分区字段：dt（日期分区 yyyy-MM-dd）
-- ============================================

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.ods_traffic_status_di (
    road_id         STRING      COMMENT '道路ID',
    avg_speed       INT         COMMENT '平均车速(km/h)',
    traffic_flow    INT         COMMENT '车流量(辆)',
    jam_level       INT         COMMENT '拥堵等级(1-畅通/2-基本畅通/3-轻度拥堵/4-中度拥堵/5-严重拥堵)',
    congestion_rate DECIMAL(5,2) COMMENT '拥堵率(%)',
    peak_flag       STRING      COMMENT '高峰标识(PEAK_HOUR/NORMAL/OFF_PEAK)',
    sample_time     STRING      COMMENT '采样时间(yyyy-MM-dd HH:mm:ss)'
)
COMMENT '路况监测ODS原始记录表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_traffic_status_di'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai',
    'skip.header.line.count' = '0'
);
