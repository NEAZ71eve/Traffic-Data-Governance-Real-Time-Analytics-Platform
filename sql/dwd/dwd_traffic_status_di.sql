-- ============================================
-- DWD层：路况监测明细表（建表DDL + ETL）
-- 数据来源：ODS层 ods_traffic_status_di
-- 清洗逻辑：拥堵等级校验(1~5)、拥堵率校验(0~100)、数据去重
-- 派生字段：jam_desc 拥堵描述
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dwd_traffic_status_di (
    road_id         STRING      COMMENT '道路ID',
    avg_speed       INT         COMMENT '平均车速(km/h)',
    traffic_flow    INT         COMMENT '车流量(辆)',
    jam_level       INT         COMMENT '拥堵等级(1-5)',
    congestion_rate DECIMAL(5,2) COMMENT '拥堵率(%)',
    peak_flag       STRING      COMMENT '高峰标识',
    sample_time     TIMESTAMP   COMMENT '采样时间',
    jam_desc        STRING      COMMENT '拥堵描述'
)
COMMENT '路况监测明细清洗表(DWD)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dwd_traffic_status_di'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'road_id'
);

-- ============================================
-- 每日ETL
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE traffic_db.dwd_traffic_status_di PARTITION (dt)
SELECT
    road_id,
    CAST(avg_speed AS INT)                                                      AS avg_speed,
    CAST(traffic_flow AS INT)                                                   AS traffic_flow,
    CAST(jam_level AS INT)                                                      AS jam_level,
    CAST(congestion_rate AS DECIMAL(5,2))                                       AS congestion_rate,
    CASE
        WHEN peak_flag IN ('PEEK_HOUR', 'NORMAL', 'OFF_PEEK') THEN peak_flag
        ELSE 'NORMAL'
    END                                                                         AS peak_flag,
    CAST(sample_time AS TIMESTAMP)                                              AS sample_time,
    CASE
        WHEN CAST(jam_level AS INT) = 1 THEN '畅通'
        WHEN CAST(jam_level AS INT) = 2 THEN '基本畅通'
        WHEN CAST(jam_level AS INT) = 3 THEN '轻度拥堵'
        WHEN CAST(jam_level AS INT) = 4 THEN '中度拥堵'
        WHEN CAST(jam_level AS INT) = 5 THEN '严重拥堵'
        ELSE '未知'
    END                                                                         AS jam_desc,
    dt
FROM (
    SELECT
        road_id, avg_speed, traffic_flow, jam_level, congestion_rate,
        peak_flag, sample_time, dt,
        ROW_NUMBER() OVER (PARTITION BY road_id, sample_time ORDER BY sample_time) AS rn
    FROM traffic_db.ods_traffic_status_di
    WHERE dt = '${date}'
      AND road_id IS NOT NULL
      AND CAST(jam_level AS INT) BETWEEN 1 AND 5
      AND CAST(congestion_rate AS DECIMAL(5,2)) BETWEEN 0 AND 100
) t
WHERE rn = 1;

ALTER TABLE traffic_db.dwd_traffic_status_di ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.dwd_traffic_status_di;
