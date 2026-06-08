-- ============================================
-- DWS层：设备健康日汇总表（建表DDL + ETL）
-- 数据来源：DWD层 dwd_device_status_di
-- 聚合粒度：device_id + day
-- 指标：在线时长、平均CPU/内存/温度、异常次数、最后心跳
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dws_device_health_day (
    device_id         STRING      COMMENT '设备ID',
    online_duration   BIGINT      COMMENT '在线时长(分钟)',
    offline_count     BIGINT      COMMENT '离线次数',
    avg_cpu_usage     DECIMAL(5,2) COMMENT '平均CPU使用率(%)',
    max_cpu_usage     DECIMAL(5,2) COMMENT '最大CPU使用率(%)',
    avg_memory_usage  DECIMAL(5,2) COMMENT '平均内存使用率(%)',
    max_memory_usage  DECIMAL(5,2) COMMENT '最大内存使用率(%)',
    avg_temperature   DECIMAL(4,1) COMMENT '平均温度(℃)',
    max_temperature   DECIMAL(4,1) COMMENT '最高温度(℃)',
    abnormal_count    BIGINT      COMMENT '异常状态次数',
    warning_count     BIGINT      COMMENT '预警状态次数',
    normal_count      BIGINT      COMMENT '正常状态次数',
    last_heartbeat    STRING      COMMENT '最后心跳时间',
    min_signal        INT         COMMENT '最低信号强度(dBm)',
    avg_signal        DECIMAL(5,2) COMMENT '平均信号强度(dBm)'
)
COMMENT '设备健康日汇总表(DWS)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dws_device_health_day'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'device_id'
);

-- ============================================
-- 每日ETL
-- ============================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE traffic_db.dws_device_health_day PARTITION (dt)
SELECT
    device_id,
    SUM(CASE WHEN online_flag = 'ONLINE' THEN 1 ELSE 0 END)                    AS online_duration,
    SUM(CASE WHEN online_flag = 'OFFLINE' THEN 1 ELSE 0 END)                   AS offline_count,
    ROUND(AVG(cpu_usage), 2)                                                    AS avg_cpu_usage,
    ROUND(MAX(cpu_usage), 2)                                                    AS max_cpu_usage,
    ROUND(AVG(memory_usage), 2)                                                 AS avg_memory_usage,
    ROUND(MAX(memory_usage), 2)                                                 AS max_memory_usage,
    ROUND(AVG(temperature), 1)                                                  AS avg_temperature,
    ROUND(MAX(temperature), 1)                                                  AS max_temperature,
    SUM(CASE WHEN health_flag = 'ABNORMAL' THEN 1 ELSE 0 END)                  AS abnormal_count,
    SUM(CASE WHEN health_flag = 'WARNING'  THEN 1 ELSE 0 END)                  AS warning_count,
    SUM(CASE WHEN health_flag = 'NORMAL'   THEN 1 ELSE 0 END)                  AS normal_count,
    MAX(CAST(heartbeat_time AS STRING))                                         AS last_heartbeat,
    MIN(signal_strength)                                                        AS min_signal,
    ROUND(AVG(signal_strength), 2)                                              AS avg_signal,
    dt
FROM traffic_db.dwd_device_status_di
WHERE dt = '${date}'
  AND device_id IS NOT NULL
GROUP BY device_id, dt;

ALTER TABLE traffic_db.dws_device_health_day ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.dws_device_health_day;
