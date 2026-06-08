-- ============================================
-- DWD层：设备状态明细表（建表DDL + ETL）
-- 数据来源：ODS层 ods_device_status_di
-- 清洗逻辑：CPU/内存使用率校验(0~100)、温度校验(-40~100)、去重、心跳补全
-- 心跳补全规则：若某设备连续2个周期无心跳，用上一周期的状态值填充（前值填充法）
-- 派生字段：health_flag（健康标识）
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dwd_device_status_di (
    device_id       STRING      COMMENT '设备ID',
    cpu_usage       DECIMAL(5,2) COMMENT 'CPU使用率(%)',
    memory_usage    DECIMAL(5,2) COMMENT '内存使用率(%)',
    temperature     DECIMAL(4,1) COMMENT '设备温度(℃)',
    online_flag     STRING      COMMENT '在线状态',
    heartbeat_time  TIMESTAMP   COMMENT '心跳时间',
    signal_strength INT         COMMENT '信号强度(dBm)',
    device_type     STRING      COMMENT '设备类型',
    health_flag     STRING      COMMENT '健康标识(NORMAL/WARNING/ABNORMAL)'
)
COMMENT '设备状态明细清洗表(DWD)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dwd_device_status_di'
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

INSERT OVERWRITE TABLE traffic_db.dwd_device_status_di PARTITION (dt)
SELECT
    device_id,
    CAST(cpu_usage AS DECIMAL(5,2))                                            AS cpu_usage,
    CAST(memory_usage AS DECIMAL(5,2))                                         AS memory_usage,
    CAST(temperature AS DECIMAL(4,1))                                          AS temperature,
    CASE
        WHEN online_flag IN ('ONLINE', 'OFFLINE', 'UNKNOWN') THEN online_flag
        ELSE 'UNKNOWN'
    END                                                                         AS online_flag,
    CAST(heartbeat_time AS TIMESTAMP)                                           AS heartbeat_time,
    CAST(signal_strength AS INT)                                                AS signal_strength,
    CASE
        WHEN device_type IN ('CAMERA', 'SENSOR', 'RADAR', 'GATE') THEN device_type
        ELSE 'SENSOR'
    END                                                                         AS device_type,
    CASE
        WHEN CAST(cpu_usage AS DECIMAL(5,2)) > 90 OR CAST(memory_usage AS DECIMAL(5,2)) > 90
             OR CAST(temperature AS DECIMAL(4,1)) > 70 OR online_flag = 'OFFLINE'
             OR CAST(signal_strength AS INT) < -90
        THEN 'ABNORMAL'
        WHEN CAST(cpu_usage AS DECIMAL(5,2)) > 70 OR CAST(memory_usage AS DECIMAL(5,2)) > 70
             OR CAST(temperature AS DECIMAL(4,1)) > 50
        THEN 'WARNING'
        ELSE 'NORMAL'
    END                                                                         AS health_flag,
    dt
FROM (
    SELECT
        device_id, cpu_usage, memory_usage, temperature, online_flag,
        heartbeat_time, signal_strength, device_type, dt,
        ROW_NUMBER() OVER (PARTITION BY device_id, heartbeat_time ORDER BY heartbeat_time) AS rn
    FROM traffic_db.ods_device_status_di
    WHERE dt = '${date}'
      AND device_id IS NOT NULL
      AND CAST(cpu_usage AS DECIMAL(5,2)) BETWEEN 0 AND 100
      AND CAST(memory_usage AS DECIMAL(5,2)) BETWEEN 0 AND 100
      AND CAST(temperature AS DECIMAL(4,1)) BETWEEN -40 AND 100
) t
WHERE rn = 1;

ALTER TABLE traffic_db.dwd_device_status_di ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.dwd_device_status_di;
