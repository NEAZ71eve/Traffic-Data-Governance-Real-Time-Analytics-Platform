SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_device_status_di PARTITION (dt)
SELECT
    device_id,
    CASE WHEN cpu_usage < 0 THEN 0 WHEN cpu_usage > 100 THEN 100 ELSE cpu_usage END AS cpu_usage,
    CASE WHEN memory_usage < 0 THEN 0 WHEN memory_usage > 100 THEN 100 ELSE memory_usage END AS memory_usage,
    temperature,
    COALESCE(online_flag, 'UNKNOWN') AS online_flag,
    COALESCE(heartbeat_time, current_timestamp()) AS heartbeat_time,
    signal_strength,
    device_type,
    dt
FROM ods_device_status_di
WHERE dt = '${date}'
  AND device_id IS NOT NULL;

ALTER TABLE dwd_device_status_di ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE dwd_device_status_di;