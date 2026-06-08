SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ads_device_mtbf_mttr PARTITION (dt)
SELECT
    device_id,
    dd.device_type,
    COUNT(DISTINCT alarm_id) AS total_failures,
    ROUND(86400 / COUNT(DISTINCT alarm_id), 2) AS mtbf_seconds,
    ROUND(AVG(UNIX_TIMESTAMP(recover_time) - UNIX_TIMESTAMP(alarm_time)), 2) AS mttr_seconds,
    ROUND(AVG(UNIX_TIMESTAMP(recover_time) - UNIX_TIMESTAMP(alarm_time)) / 3600, 2) AS mttr_hours,
    dt
FROM dwd_alarm_log_di al
JOIN dim_device_zip dd ON al.device_id = dd.device_id AND dd.is_current = 'Y'
WHERE al.dt = '${date}'
  AND al.status = 'RESOLVED'
GROUP BY device_id, dd.device_type, dt;

ALTER TABLE ads_device_mtbf_mttr ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE ads_device_mtbf_mttr;