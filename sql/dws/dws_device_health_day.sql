SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dws_device_health_day PARTITION (dt)
SELECT
    device_id,
    COUNT(*) AS total_records,
    SUM(CASE WHEN online_flag = 'ONLINE' THEN 1 ELSE 0 END) AS online_count,
    ROUND(AVG(cpu_usage), 2) AS avg_cpu_usage,
    ROUND(AVG(memory_usage), 2) AS avg_memory_usage,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    COUNT(DISTINCT CASE WHEN online_flag = 'OFFLINE' THEN heartbeat_time END) AS offline_count,
    dt
FROM dwd_device_status_di
WHERE dt = '${date}'
GROUP BY device_id, dt;

ALTER TABLE dws_device_health_day ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE dws_device_health_day;