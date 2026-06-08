SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ads_device_health_score PARTITION (dt)
SELECT
    dhd.device_id,
    dd.device_type,
    dd.area,
    dhd.avg_cpu_usage,
    dhd.avg_memory_usage,
    dhd.avg_temperature,
    ROUND(dhd.online_count / dhd.total_records * 100, 2) AS online_rate,
    dhd.offline_count,
    ROUND(
        0.4 * (dhd.online_count / dhd.total_records) +
        0.3 * (1 - COALESCE(ad.alarm_count, 0) / dhd.total_records) +
        0.3 * (1 - (dhd.avg_cpu_usage + dhd.avg_memory_usage) / 200)
    , 2) AS health_score,
    dt
FROM dws_device_health_day dhd
LEFT JOIN (
    SELECT device_id, SUM(alarm_count) AS alarm_count 
    FROM dws_alarm_day 
    WHERE dt = '${date}' 
    GROUP BY device_id
) ad ON dhd.device_id = ad.device_id
JOIN dim_device_zip dd ON dhd.device_id = dd.device_id AND dd.is_current = 'Y'
WHERE dhd.dt = '${date}';

ALTER TABLE ads_device_health_score ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE ads_device_health_score;