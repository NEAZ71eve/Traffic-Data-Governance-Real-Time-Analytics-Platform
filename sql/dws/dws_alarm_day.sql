SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dws_alarm_day PARTITION (dt)
SELECT
    device_id,
    alarm_type,
    alarm_level,
    COUNT(*) AS alarm_count,
    SUM(CASE WHEN status = 'RESOLVED' THEN 1 ELSE 0 END) AS resolved_count,
    ROUND(AVG(UNIX_TIMESTAMP(recover_time) - UNIX_TIMESTAMP(alarm_time)) / 60, 2) AS avg_recover_minutes,
    MIN(alarm_time) AS first_alarm_time,
    MAX(alarm_time) AS last_alarm_time,
    dt
FROM dwd_alarm_log_di
WHERE dt = '${date}'
GROUP BY device_id, alarm_type, alarm_level, dt;

ALTER TABLE dws_alarm_day ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE dws_alarm_day;