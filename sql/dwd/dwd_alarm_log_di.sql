SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_alarm_log_di PARTITION (dt)
SELECT
    alarm_id,
    device_id,
    alarm_type,
    alarm_level,
    alarm_time,
    recover_time,
    alarm_desc,
    status,
    dt
FROM (
    SELECT
        alarm_id,
        device_id,
        alarm_type,
        alarm_level,
        alarm_time,
        recover_time,
        alarm_desc,
        status,
        dt,
        ROW_NUMBER() OVER(PARTITION BY device_id, alarm_type, alarm_time ORDER BY alarm_time) AS rn
    FROM ods_alarm_log_di
    WHERE dt = '${date}'
      AND alarm_id IS NOT NULL
      AND device_id IS NOT NULL
      AND alarm_time IS NOT NULL
      AND alarm_level IN ('CRITICAL', 'WARNING', 'INFO')
) t
WHERE rn = 1;

ALTER TABLE dwd_alarm_log_di ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE dwd_alarm_log_di;