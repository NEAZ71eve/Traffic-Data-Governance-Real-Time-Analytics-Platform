SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_traffic_status_di PARTITION (dt)
SELECT
    road_id,
    CASE WHEN avg_speed < 0 THEN NULL ELSE avg_speed END AS avg_speed,
    CASE WHEN traffic_flow < 0 THEN 0 ELSE traffic_flow END AS traffic_flow,
    CASE 
        WHEN jam_level < 1 THEN 1 
        WHEN jam_level > 5 THEN 5 
        ELSE jam_level 
    END AS jam_level,
    CASE 
        WHEN congestion_rate < 0 THEN 0 
        WHEN congestion_rate > 100 THEN 100 
        ELSE congestion_rate 
    END AS congestion_rate,
    peak_flag,
    sample_time,
    dt
FROM ods_traffic_status_di
WHERE dt = '${date}'
  AND road_id IS NOT NULL
  AND sample_time IS NOT NULL;

ALTER TABLE dwd_traffic_status_di ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE dwd_traffic_status_di;