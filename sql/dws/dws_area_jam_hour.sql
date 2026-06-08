SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dws_area_jam_hour PARTITION (dt)
SELECT
    dr.area,
    dt.tm_hour,
    ROUND(AVG(ts.jam_level), 2) AS avg_jam_level,
    SUM(ts.traffic_flow) AS total_flow,
    ROUND(AVG(ts.congestion_rate), 2) AS avg_congestion_rate,
    ROUND(AVG(ts.avg_speed), 2) AS avg_speed,
    COUNT(DISTINCT ts.road_id) AS road_count,
    ts.dt
FROM dwd_traffic_status_di ts
JOIN dim_road_zip dr ON ts.road_id = dr.road_id AND dr.is_current = 'Y'
JOIN dim_time dt ON SUBSTR(ts.sample_time, 1, 14) = dt.time_id
WHERE ts.dt = '${date}'
GROUP BY dr.area, dt.tm_hour, ts.dt;

ALTER TABLE dws_area_jam_hour ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE dws_area_jam_hour;