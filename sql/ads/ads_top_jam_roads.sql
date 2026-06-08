SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ads_top_jam_roads PARTITION (dt)
SELECT
    road_id,
    dr.road_name,
    dr.area,
    dr.road_level,
    ROUND(AVG(jam_level), 2) AS avg_jam_level,
    ROUND(AVG(congestion_rate), 2) AS avg_congestion_rate,
    ROUND(AVG(avg_speed), 2) AS avg_speed,
    SUM(traffic_flow) AS total_flow,
    dt,
    RANK() OVER(ORDER BY AVG(jam_level) DESC) AS jam_rank
FROM dwd_traffic_status_di ts
JOIN dim_road_zip dr ON ts.road_id = dr.road_id AND dr.is_current = 'Y'
WHERE ts.dt = '${date}'
GROUP BY road_id, dr.road_name, dr.area, dr.road_level, dt
ORDER BY avg_jam_level DESC
LIMIT 10;

ALTER TABLE ads_top_jam_roads ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE ads_top_jam_roads;