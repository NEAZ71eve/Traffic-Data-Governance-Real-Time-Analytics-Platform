SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ads_traffic_operation PARTITION (dt)
SELECT
    'CITY' AS level_type,
    NULL AS area_id,
    '全市' AS area_name,
    ROUND(AVG(jam.avg_jam_level), 2) AS city_jam_index,
    ROUND(AVG(rhf.avg_speed), 2) AS avg_speed,
    SUM(rhf.traffic_count) AS total_flow,
    dt
FROM dws_road_hour_flow rhf
JOIN dws_area_jam_hour jam ON rhf.dt = jam.dt
WHERE rhf.dt = '${date}'
GROUP BY dt

UNION ALL

SELECT
    'AREA' AS level_type,
    da.area_id,
    da.area_name,
    ROUND(AVG(jam.avg_jam_level), 2) AS city_jam_index,
    ROUND(AVG(rhf.avg_speed), 2) AS avg_speed,
    SUM(rhf.traffic_count) AS total_flow,
    rhf.dt
FROM dws_road_hour_flow rhf
JOIN dws_area_jam_hour jam ON rhf.dt = jam.dt AND rhf.road_id = jam.road_id
JOIN dim_area da ON jam.area = da.area_name
WHERE rhf.dt = '${date}'
GROUP BY da.area_id, da.area_name, rhf.dt;

ALTER TABLE ads_traffic_operation ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE ads_traffic_operation;