SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dws_road_hour_flow PARTITION (dt)
SELECT
    road_id,
    hour,
    COUNT(*) AS traffic_count,
    ROUND(AVG(speed), 2) AS avg_speed,
    COUNT(DISTINCT vehicle_id) AS unique_vehicle_count,
    SUM(CASE WHEN direction = 'UP' THEN 1 ELSE 0 END) AS up_count,
    SUM(CASE WHEN direction = 'DOWN' THEN 1 ELSE 0 END) AS down_count,
    dt
FROM dwd_vehicle_pass_di
WHERE dt = '${date}'
GROUP BY road_id, hour, dt;

ALTER TABLE dws_road_hour_flow ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE dws_road_hour_flow;