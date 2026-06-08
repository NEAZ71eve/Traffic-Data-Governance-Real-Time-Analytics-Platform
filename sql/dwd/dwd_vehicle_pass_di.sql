SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_vehicle_pass_di PARTITION (dt)
SELECT
    vehicle_id,
    road_id,
    device_id,
    pass_time,
    CASE WHEN speed < 0 OR speed > 200 THEN NULL ELSE speed END AS speed,
    direction,
    plate_number,
    vehicle_type,
    lane,
    dt
FROM (
    SELECT
        vehicle_id,
        road_id,
        device_id,
        pass_time,
        speed,
        direction,
        plate_number,
        vehicle_type,
        lane,
        dt,
        ROW_NUMBER() OVER(PARTITION BY vehicle_id, pass_time ORDER BY pass_time) AS rn
    FROM ods_vehicle_pass_di
    WHERE dt = '${date}'
      AND vehicle_id IS NOT NULL
      AND road_id IS NOT NULL
      AND device_id IS NOT NULL
      AND pass_time IS NOT NULL
) t
WHERE rn = 1;

ALTER TABLE dwd_vehicle_pass_di ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE dwd_vehicle_pass_di;