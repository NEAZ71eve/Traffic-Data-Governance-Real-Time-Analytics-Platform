-- ============================================
-- DIM层：设备维度拉链表（SCD2缓慢变化维）
-- 变更策略：设备信息变更时闭合旧记录、新增当前记录
-- 分区字段：dt（日期分区 yyyy-MM-dd）
-- 使用方式：WHERE is_current = 'Y' 获取当前有效记录
-- ============================================

CREATE TABLE IF NOT EXISTS traffic_db.dim_device_zip (
    device_id       STRING      COMMENT '设备ID(主键)',
    device_name     STRING      COMMENT '设备名称',
    device_type     STRING      COMMENT '设备类型(CAMERA/SENSOR/RADAR/GATE)',
    device_model    STRING      COMMENT '设备型号',
    road_id         STRING      COMMENT '所属道路ID',
    install_date    STRING      COMMENT '安装日期(yyyy-MM-dd)',
    manufacturer    STRING      COMMENT '生产厂商',
    firmware_ver    STRING      COMMENT '固件版本',
    status          STRING      COMMENT '设备状态(RUNNING/MAINTENANCE/DECOMMISSIONED)',
    start_time      STRING      COMMENT '拉链生效时间(yyyy-MM-dd)',
    end_time        STRING      COMMENT '拉链失效时间(yyyy-MM-dd)，9999-12-31表示当前有效',
    is_current      STRING      COMMENT '是否为当前记录(Y/N)'
)
COMMENT '设备维度拉链表(SCD2)'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/traffic_db.db/dim_device_zip'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'device_id'
);

-- ============================================
-- 拉链表初始化（首次全量）
-- ============================================
-- INSERT OVERWRITE TABLE traffic_db.dim_device_zip PARTITION (dt = '${date}')
-- SELECT
--     device_id, device_name, device_type, device_model, road_id,
--     install_date, manufacturer, firmware_ver, status,
--     '1970-01-01' AS start_time, '9999-12-31' AS end_time, 'Y' AS is_current
-- FROM traffic_db.ods_device_info;

-- ============================================
-- 拉链表增量更新逻辑同 dim_road_zip
-- 对比字段：device_name, device_type, firmware_ver, status
-- ============================================

ALTER TABLE traffic_db.dim_device_zip ADD IF NOT EXISTS PARTITION (dt = '${date}');
MSCK REPAIR TABLE traffic_db.dim_device_zip;
