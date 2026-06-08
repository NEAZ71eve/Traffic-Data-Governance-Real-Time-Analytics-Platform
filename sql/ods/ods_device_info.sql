-- ============================================
-- ODS层：设备基础信息表（DataX全量/增量同步）
-- 数据来源：MySQL traffic_business.t_device → DataX → HDFS
-- 供 dim_device_zip 拉链表初始化使用
-- ============================================

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.ods_device_info (
    device_id       STRING      COMMENT '设备ID(主键)',
    device_name     STRING      COMMENT '设备名称',
    device_type     STRING      COMMENT '设备类型(CAMERA/SENSOR/RADAR/GATE/TRAFFIC_LIGHT)',
    device_model    STRING      COMMENT '设备型号',
    road_id         STRING      COMMENT '所属道路ID',
    area_id         STRING      COMMENT '所属区域ID',
    install_date    STRING      COMMENT '安装日期',
    manufacturer    STRING      COMMENT '生产厂商',
    firmware_ver    STRING      COMMENT '固件版本',
    ip_address      STRING      COMMENT 'IP地址',
    status          STRING      COMMENT '设备状态(RUNNING/MAINTENANCE/DECOMMISSIONED/OFFLINE)',
    updated_at      STRING      COMMENT '更新时间'
)
COMMENT '设备基础信息ODS表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/ods_device_info';
