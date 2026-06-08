CREATE TABLE IF NOT EXISTS dim_device_zip (
    device_id STRING COMMENT '设备ID',
    device_type STRING COMMENT '设备类型(摄像头/地磁/红绿灯/电子警察)',
    area STRING COMMENT '所属区域',
    road_id STRING COMMENT '所属道路ID',
    install_location STRING COMMENT '安装位置',
    install_date STRING COMMENT '安装日期',
    manufacturer STRING COMMENT '设备厂商',
    model STRING COMMENT '设备型号',
    firmware_version STRING COMMENT '固件版本',
    status STRING COMMENT '设备状态(正常/维修/报废)',
    start_date STRING COMMENT '生效开始日期',
    end_date STRING COMMENT '生效结束日期',
    is_current STRING COMMENT '是否当前版本(Y/N)'
) COMMENT '设备维度拉链表'
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transactional' = 'true'
);