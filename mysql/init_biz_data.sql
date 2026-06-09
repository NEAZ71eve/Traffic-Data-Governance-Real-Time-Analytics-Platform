-- ============================================================================
-- MySQL 业务库初始化脚本
-- 创建维表和模拟数据，供 DataX/Maxwell/Flume 采集
-- ============================================================================

USE traffic_biz;

-- ============================================================================
-- 1. 道路信息表 (供 DataX 全量同步到 Hive ODS)
-- ============================================================================
CREATE TABLE IF NOT EXISTS t_road (
    road_id         VARCHAR(32) PRIMARY KEY COMMENT '道路ID',
    road_name       VARCHAR(128) NOT NULL COMMENT '道路名称',
    road_type       VARCHAR(32) COMMENT '道路类型(主干道/次干道/支路/快速路/高速)',
    road_length     DECIMAL(8,2) COMMENT '道路长度(公里)',
    lane_count      INT DEFAULT 2 COMMENT '车道数',
    speed_limit     INT DEFAULT 60 COMMENT '限速(km/h)',
    area_id         VARCHAR(32) COMMENT '所属区域ID',
    direction       VARCHAR(32) COMMENT '行驶方向',
    status          VARCHAR(16) DEFAULT 'ACTIVE' COMMENT '状态(ACTIVE/INACTIVE)',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='道路信息维表';

INSERT INTO t_road (road_id, road_name, road_type, road_length, lane_count, speed_limit, area_id, direction) VALUES
('R001', '中山路', '主干道', 12.50, 6, 60, 'A001', 'BIDIRECTIONAL'),
('R002', '解放大道', '主干道', 15.80, 8, 70, 'A001', 'BIDIRECTIONAL'),
('R003', '建设路', '次干道', 8.20, 4, 50, 'A002', 'BIDIRECTIONAL'),
('R004', '人民路', '次干道', 6.50, 4, 50, 'A002', 'BIDIRECTIONAL'),
('R005', '滨江大道', '快速路', 22.00, 6, 80, 'A003', 'BIDIRECTIONAL'),
('R006', '环城高速', '高速', 45.00, 4, 120, 'A004', 'BIDIRECTIONAL'),
('R007', '科技大道', '主干道', 10.00, 6, 60, 'A001', 'BIDIRECTIONAL'),
('R008', '文化路', '支路', 3.50, 2, 30, 'A002', 'BIDIRECTIONAL'),
('R009', '体育路', '次干道', 5.00, 4, 50, 'A003', 'BIDIRECTIONAL'),
('R010', '机场高速', '高速', 28.00, 4, 100, 'A004', 'BIDIRECTIONAL'),
('R011', '青年路', '主干道', 9.80, 6, 60, 'A001', 'BIDIRECTIONAL'),
('R012', '和平大道', '次干道', 7.20, 4, 50, 'A002', 'BIDIRECTIONAL'),
('R013', '友谊路', '支路', 2.80, 2, 30, 'A003', 'BIDIRECTIONAL'),
('R014', '工业大道', '主干道', 11.50, 6, 60, 'A004', 'BIDIRECTIONAL'),
('R015', '商业街', '支路', 1.50, 2, 20, 'A001', 'BIDIRECTIONAL');

-- ============================================================================
-- 2. 设备信息表 (供 Maxwell CDC 采集变更)
-- ============================================================================
CREATE TABLE IF NOT EXISTS t_device (
    device_id       VARCHAR(32) PRIMARY KEY COMMENT '设备ID',
    device_type     VARCHAR(32) COMMENT '设备类型(摄像头/雷达/地磁/气象站)',
    road_id         VARCHAR(32) COMMENT '所属道路ID',
    area_id         VARCHAR(32) COMMENT '所属区域ID',
    install_date    DATE COMMENT '安装日期',
    status          VARCHAR(16) DEFAULT 'ONLINE' COMMENT '状态(ONLINE/OFFLINE/MAINTENANCE)',
    ip_address      VARCHAR(32) COMMENT 'IP地址',
    firmware_version VARCHAR(32) COMMENT '固件版本',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备信息维表';

INSERT INTO t_device (device_id, device_type, road_id, area_id, install_date, status, ip_address, firmware_version) VALUES
('D001', '摄像头', 'R001', 'A001', '2024-01-15', 'ONLINE', '192.168.1.101', 'v2.1.0'),
('D002', '雷达', 'R001', 'A001', '2024-01-15', 'ONLINE', '192.168.1.102', 'v2.1.0'),
('D003', '地磁', 'R002', 'A001', '2024-02-20', 'ONLINE', '192.168.1.103', 'v2.0.5'),
('D004', '摄像头', 'R003', 'A002', '2024-03-10', 'ONLINE', '192.168.1.104', 'v2.1.0'),
('D005', '气象站', 'R005', 'A003', '2024-01-05', 'ONLINE', '192.168.1.105', 'v1.9.8'),
('D006', '摄像头', 'R006', 'A004', '2024-04-01', 'OFFLINE', '192.168.1.106', 'v2.1.0'),
('D007', '雷达', 'R007', 'A001', '2024-02-28', 'ONLINE', '192.168.1.107', 'v2.1.0'),
('D008', '地磁', 'R008', 'A002', '2024-03-15', 'ONLINE', '192.168.1.108', 'v2.0.5'),
('D009', '摄像头', 'R009', 'A003', '2024-01-20', 'ONLINE', '192.168.1.109', 'v2.1.0'),
('D010', '雷达', 'R010', 'A004', '2024-05-01', 'MAINTENANCE', '192.168.1.110', 'v2.1.0'),
('D011', '摄像头', 'R011', 'A001', '2024-02-10', 'ONLINE', '192.168.1.111', 'v2.1.0'),
('D012', '地磁', 'R012', 'A002', '2024-03-20', 'ONLINE', '192.168.1.112', 'v2.0.5'),
('D013', '气象站', 'R013', 'A003', '2024-01-25', 'ONLINE', '192.168.1.113', 'v1.9.8'),
('D014', '摄像头', 'R014', 'A004', '2024-04-15', 'ONLINE', '192.168.1.114', 'v2.1.0'),
('D015', '雷达', 'R015', 'A001', '2024-02-05', 'ONLINE', '192.168.1.115', 'v2.1.0'),
('D016', '摄像头', 'R001', 'A001', '2024-06-01', 'ONLINE', '192.168.1.116', 'v2.2.0'),
('D017', '地磁', 'R002', 'A001', '2024-06-01', 'ONLINE', '192.168.1.117', 'v2.0.5'),
('D018', '雷达', 'R005', 'A003', '2024-06-01', 'ONLINE', '192.168.1.118', 'v2.2.0'),
('D019', '摄像头', 'R006', 'A004', '2024-06-01', 'ONLINE', '192.168.1.119', 'v2.2.0'),
('D020', '气象站', 'R007', 'A001', '2024-06-01', 'ONLINE', '192.168.1.120', 'v2.0.0');

-- ============================================================================
-- 3. 区域信息表
-- ============================================================================
CREATE TABLE IF NOT EXISTS t_area (
    area_id         VARCHAR(32) PRIMARY KEY COMMENT '区域ID',
    area_name       VARCHAR(128) NOT NULL COMMENT '区域名称',
    city_id         VARCHAR(32) COMMENT '城市ID',
    city_name       VARCHAR(128) COMMENT '城市名称',
    area_type       VARCHAR(32) COMMENT '区域类型(商业区/住宅区/工业区/交通枢纽)',
    population      INT COMMENT '人口数量(万)',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='区域信息维表';

INSERT INTO t_area (area_id, area_name, city_id, city_name, area_type, population) VALUES
('A001', '朝阳区', 'C001', '北京市', '商业区', 120),
('A002', '海淀区', 'C001', '北京市', '住宅区', 150),
('A003', '通州区', 'C001', '北京市', '工业区', 80),
('A004', '大兴区', 'C001', '北京市', '交通枢纽', 60);

-- ============================================================================
-- 4. 告警配置表 (供 Maxwell CDC 采集)
-- ============================================================================
CREATE TABLE IF NOT EXISTS t_alarm_config (
    config_id       VARCHAR(32) PRIMARY KEY COMMENT '配置ID',
    device_id       VARCHAR(32) COMMENT '设备ID',
    alarm_type      VARCHAR(32) COMMENT '告警类型(OFFLINE/HIGH_CPU/HIGH_TEMP)',
    threshold_value DECIMAL(8,2) COMMENT '阈值',
    notify_method   VARCHAR(32) COMMENT '通知方式(EMAIL/SMS/DINGTALK)',
    is_enabled      TINYINT DEFAULT 1 COMMENT '是否启用(0/1)',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='告警配置表';

INSERT INTO t_alarm_config (config_id, device_id, alarm_type, threshold_value, notify_method) VALUES
('AC001', 'D001', 'OFFLINE', 1, 'DINGTALK'),
('AC002', 'D001', 'HIGH_CPU', 90, 'DINGTALK'),
('AC003', 'D002', 'OFFLINE', 1, 'EMAIL'),
('AC004', 'D003', 'HIGH_TEMP', 80, 'SMS'),
('AC005', 'D004', 'OFFLINE', 1, 'DINGTALK'),
('AC006', 'D005', 'HIGH_CPU', 85, 'EMAIL'),
('AC007', 'D006', 'OFFLINE', 1, 'DINGTALK'),
('AC008', 'D007', 'HIGH_TEMP', 80, 'SMS'),
('AC009', 'D008', 'OFFLINE', 1, 'EMAIL'),
('AC010', 'D009', 'HIGH_CPU', 90, 'DINGTALK');

-- ============================================================================
-- 5. 车辆通行记录表 (模拟业务数据)
-- ============================================================================
CREATE TABLE IF NOT EXISTS t_vehicle_pass (
    pass_id         BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '通行记录ID',
    vehicle_id      VARCHAR(32) COMMENT '车辆ID',
    road_id         VARCHAR(32) COMMENT '道路ID',
    device_id       VARCHAR(32) COMMENT '设备ID',
    pass_time       DATETIME COMMENT '通行时间',
    speed           INT COMMENT '车速(km/h)',
    direction       VARCHAR(16) COMMENT '行驶方向',
    plate_number    VARCHAR(32) COMMENT '车牌号',
    vehicle_type    VARCHAR(32) COMMENT '车辆类型(轿车/货车/客车/摩托车)',
    lane            INT COMMENT '车道号',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='车辆通行记录表';

-- 插入模拟通行数据
INSERT INTO t_vehicle_pass (vehicle_id, road_id, device_id, pass_time, speed, direction, plate_number, vehicle_type, lane) VALUES
('V001', 'R001', 'D001', '2026-06-09 08:00:00', 55, 'N', '京A12345', '轿车', 1),
('V002', 'R001', 'D001', '2026-06-09 08:00:05', 62, 'S', '京B67890', '货车', 2),
('V003', 'R002', 'D003', '2026-06-09 08:00:10', 48, 'N', '京C11111', '客车', 1),
('V004', 'R003', 'D004', '2026-06-09 08:00:15', 35, 'E', '京D22222', '轿车', 1),
('V005', 'R001', 'D002', '2026-06-09 08:00:20', 58, 'N', '京E33333', '摩托车', 1),
('V006', 'R005', 'D005', '2026-06-09 08:00:25', 72, 'W', '京F44444', '轿车', 1),
('V007', 'R006', 'D006', '2026-06-09 08:00:30', 95, 'E', '京G55555', '货车', 2),
('V008', 'R007', 'D007', '2026-06-09 08:00:35', 45, 'N', '京H66666', '客车', 1),
('V009', 'R008', 'D008', '2026-06-09 08:00:40', 25, 'E', '京J77777', '轿车', 1),
('V010', 'R009', 'D009', '2026-06-09 08:00:45', 52, 'S', '京K88888', '摩托车', 1);

-- ============================================================================
-- 6. 创建 Maxwell 所需用户
-- ============================================================================
CREATE USER IF NOT EXISTS 'maxwell'@'%' IDENTIFIED BY 'maxwell123';
GRANT ALL ON maxwell.* TO 'maxwell'@'%';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'maxwell'@'%';
FLUSH PRIVILEGES;

-- ============================================================================
-- 7. 创建 DataX 所需用户
-- ============================================================================
CREATE USER IF NOT EXISTS 'datax_reader'@'%' IDENTIFIED BY 'datax123';
GRANT SELECT ON traffic_biz.* TO 'datax_reader'@'%';
FLUSH PRIVILEGES;

-- ============================================================================
-- 完成
-- ============================================================================
SELECT 'MySQL 业务数据初始化完成' AS status;
SELECT CONCAT('道路: ', COUNT(*), ' 条') FROM t_road;
SELECT CONCAT('设备: ', COUNT(*), ' 条') FROM t_device;
SELECT CONCAT('区域: ', COUNT(*), ' 条') FROM t_area;
SELECT CONCAT('告警配置: ', COUNT(*), ' 条') FROM t_alarm_config;
SELECT CONCAT('通行记录: ', COUNT(*), ' 条') FROM t_vehicle_pass;
