-- ============================================
-- MySQL 交通业务库初始化 DDL
-- 智慧城市交通数据治理与实时分析平台
-- 用途：模拟交通管理中心生产业务库
-- ============================================

CREATE DATABASE IF NOT EXISTS traffic_business
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE traffic_business;

-- ============================================
-- 1. 区域信息表（静态维表）
-- ============================================
CREATE TABLE IF NOT EXISTS t_area (
    area_id         VARCHAR(32)   NOT NULL COMMENT '区域ID(主键)',
    area_name       VARCHAR(64)   NOT NULL COMMENT '区域名称',
    area_code       VARCHAR(12)   NOT NULL COMMENT '行政区划代码',
    city_id         VARCHAR(32)   NOT NULL COMMENT '所属城市ID',
    city_name       VARCHAR(64)   NOT NULL COMMENT '所属城市名称',
    province_id     VARCHAR(32)   NOT NULL COMMENT '所属省份ID',
    province_name   VARCHAR(64)   NOT NULL COMMENT '所属省份名称',
    area_level      VARCHAR(16)   NOT NULL COMMENT '区域级别(DISTRICT/COUNTY)',
    area_type       VARCHAR(16)   NOT NULL COMMENT '区域类型(URBAN/SUBURBAN/RURAL)',
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (area_id),
    INDEX idx_city (city_id),
    INDEX idx_province (province_id)
) ENGINE=InnoDB COMMENT='区域基础信息表';

-- ============================================
-- 2. 道路信息表（静态维表，Maxwell CDC采集变更）
-- ============================================
CREATE TABLE IF NOT EXISTS t_road (
    road_id         VARCHAR(32)   NOT NULL COMMENT '道路ID(主键)',
    road_name       VARCHAR(128)  NOT NULL COMMENT '道路名称',
    road_type       VARCHAR(32)   NOT NULL COMMENT '道路类型(主干道/次干道/支路/快速路/高速)',
    road_length     DECIMAL(8,2)  NOT NULL COMMENT '道路长度(公里)',
    lane_count      TINYINT       NOT NULL COMMENT '车道数',
    speed_limit     INT           NOT NULL COMMENT '限速(km/h)',
    area_id         VARCHAR(32)   NOT NULL COMMENT '所属区域ID',
    direction       VARCHAR(16)   NOT NULL COMMENT '行驶方向(N/S/E/W/BIDIRECTIONAL)',
    status          VARCHAR(16)   NOT NULL DEFAULT 'ACTIVE' COMMENT '状态(ACTIVE/CLOSED/MAINTENANCE)',
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (road_id),
    INDEX idx_area (area_id),
    INDEX idx_status (status)
) ENGINE=InnoDB COMMENT='道路基础信息表';

-- ============================================
-- 3. 设备信息表（静态维表，Maxwell CDC采集变更）
-- ============================================
CREATE TABLE IF NOT EXISTS t_device (
    device_id       VARCHAR(32)   NOT NULL COMMENT '设备ID(主键)',
    device_name     VARCHAR(128)  NOT NULL COMMENT '设备名称',
    device_type     VARCHAR(32)   NOT NULL COMMENT '设备类型(CAMERA/SENSOR/RADAR/GATE/TRAFFIC_LIGHT)',
    device_model    VARCHAR(64)   NOT NULL COMMENT '设备型号',
    road_id         VARCHAR(32)   NOT NULL COMMENT '所属道路ID',
    area_id         VARCHAR(32)   NOT NULL COMMENT '所属区域ID',
    install_date    DATE          NOT NULL COMMENT '安装日期',
    manufacturer    VARCHAR(64)   NOT NULL COMMENT '生产厂商',
    firmware_ver    VARCHAR(32)   NOT NULL COMMENT '固件版本',
    ip_address      VARCHAR(45)   NOT NULL COMMENT 'IP地址',
    status          VARCHAR(16)   NOT NULL DEFAULT 'RUNNING' COMMENT '状态(RUNNING/MAINTENANCE/DECOMMISSIONED/OFFLINE)',
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (device_id),
    INDEX idx_road (road_id),
    INDEX idx_area (area_id),
    INDEX idx_status (status),
    INDEX idx_type (device_type)
) ENGINE=InnoDB COMMENT='设备基础信息表';

-- ============================================
-- 4. 设备维修记录表（Maxwell CDC采集变更）
-- ============================================
CREATE TABLE IF NOT EXISTS t_device_repair (
    repair_id       BIGINT        NOT NULL AUTO_INCREMENT COMMENT '维修记录ID',
    device_id       VARCHAR(32)   NOT NULL COMMENT '设备ID',
    repair_type     VARCHAR(32)   NOT NULL COMMENT '维修类型(ROUTINE/EMERGENCY/OVERHAUL)',
    fault_desc      TEXT          COMMENT '故障描述',
    repair_desc     TEXT          COMMENT '维修描述',
    repair_status   VARCHAR(16)   NOT NULL DEFAULT 'IN_PROGRESS' COMMENT '维修状态(IN_PROGRESS/DONE/VERIFIED)',
    start_time      DATETIME      NOT NULL COMMENT '维修开始时间',
    end_time        DATETIME      COMMENT '维修结束时间',
    engineer        VARCHAR(64)   COMMENT '维修工程师',
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (repair_id),
    INDEX idx_device (device_id),
    INDEX idx_status (repair_status),
    INDEX idx_start_time (start_time)
) ENGINE=InnoDB COMMENT='设备维修记录表';

-- ============================================
-- 5. 配置信息表（Maxwell CDC采集变更）
-- ============================================
CREATE TABLE IF NOT EXISTS t_config (
    config_id       BIGINT        NOT NULL AUTO_INCREMENT COMMENT '配置ID',
    device_id       VARCHAR(32)   NOT NULL COMMENT '设备ID',
    config_key      VARCHAR(64)   NOT NULL COMMENT '配置项名称',
    config_value    VARCHAR(256)  NOT NULL COMMENT '配置值',
    config_desc     VARCHAR(256)  COMMENT '配置说明',
    modified_by     VARCHAR(64)   COMMENT '修改人',
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (config_id),
    UNIQUE KEY uk_device_key (device_id, config_key),
    INDEX idx_device (device_id)
) ENGINE=InnoDB COMMENT='设备配置信息表';

-- ============================================
-- 6. 告警记录表（Maxwell CDC采集 binlog）
-- ============================================
CREATE TABLE IF NOT EXISTS t_alarm (
    alarm_id        BIGINT        NOT NULL AUTO_INCREMENT COMMENT '告警ID',
    device_id       VARCHAR(32)   NOT NULL COMMENT '设备ID',
    alarm_type      VARCHAR(32)   NOT NULL COMMENT '告警类型(HARDWARE/SOFTWARE/NETWORK/POWER/OTHER)',
    alarm_level     VARCHAR(16)   NOT NULL COMMENT '告警级别(CRITICAL/MAJOR/MINOR/WARNING)',
    alarm_content   TEXT          COMMENT '告警内容',
    alarm_time      DATETIME      NOT NULL COMMENT '告警时间',
    recover_time    DATETIME      COMMENT '恢复时间',
    recover_status  VARCHAR(16)   NOT NULL DEFAULT 'UNRECOVERED' COMMENT '恢复状态(UNRECOVERED/RECOVERED/AUTO_RECOVERED)',
    ack_by          VARCHAR(64)   COMMENT '确认人',
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (alarm_id),
    INDEX idx_device (device_id),
    INDEX idx_alarm_time (alarm_time),
    INDEX idx_level (alarm_level),
    INDEX idx_recover (recover_status)
) ENGINE=InnoDB COMMENT='告警记录表';

-- ============================================
-- 初始数据：区域（5个区）
-- ============================================
INSERT INTO t_area (area_id, area_name, area_code, city_id, city_name, province_id, province_name, area_level, area_type) VALUES
('A001', '朝阳区',  '110105', 'BJ', '北京市', '11', '北京市', 'DISTRICT', 'URBAN'),
('A002', '海淀区',  '110108', 'BJ', '北京市', '11', '北京市', 'DISTRICT', 'URBAN'),
('A003', '浦东新区','310115', 'SH', '上海市', '31', '上海市', 'DISTRICT', 'URBAN'),
('A004', '天河区',  '440106', 'GZ', '广州市', '44', '广东省', 'DISTRICT', 'URBAN'),
('A005', '南山区',  '440305', 'SZ', '深圳市', '44', '广东省', 'DISTRICT', 'URBAN');

-- ============================================
-- 初始数据：道路（每条主干道配几条支路）
-- ============================================
INSERT INTO t_road (road_id, road_name, road_type, road_length, lane_count, speed_limit, area_id, direction) VALUES
('R001', '长安街',         '主干道', 12.50, 8, 70, 'A001', 'BIDIRECTIONAL'),
('R002', '东三环路',       '快速路', 15.20, 6, 80, 'A001', 'S'),
('R003', '西三环路',       '快速路', 14.80, 6, 80, 'A001', 'N'),
('R004', '建国路',         '主干道', 8.30,  6, 60, 'A001', 'E'),
('R005', '朝阳北路',       '次干道', 6.70,  4, 50, 'A001', 'E'),
('R006', '中关村大街',     '主干道', 10.20, 6, 60, 'A002', 'BIDIRECTIONAL'),
('R007', '学院路',         '次干道', 5.80,  4, 50, 'A002', 'BIDIRECTIONAL'),
('R008', '知春路',         '次干道', 4.50,  4, 50, 'A002', 'BIDIRECTIONAL'),
('R009', '世纪大道',       '主干道', 8.00,  8, 70, 'A003', 'BIDIRECTIONAL'),
('R010', '张江路',         '次干道', 5.50,  4, 50, 'A003', 'BIDIRECTIONAL'),
('R011', '天河路',         '主干道', 7.20,  6, 60, 'A004', 'BIDIRECTIONAL'),
('R012', '中山大道',       '主干道', 11.00, 6, 60, 'A004', 'E'),
('R013', '深南大道',       '主干道', 13.50, 8, 70, 'A005', 'E'),
('R014', '科技园路',       '次干道', 4.20,  4, 50, 'A005', 'BIDIRECTIONAL'),
('R015', '滨海大道',       '快速路', 16.00, 6, 80, 'A005', 'S');

-- ============================================
-- 初始数据：设备（摄像头/传感器/雷达/卡口/红绿灯）
-- ============================================
INSERT INTO t_device (device_id, device_name, device_type, device_model, road_id, area_id, install_date, manufacturer, firmware_ver, ip_address, status) VALUES
('D001', '长安街卡口1号',       'GATE',          'HK-2000', 'R001', 'A001', '2023-03-15', '海康威视', 'v2.3.1', '10.1.1.101', 'RUNNING'),
('D002', '长安街卡口2号',       'GATE',          'HK-2000', 'R001', 'A001', '2023-03-15', '海康威视', 'v2.3.1', '10.1.1.102', 'RUNNING'),
('D003', '东三环雷达1号',       'RADAR',         'RD-500',  'R002', 'A001', '2023-05-20', '大华',     'v1.8.0', '10.1.2.101', 'RUNNING'),
('D004', '西三环地磁传感器1号', 'SENSOR',        'GM-100',  'R003', 'A001', '2023-06-10', '博世',     'v3.1.0', '10.1.3.101', 'RUNNING'),
('D005', '建国路摄像头1号',     'CAMERA',        'IPC-800', 'R004', 'A001', '2023-04-01', '海康威视', 'v2.3.1', '10.1.4.101', 'RUNNING'),
('D006', '朝阳北路红绿灯1号',   'TRAFFIC_LIGHT', 'TL-300',  'R005', 'A001', '2023-07-01', '西门子',   'v1.2.0', '10.1.5.101', 'RUNNING'),
('D007', '中关村卡口1号',       'GATE',          'HK-2000', 'R006', 'A002', '2023-03-20', '海康威视', 'v2.3.1', '10.2.1.101', 'RUNNING'),
('D008', '中关村摄像头1号',     'CAMERA',        'IPC-800', 'R006', 'A002', '2023-03-20', '海康威视', 'v2.3.1', '10.2.1.102', 'MAINTENANCE'),
('D009', '学院路地磁传感器1号', 'SENSOR',        'GM-100',  'R007', 'A002', '2023-08-15', '博世',     'v3.1.0', '10.2.2.101', 'RUNNING'),
('D010', '世纪大道雷达1号',     'RADAR',         'RD-500',  'R009', 'A003', '2023-04-10', '大华',     'v1.8.0', '10.3.1.101', 'RUNNING'),
('D011', '世纪大道卡口1号',     'GATE',          'HK-2000', 'R009', 'A003', '2023-04-10', '海康威视', 'v2.3.1', '10.3.1.102', 'RUNNING'),
('D012', '张江路摄像头1号',     'CAMERA',        'IPC-800', 'R010', 'A003', '2023-09-01', '大华',     'v1.5.2', '10.3.2.101', 'OFFLINE'),
('D013', '天河路卡口1号',       'GATE',          'HK-2000', 'R011', 'A004', '2023-05-05', '海康威视', 'v2.3.1', '10.4.1.101', 'RUNNING'),
('D014', '天河路雷达1号',       'RADAR',         'RD-500',  'R011', 'A004', '2023-05-05', '大华',     'v1.8.0', '10.4.1.102', 'RUNNING'),
('D015', '深南大道卡口1号',     'GATE',          'HK-2000', 'R013', 'A005', '2023-02-18', '海康威视', 'v2.3.1', '10.5.1.101', 'RUNNING'),
('D016', '深南大道卡口2号',     'GATE',          'HK-2000', 'R013', 'A005', '2023-02-18', '海康威视', 'v2.3.1', '10.5.1.102', 'RUNNING'),
('D017', '科技园路摄像头1号',   'CAMERA',        'IPC-800', 'R014', 'A005', '2023-10-01', '大华',     'v1.5.2', '10.5.2.101', 'RUNNING'),
('D018', '滨海大道雷达1号',     'RADAR',         'RD-500',  'R015', 'A005', '2023-01-15', '大华',     'v1.8.0', '10.5.3.101', 'RUNNING'),
('D019', '滨海大道红绿灯1号',   'TRAFFIC_LIGHT', 'TL-300',  'R015', 'A005', '2023-01-15', '西门子',   'v1.2.0', '10.5.3.102', 'RUNNING'),
('D020', '中山大道卡口1号',     'GATE',          'HK-2000', 'R012', 'A004', '2023-06-20', '海康威视', 'v2.3.1', '10.4.2.101', 'RUNNING');
