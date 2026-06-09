-- ============================================================
-- 交通业务数据库 (traffic_biz) — MySQL Schema
-- 引擎: InnoDB  |  字符集: utf8mb4  |  排序: utf8mb4_unicode_ci
-- ============================================================

CREATE DATABASE IF NOT EXISTS traffic_biz
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE traffic_biz;

-- ============================================================
-- 1. 道路基础信息表
-- ============================================================
CREATE TABLE IF NOT EXISTS biz_road_info (
    road_id         BIGINT       NOT NULL AUTO_INCREMENT  COMMENT '道路ID(主键, 自增)',
    road_name       VARCHAR(100) NOT NULL                 COMMENT '道路名称',
    road_type       VARCHAR(20)  NOT NULL DEFAULT ''      COMMENT '道路类型(主干道/次干道/支路/快速路/高速)',
    length_meters   DECIMAL(10,2) NOT NULL DEFAULT 0.00  COMMENT '道路长度(米)',
    lane_count      TINYINT      NOT NULL DEFAULT 1       COMMENT '车道数量',
    limit_speed     SMALLINT     NOT NULL DEFAULT 0       COMMENT '限速(km/h)',
    city_id         VARCHAR(20)  NOT NULL DEFAULT ''      COMMENT '所属城市ID',
    district        VARCHAR(50)  NOT NULL DEFAULT ''      COMMENT '所属行政区',
    create_time     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    PRIMARY KEY (road_id),
    INDEX idx_road_name (road_name),
    INDEX idx_road_type (road_type),
    INDEX idx_city_district (city_id, district)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='道路基础信息表';

-- ============================================================
-- 2. 设备基础信息表
-- ============================================================
CREATE TABLE IF NOT EXISTS biz_device_info (
    device_id       BIGINT       NOT NULL AUTO_INCREMENT  COMMENT '设备ID(主键, 自增)',
    device_name     VARCHAR(100) NOT NULL                 COMMENT '设备名称',
    device_type     VARCHAR(20)  NOT NULL DEFAULT ''      COMMENT '设备类型(camera/signal/sensor/display)',
    road_id         BIGINT       NOT NULL                 COMMENT '所属道路ID(外键)',
    latitude        DECIMAL(10,7) NOT NULL DEFAULT 0.0   COMMENT '纬度',
    longitude       DECIMAL(10,7) NOT NULL DEFAULT 0.0   COMMENT '经度',
    install_date    DATE         DEFAULT NULL             COMMENT '安装日期',
    manufacturer    VARCHAR(50)  NOT NULL DEFAULT ''      COMMENT '生产厂商',
    status          VARCHAR(20)  NOT NULL DEFAULT 'online' COMMENT '设备状态(online/offline/maintenance)',
    create_time     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    PRIMARY KEY (device_id),
    INDEX idx_device_name (device_name),
    INDEX idx_device_type (device_type),
    INDEX idx_device_status (status),
    INDEX idx_device_road (road_id),
    CONSTRAINT fk_device_road FOREIGN KEY (road_id) REFERENCES biz_road_info(road_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='设备基础信息表';

-- ============================================================
-- 3. 告警配置表
-- ============================================================
CREATE TABLE IF NOT EXISTS biz_alarm_config (
    config_id       INT          NOT NULL AUTO_INCREMENT  COMMENT '配置ID(主键, 自增)',
    device_type     VARCHAR(20)  NOT NULL DEFAULT ''      COMMENT '适用设备类型(camera/signal/sensor/display)',
    alarm_type      VARCHAR(30)  NOT NULL DEFAULT ''      COMMENT '告警类型(offline/cpu_high/temp_high/low_flow)',
    threshold_value DECIMAL(10,2) NOT NULL DEFAULT 0.00  COMMENT '告警阈值',
    unit            VARCHAR(10)  NOT NULL DEFAULT ''      COMMENT '阈值单位(次/百分比/°C)',
    enabled         TINYINT(1)   NOT NULL DEFAULT 1       COMMENT '是否启用(1=是, 0=否)',
    notify_channels VARCHAR(50)  NOT NULL DEFAULT ''      COMMENT '通知渠道(dingtalk/email/sms, 逗号分隔多项)',
    PRIMARY KEY (config_id),
    INDEX idx_alarm_type (alarm_type),
    INDEX idx_enabled (enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='告警配置表';

-- ============================================================
-- 4. 告警记录表
-- ============================================================
CREATE TABLE IF NOT EXISTS biz_alert_record (
    alert_id        BIGINT       NOT NULL AUTO_INCREMENT  COMMENT '告警ID(主键, 自增)',
    device_id       BIGINT       NOT NULL                 COMMENT '设备ID(外键)',
    alarm_type      VARCHAR(30)  NOT NULL DEFAULT ''      COMMENT '告警类型(offline/cpu_high/temp_high/low_flow)',
    alert_level     ENUM('P0','P1','P2','P3') NOT NULL   COMMENT '告警等级(P0=紧急, P1=严重, P2=一般, P3=提示)',
    alert_time      DATETIME     NOT NULL                 COMMENT '告警触发时间',
    recover_time    DATETIME     DEFAULT NULL             COMMENT '恢复时间(NULL=未恢复)',
    is_recovered    TINYINT(1)   NOT NULL DEFAULT 0       COMMENT '是否已恢复(1=是, 0=否)',
    description     TEXT                                  COMMENT '告警详细描述',
    PRIMARY KEY (alert_id),
    INDEX idx_device_id (device_id),
    INDEX idx_alarm_type (alarm_type),
    INDEX idx_alert_level (alert_level),
    INDEX idx_alert_time (alert_time),
    INDEX idx_recovered (is_recovered),
    CONSTRAINT fk_alert_device FOREIGN KEY (device_id) REFERENCES biz_device_info(device_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='告警记录表';

-- ============================================================
-- 初始化数据: 道路 (10 条真实中国城市道路)
-- ============================================================
INSERT INTO biz_road_info (road_name, road_type, length_meters, lane_count, limit_speed, city_id, district) VALUES
('京藏高速(北京段)', '高速',   25600.00, 4, 120, '110000', '海淀区'),
('北四环路',         '主干道',  8200.00, 6,  80, '110000', '朝阳区'),
('长安街',           '主干道',  6800.00, 8,  60, '110000', '东城区'),
('二环路',           '快速路', 32700.00, 6,  80, '110000', '西城区'),
('京通快速路',       '快速路', 14000.00, 4, 100, '110000', '朝阳区'),
('三环路',           '快速路', 48300.00, 6,  80, '110000', '丰台区'),
('延安高架路',       '快速路', 15300.00, 6,  80, '310000', '黄浦区'),
('内环高架路',       '快速路', 47570.00, 4,  80, '310000', '徐汇区'),
('南京路步行街',     '支路',    5500.00, 2,  30, '310000', '黄浦区'),
('深南大道',         '主干道', 28100.00, 8,  60, '440300', '南山区');

-- ============================================================
-- 初始化数据: 设备 (15 台, 四种类型混合, 分布在不同道路)
-- ============================================================
INSERT INTO biz_device_info (device_name, device_type, road_id, latitude, longitude, install_date, manufacturer, status) VALUES
-- camera 摄像头
('北四环高清摄像头01',  'camera',  2, 39.9812, 116.3723, '2024-03-15', '海康威视', 'online'),
('长安街全景摄像头01',  'camera',  3, 39.9054, 116.3976, '2024-01-20', '大华',     'online'),
('二环路测速摄像头01',  'camera',  4, 39.9133, 116.3625, '2023-11-10', '海康威视', 'online'),
('三环路卡口摄像头01',  'camera',  6, 39.8722, 116.3585, '2024-06-01', '宇视',     'online'),
('深南大道监控摄像头01','camera', 10, 22.5431, 113.9425, '2024-02-28', '海康威视', 'maintenance'),
-- signal 信号灯
('长安街智能信号灯01',  'signal',  3, 39.9050, 116.3950, '2023-09-01', '西门子',   'online'),
('二环路信号灯01',      'signal',  4, 39.9150, 116.3680, '2023-08-15', '西门子',   'online'),
('延安高架信号灯01',    'signal',  7, 31.2320, 121.4720, '2024-04-10', '海信',     'online'),
-- sensor 传感器
('京藏高速地磁传感器01', 'sensor', 1, 40.0210, 116.2820, '2024-05-20', 'Geophone', 'online'),
('北四环流量传感器01',  'sensor',  2, 39.9800, 116.3700, '2024-07-15', 'Geophone', 'online'),
('京通快速温湿度传感器','sensor',  5, 39.9345, 116.5114, '2024-03-08', 'Sensirion','online'),
-- display 信息屏
('二环路交通诱导屏01',  'display', 4, 39.9100, 116.3650, '2023-12-20', '利亚德',   'online'),
('三环路信息发布屏01',  'display', 6, 39.8750, 116.3550, '2024-08-05', '洲明科技', 'online'),
('内环高架信息屏01',    'display', 8, 31.2150, 121.4520, '2024-01-30', '利亚德',   'offline'),
('延安高架交通诱导屏01','display', 7, 31.2300, 121.4700, '2024-06-18', '洲明科技', 'online');

-- ============================================================
-- 初始化数据: 告警配置 (3 条)
-- ============================================================
INSERT INTO biz_alarm_config (device_type, alarm_type, threshold_value, unit, enabled, notify_channels) VALUES
('camera',  'offline',  3,    '次',      1, 'dingtalk,email'),
('sensor',  'cpu_high', 90.0, '百分比',  1, 'dingtalk,sms'),
('display', 'temp_high',80.0, '°C',      1, 'dingtalk,email,sms');
