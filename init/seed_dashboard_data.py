#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Superset 专用数据初始化 — 创建实际业务视图 + 灌入丰富的演示数据
给 Superset 大屏提供可直接使用的结构化数据

用法:
  python init/seed_dashboard_data.py
"""
import sqlite3, os, random, sys
from datetime import datetime, timedelta

DB = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "traffic_data.db"))
# Docker 内容器路径
DOCKER_DB = "/app/data/traffic_data.db"

AREAS = ["高新区", "老城区", "新城区", "开发区", "滨江区"]
ROADS = [
    ("长安街", "主干道", 60, 5.2), ("东三环路", "快速路", 80, 8.5),
    ("西二环路", "快速路", 80, 7.8), ("人民路", "主干道", 60, 4.5),
    ("深南大道", "主干道", 70, 6.8), ("天府大道", "主干道", 70, 7.2),
    ("南京路", "商业街", 40, 3.2), ("京藏高速", "高速公路", 120, 12.5),
    ("北四环路", "快速路", 80, 6.5), ("京通快速", "高速公路", 100, 8.0),
]
DEVICE_NAMES = [f"CT-{i:04d}" for i in range(1, 21)]
DEVICE_TYPES = ["摄像头", "信号灯", "流量计", "雷达", "诱导屏"]
TABLE_NAMES = [
    "ods_vehicle_pass_di", "dwd_vehicle_pass_di", "dwd_device_status_di",
    "dws_road_hour_flow", "dws_area_hour_flow", "ads_traffic_operation",
    "ads_top_jam_roads", "ads_device_health_score", "ads_device_mtbf_mttr"
]


def get_conn():
    """获取数据库连接"""
    paths = [DB, DOCKER_DB]
    for p in paths:
        if os.path.exists(p):
            print(f"[INFO] 使用数据库: {p}")
            return sqlite3.connect(p)
    # 创建新的
    print(f"[INFO] 创建新数据库: {DB}")
    return sqlite3.connect(DB)


def seed():
    conn = get_conn()
    cur = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")
    print(f"[SEED] 初始化日期: {today}")

    # ========== 1. 维度表 ==========

    # dim_area
    cur.execute("DROP TABLE IF EXISTS dim_area")
    cur.execute("CREATE TABLE dim_area (area_id INTEGER PRIMARY KEY, area_name TEXT, city TEXT, admin_level TEXT)")
    area_data = [
        (1, "高新区", "北京市", "城区"), (2, "老城区", "北京市", "城区"),
        (3, "新城区", "北京市", "城区"), (4, "开发区", "北京市", "开发区"),
        (5, "滨江区", "北京市", "城区"),
    ]
    for a in area_data:
        cur.execute("INSERT INTO dim_area VALUES (?,?,?,?)", a)

    # dim_road
    cur.execute("DROP TABLE IF EXISTS dim_road")
    cur.execute("""CREATE TABLE dim_road (
        road_id INTEGER PRIMARY KEY, road_name TEXT, road_type TEXT,
        limit_speed INTEGER, length_km REAL, area_id INTEGER)""")
    for i, (name, rtype, speed, length) in enumerate(ROADS, 1):
        cur.execute("INSERT INTO dim_road VALUES (?,?,?,?,?,?)",
                    (i, name, rtype, speed, length, random.randint(1, 5)))

    # dim_time
    cur.execute("DROP TABLE IF EXISTS dim_time")
    cur.execute("CREATE TABLE dim_time (hour INTEGER PRIMARY KEY, period TEXT)")
    for h in range(24):
        if 7 <= h <= 9:
            period = "早高峰"
        elif 17 <= h <= 19:
            period = "晚高峰"
        elif 0 <= h <= 5:
            period = "凌晨"
        elif 12 <= h <= 14:
            period = "午间"
        else:
            period = "平峰"
        cur.execute("INSERT INTO dim_time VALUES (?,?)", (h, period))

    # dim_device
    cur.execute("DROP TABLE IF EXISTS dim_device")
    cur.execute("""CREATE TABLE dim_device (
        device_id TEXT PRIMARY KEY, device_name TEXT, device_type TEXT,
        road_id INTEGER, install_date TEXT)""")
    for i, dev in enumerate(DEVICE_NAMES, 1):
        cur.execute("INSERT INTO dim_device VALUES (?,?,?,?,?)",
                    (f"D{i:04d}", dev, random.choice(DEVICE_TYPES),
                     random.randint(1, 10),
                     f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}"))

    # ========== 2. ODS 层 ==========
    cur.execute("DROP TABLE IF EXISTS ods_vehicle_pass_di")
    cur.execute("""CREATE TABLE ods_vehicle_pass_di (
        dt TEXT, road_id TEXT, plate TEXT, pass_time TEXT, speed REAL, vehicle_type TEXT)""")
    plates = []
    for _ in range(5000):
        dt_str = f"{today} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        plate = f"{random.choice('京津冀沪渝')}{chr(random.randint(65,90))}{random.randint(10000,99999)}"
        plates.append(plate)
        vtype = random.choice(["轿车", "SUV", "货车", "公交车", "摩托车"])
        cur.execute("INSERT INTO ods_vehicle_pass_di VALUES (?,?,?,?,?,?)",
                    (today, random.choice([r[0] for r in ROADS]),
                     plate, dt_str, round(random.uniform(10, 120), 1), vtype))

    # ========== 3. DWD 层 ==========
    cur.execute("DROP TABLE IF EXISTS dwd_vehicle_pass_di")
    cur.execute("""CREATE TABLE dwd_vehicle_pass_di (
        dt TEXT, hour INTEGER, road_id TEXT, plate TEXT, pass_time TEXT,
        speed REAL, vehicle_type TEXT, is_overspeed INTEGER)""")
    for _ in range(3000):
        dt_str = f"{today} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        speed = round(random.uniform(10, 130), 1)
        overspeed = 1 if speed > 80 else 0
        cur.execute("INSERT INTO dwd_vehicle_pass_di VALUES (?,?,?,?,?,?,?,?)",
                    (today, random.randint(0, 23), random.choice([r[0] for r in ROADS]),
                     random.choice(plates), dt_str, speed,
                     random.choice(["轿车", "SUV", "货车", "公交车", "摩托车"]), overspeed))

    cur.execute("DROP TABLE IF EXISTS dwd_device_status_di")
    cur.execute("""CREATE TABLE dwd_device_status_di (
        dt TEXT, device_id TEXT, status TEXT, cpu_usage REAL, mem_usage REAL, online_duration INTEGER)""")
    for dev in DEVICE_NAMES:
        for h in range(24):
            status = "正常" if random.random() > 0.15 else "故障"
            cur.execute("INSERT INTO dwd_device_status_di VALUES (?,?,?,?,?,?)",
                        (today, dev, status,
                         round(random.uniform(10, 95), 1),
                         round(random.uniform(15, 90), 1),
                         random.randint(30, 60)))

    # ========== 4. DWS 层 ==========
    # dws_road_hour_flow — 每小时道路流量(核心表)
    cur.execute("DROP TABLE IF EXISTS dws_road_hour_flow")
    cur.execute("""CREATE TABLE dws_road_hour_flow (
        dt TEXT, hour INTEGER, road_id TEXT, road_name TEXT,
        traffic_count INTEGER, avg_speed REAL, jam_level INTEGER,
        avg_congestion_rate REAL)""")
    for h in range(24):
        peak = 1 if (7 <= h <= 9 or 17 <= h <= 19) else 0
        for road_name, rtype, limit_speed, length in ROADS:
            if peak:
                traffic = random.randint(800, 2500)
                speed = round(random.uniform(15, 45), 1)
                jam = random.randint(3, 5)
                congestion = round(random.uniform(6.0, 9.5), 2)
            elif 0 <= h <= 5:
                traffic = random.randint(50, 300)
                speed = round(random.uniform(60, 100), 1)
                jam = random.randint(1, 2)
                congestion = round(random.uniform(1.0, 3.0), 2)
            else:
                traffic = random.randint(300, 1200)
                speed = round(random.uniform(35, 70), 1)
                jam = random.randint(1, 3)
                congestion = round(random.uniform(2.0, 5.5), 2)
            cur.execute(
                "INSERT INTO dws_road_hour_flow VALUES (?,?,?,?,?,?,?,?)",
                (today, h, f"RD-{random.randint(10000,99999)}", road_name,
                 traffic, speed, jam, congestion))

    # dws_area_hour_flow
    cur.execute("DROP TABLE IF EXISTS dws_area_hour_flow")
    cur.execute("""CREATE TABLE dws_area_hour_flow (
        dt TEXT, hour INTEGER, area_id INTEGER,
        traffic_count INTEGER, avg_speed REAL, avg_congestion REAL)""")
    for h in range(24):
        for aid in range(1, 6):
            cur.execute("INSERT INTO dws_area_hour_flow VALUES (?,?,?,?,?,?)",
                        (today, h, aid,
                         random.randint(1000, 8000),
                         round(random.uniform(25, 75), 1),
                         round(random.uniform(1.5, 8.5), 2)))

    # dws_device_health_day — 7天健康趋势
    cur.execute("DROP TABLE IF EXISTS dws_device_health_day")
    cur.execute("CREATE TABLE dws_device_health_day (dt TEXT, health_score REAL)")
    for d in range(7):
        dt = (datetime.now() - timedelta(days=d)).strftime("%Y-%m-%d")
        cur.execute("INSERT INTO dws_device_health_day VALUES (?,?)",
                    (dt, round(random.uniform(70, 95), 1)))

    # dws_area_jam_hour
    cur.execute("DROP TABLE IF EXISTS dws_area_jam_hour")
    cur.execute("""CREATE TABLE dws_area_jam_hour (
        area_id INTEGER, dt TEXT, hour INTEGER,
        jam_level INTEGER, jam_count INTEGER,
        PRIMARY KEY (area_id, dt, hour))""")
    for h in range(24):
        for aid in range(1, 6):
            cur.execute("INSERT INTO dws_area_jam_hour VALUES (?,?,?,?,?)",
                        (aid, today, h, random.randint(1, 5), random.randint(0, 20)))

    # dws_alarm_day
    cur.execute("DROP TABLE IF EXISTS dws_alarm_day")
    cur.execute("""CREATE TABLE dws_alarm_day (
        alarm_type TEXT, dt TEXT, count INTEGER,
        severity TEXT, PRIMARY KEY (alarm_type, dt))""")
    alarm_types = [
        ("设备离线告警", "critical"), ("流量突增告警", "warning"),
        ("数据延迟告警", "warning"), ("超速告警", "info"),
        ("拥堵预警", "critical"),
    ]
    for alarm_type, severity in alarm_types:
        cur.execute("INSERT INTO dws_alarm_day VALUES (?,?,?,?)",
                    (alarm_type, today, random.randint(0, 50), severity))

    # ========== 5. ADS 层 ==========
    # ads_traffic_operation
    cur.execute("DROP TABLE IF EXISTS ads_traffic_operation")
    cur.execute("""CREATE TABLE ads_traffic_operation (
        dt TEXT, area_id INTEGER,
        total_traffic_flow INTEGER, avg_congestion_rate REAL)""")
    for aid in range(1, 6):
        cur.execute("INSERT INTO ads_traffic_operation VALUES (?,?,?,?)",
                    (today, aid, random.randint(5000, 30000),
                     round(random.uniform(1.5, 8.0), 2)))

    # ads_top_jam_roads
    cur.execute("DROP TABLE IF EXISTS ads_top_jam_roads")
    cur.execute("""CREATE TABLE ads_top_jam_roads (
        dt TEXT, road_name TEXT, avg_jam_level REAL, rank_num INTEGER)""")
    ranked = sorted(ROADS, key=lambda _: random.random())
    for rank, (road_name, _, _, _) in enumerate(ranked[:10], 1):
        cur.execute("INSERT INTO ads_top_jam_roads VALUES (?,?,?,?)",
                    (today, road_name, round(random.uniform(1.5, 4.5), 1), rank))

    # ads_device_health_score
    cur.execute("DROP TABLE IF EXISTS ads_device_health_score")
    cur.execute("""CREATE TABLE ads_device_health_score (
        dt TEXT, device_name TEXT, device_type TEXT, health_score REAL,
        health_level TEXT, online_rate REAL, avg_cpu_usage REAL, avg_mem_usage REAL)""")
    for dev in DEVICE_NAMES:
        score = round(random.uniform(60, 99), 1)
        level = "优秀" if score >= 90 else ("良好" if score >= 75 else "较差")
        cur.execute("INSERT INTO ads_device_health_score VALUES (?,?,?,?,?,?,?,?)",
                    (today, dev, random.choice(DEVICE_TYPES), score, level,
                     round(random.uniform(85, 100), 1),
                     round(random.uniform(10, 85), 1),
                     round(random.uniform(15, 80), 1)))

    # ads_device_mtbf_mttr
    cur.execute("DROP TABLE IF EXISTS ads_device_mtbf_mttr")
    cur.execute("""CREATE TABLE ads_device_mtbf_mttr (
        dt TEXT, device_name TEXT, mtbf_hours REAL, mttr_minutes REAL)""")
    for dev in DEVICE_NAMES:
        cur.execute("INSERT INTO ads_device_mtbf_mttr VALUES (?,?,?,?)",
                    (today, dev, round(random.uniform(500, 5000), 1),
                     round(random.uniform(5, 60), 1)))

    # ads_device_fault_top
    cur.execute("DROP TABLE IF EXISTS ads_device_fault_top")
    cur.execute("""CREATE TABLE ads_device_fault_top (
        dt TEXT, rank_num INTEGER,
        device_name TEXT, fault_rate REAL,
        PRIMARY KEY (dt, rank_num))""")
    for rank in range(1, 11):
        cur.execute("INSERT INTO ads_device_fault_top VALUES (?,?,?,?)",
                    (today, rank, random.choice(DEVICE_NAMES),
                     round(random.uniform(0.01, 0.15), 3)))

    # ========== 6. 数据质量 ==========
    cur.execute("DROP TABLE IF EXISTS data_quality_results")
    cur.execute("""CREATE TABLE data_quality_results (
        report_date TEXT, table_name TEXT,
        completeness_rate REAL, uniqueness_rate REAL, validity_rate REAL,
        kafka_lag INTEGER, score REAL, status TEXT)""")
    for t in TABLE_NAMES:
        score = round(random.uniform(0.88, 0.99), 2)
        cur.execute(
            "INSERT INTO data_quality_results VALUES (?,?,?,?,?,?,?,?)",
            (today, t,
             round(random.uniform(0.92, 1.0), 2),
             round(random.uniform(0.90, 0.99), 2),
             round(random.uniform(0.93, 1.0), 2),
             random.randint(0, 500),
             score,
             "PASS" if score > 0.93 else "WARN"))

    conn.commit()
    conn.close()

    print(f"[SEED] ✅ 数据库初始化完成!")
    print(f"       文件: {DB}")
    print(f"       区域: {len(AREAS)} | 道路: {len(ROADS)} | 设备: {len(DEVICE_NAMES)}")
    print(f"       车辆记录: 5000 | 小时流量: {24 * len(ROADS)} 行")
    print(f"       告警类型: 5 | 质量报告: {len(TABLE_NAMES)} 表")


def verify():
    """验证数据完整性"""
    conn = get_conn()
    cur = conn.cursor()

    print("\n[VERIFY] 数据验证:")
    tables = [
        "dim_area", "dim_road", "dim_time", "dim_device",
        "ods_vehicle_pass_di", "dwd_vehicle_pass_di", "dwd_device_status_di",
        "dws_road_hour_flow", "dws_area_hour_flow",
        "dws_device_health_day", "dws_area_jam_hour", "dws_alarm_day",
        "ads_traffic_operation", "ads_top_jam_roads",
        "ads_device_health_score", "ads_device_mtbf_mttr", "ads_device_fault_top",
        "data_quality_results",
    ]
    total = 0
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            cnt = cur.fetchone()[0]
            print(f"   {t:35s} → {cnt:>6d} 行")
            total += cnt
        except Exception as e:
            print(f"   {t:35s} → ❌ {e}")

    print(f"   {'总计':35s} → {total:>6d} 行 ({len(tables)} 张表)")
    conn.close()


if __name__ == "__main__":
    seed()
    verify()
