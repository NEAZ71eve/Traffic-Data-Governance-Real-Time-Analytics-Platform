import sqlite3
import os
import random
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "traffic_data.db")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 创建维度表
cur.execute('''
CREATE TABLE IF NOT EXISTS dim_road (
    road_id INTEGER PRIMARY KEY,
    road_name TEXT,
    road_type TEXT,
    limit_speed INTEGER,
    length_km REAL
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS dim_device (
    device_id TEXT PRIMARY KEY,
    device_name TEXT,
    device_type TEXT,
    road_id INTEGER,
    install_date TEXT
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS dim_area (
    area_id TEXT PRIMARY KEY,
    area_name TEXT,
    level INTEGER
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS dim_time (
    hour INTEGER PRIMARY KEY,
    period TEXT
)
''')

# 创建DWS层表
cur.execute('''
CREATE TABLE IF NOT EXISTS dws_road_hour_flow (
    road_id INTEGER,
    dt TEXT,
    hour INTEGER,
    traffic_count INTEGER,
    avg_speed REAL,
    jam_level INTEGER,
    small_car_cnt INTEGER,
    medium_car_cnt INTEGER,
    large_car_cnt INTEGER,
    PRIMARY KEY (road_id, dt, hour)
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS dws_device_health_day (
    device_id TEXT,
    dt TEXT,
    health_score REAL,
    online_rate REAL,
    avg_cpu_usage REAL,
    avg_mem_usage REAL,
    PRIMARY KEY (device_id, dt)
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS dws_area_jam_hour (
    area_id TEXT,
    dt TEXT,
    hour INTEGER,
    jam_level INTEGER,
    PRIMARY KEY (area_id, dt, hour)
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS dws_alarm_day (
    alarm_type TEXT,
    dt TEXT,
    count INTEGER,
    PRIMARY KEY (alarm_type, dt)
)
''')

# 创建ADS层表
cur.execute('''
CREATE TABLE IF NOT EXISTS ads_traffic_operation (
    dt TEXT,
    area_id TEXT,
    total_traffic_flow INTEGER,
    avg_congestion_rate REAL,
    PRIMARY KEY (dt, area_id)
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS ads_top_jam_roads (
    dt TEXT,
    rank_num INTEGER,
    road_name TEXT,
    avg_jam_level REAL,
    PRIMARY KEY (dt, rank_num)
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS ads_device_health_score (
    dt TEXT,
    device_name TEXT,
    health_score REAL,
    health_level TEXT,
    online_rate REAL,
    avg_cpu_usage REAL,
    avg_mem_usage REAL,
    PRIMARY KEY (dt, device_name)
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS ads_device_mtbf_mttr (
    dt TEXT,
    device_name TEXT,
    mtbf_hours REAL,
    mttr_minutes REAL,
    PRIMARY KEY (dt, device_name)
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS ads_device_fault_top (
    dt TEXT,
    rank_num INTEGER,
    device_name TEXT,
    fault_rate REAL,
    PRIMARY KEY (dt, rank_num)
)
''')

# 创建数据质量表
cur.execute('''
CREATE TABLE IF NOT EXISTS data_quality_results (
    report_date TEXT,
    table_name TEXT,
    completeness_rate REAL,
    uniqueness_rate REAL,
    validity_rate REAL,
    kafka_lag INTEGER,
    status TEXT,
    score REAL,
    PRIMARY KEY (report_date, table_name)
)
''')

# 插入测试数据
roads = [('京藏高速', '高速', 120, 30), ('北四环路', '快速路', 80, 28), ('长安街', '主干道', 60, 15), 
         ('二环路', '环路', 60, 32), ('京通快速', '快速路', 80, 18), ('三环路', '环路', 80, 48), 
         ('京承高速', '高速', 120, 25), ('五环路', '环路', 100, 98)]
for i, (name, rtype, speed, length) in enumerate(roads, 1):
    cur.execute('INSERT OR IGNORE INTO dim_road VALUES (?, ?, ?, ?, ?)', (i, name, rtype, speed, length))

devices = ['DEV001','DEV002','DEV003','DEV004','DEV005','DEV006','DEV007','DEV008','DEV009','DEV010']
for i, dev in enumerate(devices, 1):
    cur.execute('INSERT OR IGNORE INTO dim_device VALUES (?, ?, ?, ?, ?)', (dev, f'设备{i}', '传感器', (i%8)+1, '2026-01-01'))

areas = [('A001','城区',1), ('A002','郊区',2), ('A003','开发区',2)]
for aid, name, level in areas:
    cur.execute('INSERT OR IGNORE INTO dim_area VALUES (?, ?, ?)', (aid, name, level))

for h in range(24):
    period = '凌晨' if h < 6 else '早高峰' if 6 <= h < 10 else '平峰' if 10 <= h < 17 else '晚高峰' if 17 <= h < 21 else '夜间'
    cur.execute('INSERT OR IGNORE INTO dim_time VALUES (?, ?)', (h, period))

# 生成7天的测试数据
for day_offset in range(7):
    dt = (datetime.now() - timedelta(days=day_offset)).strftime('%Y-%m-%d')
    for road_id in range(1, 9):
        for hour in range(24):
            traffic = random.randint(100, 2000)
            speed = random.uniform(20, 120)
            jam_level = 1 if speed > 60 else (2 if speed > 40 else 3 if speed > 25 else 4)
            cur.execute('INSERT OR IGNORE INTO dws_road_hour_flow VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                       (road_id, dt, hour, traffic, round(speed,1), jam_level, 
                        random.randint(0, traffic), random.randint(0, int(traffic*0.3)), random.randint(0, int(traffic*0.1))))

    for device_id in devices:
        health = random.uniform(50, 100)
        level = '优秀' if health >= 80 else '良好' if health >= 60 else '较差'
        cur.execute('INSERT OR IGNORE INTO dws_device_health_day VALUES (?, ?, ?, ?, ?, ?)',
                   (device_id, dt, round(health,1), random.uniform(70, 100), 
                    random.uniform(20, 80), random.uniform(30, 85)))

        cur.execute('INSERT OR IGNORE INTO ads_device_health_score VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (dt, device_id, round(health,1), level, random.uniform(70, 100), 
                    random.uniform(20, 80), random.uniform(30, 85)))

        cur.execute('INSERT OR IGNORE INTO ads_device_mtbf_mttr VALUES (?, ?, ?, ?)',
                   (dt, device_id, random.uniform(480, 1440), random.uniform(0, 120)))

    for rank in range(1, 11):
        cur.execute('INSERT OR IGNORE INTO ads_top_jam_roads VALUES (?, ?, ?, ?)',
                   (dt, rank, roads[(rank-1)%8][0], round(random.uniform(2, 5), 1)))

    for area in areas:
        cur.execute('INSERT OR IGNORE INTO ads_traffic_operation VALUES (?, ?, ?, ?)',
                   (dt, area[0], random.randint(5000, 50000), round(random.uniform(10, 80), 1)))

    for rank in range(1, 6):
        cur.execute('INSERT OR IGNORE INTO ads_device_fault_top VALUES (?, ?, ?, ?)',
                   (dt, rank, devices[rank-1], round(random.uniform(10, 50), 1)))

    cur.execute('INSERT OR IGNORE INTO data_quality_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
               (dt, 'all', 100.0, 100.0, 99.8, random.randint(100, 5000), 'PASS', 99.9))

conn.commit()
conn.close()
print(f'Database created successfully: {DB_PATH}')
