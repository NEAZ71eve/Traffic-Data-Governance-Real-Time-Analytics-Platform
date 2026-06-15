#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate ODS test data for Hive - TAB separated files"""
import random, os, sys
from datetime import datetime, timedelta

random.seed(42)

# Base date
BASE_DATE = "2026-06-10"
OUT_DIR = "/tmp/hive_etl_data"

# Road config
ROADS = [(i, f"ROAD{i:04d}", random.choice(["高速","主干道","次干道","支路"])) for i in range(1, 21)]
DEVICES = [(i, f"DEV{i:04d}", random.choice(["CAMERA","RADAR","SENSOR","GATE"])) for i in range(1, 31)]
AREAS = [(i, f"AREA{i:04d}") for i in range(1, 6)]
VEHICLE_TYPES = ["小型车", "中型车", "大型车"]

def gen_vehicle_pass(count=500):
    """Generate vehicle pass records"""
    rows = []
    for i in range(count):
        rid = random.choice(ROADS)[1]
        did = random.choice(DEVICES)[1]
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        pass_time = f"{BASE_DATE} {hour:02d}:{minute:02d}:{second:02d}"
        speed = random.randint(10, 140)
        direction = random.choice(["N","S","E","W"])
        plate = f"京{chr(random.randint(65,90))}{random.randint(10000,99999)}"
        vtype = random.choice(VEHICLE_TYPES)
        lane = random.randint(1, 4)
        rows.append(f"{random.randint(1,10000)}\t{rid}\t{did}\t{pass_time}\t{speed}\t{direction}\t{plate}\t{vtype}\t{lane}")
    return "\n".join(rows)

def gen_traffic_status(count=240):
    """Generate traffic status records"""
    rows = []
    for i in range(count):
        rid = random.choice(ROADS)[1]
        hour = random.randint(6, 22)
        minute = random.randint(0, 59)
        ts = f"{BASE_DATE} {hour:02d}:{minute:02d}:00"
        avg_speed = round(random.uniform(15, 80), 1)
        flow = random.randint(50, 400)
        jam_level = 1 if avg_speed > 60 else (2 if avg_speed > 40 else (3 if avg_speed > 25 else (4 if avg_speed > 15 else 5)))
        rows.append(f"{rid}\t{ts}\t{avg_speed}\t{flow}\t{jam_level}")
    return "\n".join(rows)

def gen_device_status(count=720):
    """Generate device status records"""
    rows = []
    for i in range(count):
        did = random.choice(DEVICES)[1]
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        ts = f"{BASE_DATE} {hour:02d}:{minute:02d}:00"
        cpu = random.randint(10, 95)
        memory = random.randint(20, 90)
        temperature = round(random.uniform(30, 85), 1)
        online = 1 if random.random() > 0.05 else 0
        rows.append(f"{did}\t{ts}\t{cpu}\t{memory}\t{temperature}\t{online}")
    return "\n".join(rows)

def gen_alarm_log(count=30):
    """Generate alarm log records"""
    alarm_types = ["OFFLINE", "CPU_OVERLOAD", "TEMP_HIGH", "SPEED_ANOMALY", "FLOW_SURGE"]
    rows = []
    for i in range(count):
        did = random.choice(DEVICES)[1]
        hour = random.randint(6, 23)
        minute = random.randint(0, 59)
        ts = f"{BASE_DATE} {hour:02d}:{minute:02d}:00"
        atype = random.choice(alarm_types)
        level = random.choices(["CRITICAL","MAJOR","MINOR"], weights=[1,3,6])[0]
        duration = random.randint(5, 120)
        rows.append(f"{i+1}\t{did}\t{atype}\t{ts}\t{level}\t{duration}")
    return "\n".join(rows)

def gen_road_info():
    """Generate road dimension data"""
    rows = []
    for i, rid, rtype in ROADS:
        length = random.randint(1, 20)
        lanes = random.randint(2, 8)
        limit_speed = random.choice([60, 80, 100, 120])
        area_id = random.randint(1, 5)
        rows.append(f"{rid}\t{i}\t{rid}\t{rtype}\t{length}\t{lanes}\t{limit_speed}\t{area_id}")
    return "\n".join(rows)

def gen_device_info():
    """Generate device dimension data"""
    rows = []
    for i, did, dtype in DEVICES:
        install = f"2026-0{random.randint(1,6):02d}-{random.randint(1,28):02d}"
        lat = round(random.uniform(39.8, 40.2), 4)
        lng = round(random.uniform(116.1, 116.6), 4)
        rows.append(f"{did}\t{did}\t{dtype}\t{install}\t{lat}\t{lng}")
    return "\n".join(rows)

def gen_area_info():
    """Generate area dimension data"""
    areas = [
        (1, "朝阳区", "北京"),
        (2, "海淀区", "北京"),
        (3, "东城区", "北京"),
        (4, "西城区", "北京"),
        (5, "丰台区", "北京"),
    ]
    return "\n".join(f"{a}\t{n}\t{c}" for a, n, c in areas)

# Main
os.makedirs(OUT_DIR, exist_ok=True)
DT = BASE_DATE

data = {
    "ods_vehicle_pass_di": gen_vehicle_pass(500),
    "ods_traffic_status_di": gen_traffic_status(240),
    "ods_device_status_di": gen_device_status(720),
    "ods_alarm_log_di": gen_alarm_log(30),
    "ods_road_info": gen_road_info(),
    "ods_device_info": gen_device_info(),
    "ods_area_info": gen_area_info(),
}

for name, content in data.items():
    path = os.path.join(OUT_DIR, f"{name}.tsv")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    rows = len(content.split("\n"))
    print(f"  {name}: {rows} rows → {path}")

print(f"\nTotal: {sum(len(c.split('\\n')) for c in data.values())} rows across {len(data)} tables")
