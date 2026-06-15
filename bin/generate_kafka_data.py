#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate and send large-scale traffic data to Kafka for Flink processing."""
import random, time, json, sys, os
from datetime import datetime, timedelta

random.seed(42)
BASE_DATE = "2026-06-10"
ROADS = [f"R{i:04d}" for i in range(1, 51)]  # 50 roads
DEVICES = [f"DEV{i:04d}" for i in range(1, 101)]  # 100 devices
DTYPES = ["CAMERA","SENSOR","RADAR","GATE","TRAFFIC_LIGHT"]
VEHICLE_TYPES = ["小型车", "中型车", "大型车"]
TOPICS = ["traffic_vehicle", "traffic_status", "device_status", "device_alarm"]
KAFAK_BOOTSTRAP = "localhost:9092"
TOTAL_RECORDS = 100000

def rand_ts():
    h, m, s = random.randint(0,23), random.randint(0,59), random.randint(0,59)
    return int(datetime(2026,6,10,h,m,s).timestamp() * 1000)

def fmt_ts():
    h, m, s = random.randint(0,23), random.randint(0,59), random.randint(0,59)
    return f"{BASE_DATE} {h:02d}:{m:02d}:{s:02d}"

def gen_vehicle(count):
    rows = []
    for i in range(count):
        rows.append(f"{random.randint(1,10000)}\t{random.choice(ROADS)}\t{random.choice(DEVICES)}\t{fmt_ts()}\t{random.randint(10,140)}\t{random.choice(['N','S','E','W'])}\t京{chr(random.randint(65,90))}{random.randint(10000,99999)}\t{random.choice(VEHICLE_TYPES)}\t{random.randint(1,4)}")
    return rows

def gen_traffic_status(count):
    rows = []
    for _ in range(count):
        spd = random.randint(15,80)
        flow = random.randint(50,400)
        jam = 1 if spd>60 else (2 if spd>40 else (3 if spd>25 else (4 if spd>15 else 5)))
        rows.append(f"{random.choice(ROADS)}\t{spd}\t{flow}\t{jam}\t{round(random.uniform(0,jam*18),2)}\t{random.choice(['PEAK_HOUR','NORMAL','OFF_PEAK'])}\t{fmt_ts()}")
    return rows

def gen_device_status(count):
    rows = []
    for _ in range(count):
        rows.append(f"{random.choice(DEVICES)}\t{round(random.uniform(10,95),2)}\t{round(random.uniform(20,90),2)}\t{round(random.uniform(30,85),1)}\t{'ONLINE' if random.random()>0.05 else 'OFFLINE'}\t{fmt_ts()}\t{random.randint(-100,-40)}\t{random.choice(DTYPES)}")
    return rows

# Generate data to files first (for Kafka shell producer)
os.makedirs("/tmp/kafka_data", exist_ok=True)

print(f"Generating {TOTAL_RECORDS:,} records...")
data = gen_vehicle(TOTAL_RECORDS)
with open("/tmp/kafka_data/traffic_vehicle.txt", "w") as f:
    f.write("\n".join(data))
print(f"  traffic_vehicle: {len(data):,} rows")

data2 = gen_traffic_status(50000)
with open("/tmp/kafka_data/traffic_status.txt", "w") as f:
    f.write("\n".join(data2))
print(f"  traffic_status: {len(data2):,} rows")

data3 = gen_device_status(100000)
with open("/tmp/kafka_data/device_status.txt", "w") as f:
    f.write("\n".join(data3))
print(f"  device_status: {len(data3):,} rows")

print(f"\nTotal: {len(data)+len(data2)+len(data3):,} records")
print(f"Data written to /tmp/kafka_data/")
