#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate ODS test data TSV files matching Hive DDL column counts."""
import random, os
random.seed(42)
BASE_DATE = "2026-06-10"
OUT = "tmp_hive_data"
ROADS = [f"R{i:04d}" for i in range(1, 21)]
DEVICES = [f"DEV{i:04d}" for i in range(1, 31)]
DTYPES = ["CAMERA","SENSOR","RADAR","GATE","TRAFFIC_LIGHT"]

def rand_ts(h0=0, h1=23):
    return f"{BASE_DATE} {random.randint(h0,h1):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"

def null():
    return chr(92) + "N"  # Hive NULL representation: \N

os.makedirs(OUT, exist_ok=True)

# vehicle_pass: 9 cols
rows = []
for _ in range(500):
    rows.append(f"{random.randint(1,10000)}\t{random.choice(ROADS)}\t{random.choice(DEVICES)}\t{rand_ts()}\t{random.randint(10,140)}\t{random.choice(['N','S','E','W'])}\t京{chr(random.randint(65,90))}{random.randint(10000,99999)}\t{random.choice(['小型车','中型车','大型车'])}\t{random.randint(1,4)}")
open(f"{OUT}/ods_vehicle_pass_di.tsv","w").write("\n".join(rows))

# traffic_status: 7 cols
rows = []
for _ in range(240):
    spd = random.randint(15, 80)
    flow = random.randint(50, 400)
    jam = 1 if spd > 60 else (2 if spd > 40 else (3 if spd > 25 else (4 if spd > 15 else 5)))
    rate = round(random.uniform(0, jam * 18), 2)
    rows.append(f"{random.choice(ROADS)}\t{spd}\t{flow}\t{jam}\t{rate}\t{random.choice(['PEAK_HOUR','NORMAL','OFF_PEAK'])}\t{rand_ts(6,22)}")
open(f"{OUT}/ods_traffic_status_di.tsv","w").write("\n".join(rows))

# device_status: 8 cols
rows = []
for _ in range(720):
    rows.append(f"{random.choice(DEVICES)}\t{round(random.uniform(10,95),2)}\t{round(random.uniform(20,90),2)}\t{round(random.uniform(30,85),1)}\t{'ONLINE' if random.random()>0.05 else 'OFFLINE'}\t{rand_ts()}\t{random.randint(-100,-40)}\t{random.choice(DTYPES)}")
open(f"{OUT}/ods_device_status_di.tsv","w").write("\n".join(rows))

# alarm_log: 8 cols
rows = []
for i in range(30):
    dev = random.choice(DEVICES)
    at = random.choice(["OFFLINE","CPU_HIGH","MEMORY_HIGH","TEMP_HIGH","SIGNAL_WEAK","HARDWARE_FAULT"])
    al = random.choices(["CRITICAL","MAJOR","MINOR","WARNING"], weights=[1,3,6,2])[0]
    ts = rand_ts(6, 23)
    rs = random.choice(["RECOVERED","RECOVERED","RECOVERED","UNRECOVERED"])
    rt_val = rand_ts(6,23) if rs == "RECOVERED" else null()
    rows.append(f"{i+1}\t{dev}\t{at}\t{al}\t{dev} {at} at {ts}\t{ts}\t{rt_val}\t{rs}")
open(f"{OUT}/ods_alarm_log_di.tsv","w").write("\n".join(rows))

# road_info: 10 cols
rows = [f"R{i:04d}\tROAD{i:04d}\t{random.choice(['主干道','次干道','支路','快速路','高速'])}\t{random.randint(1,20)}.00\t{random.randint(2,8)}\t{random.choice([60,80,100,120])}\t{random.randint(1,5)}\t{random.choice(['N','S','E','W','BIDIRECTIONAL'])}\t{random.choice(['ACTIVE','ACTIVE','ACTIVE','MAINTENANCE'])}\t{rand_ts()}" for i in range(1,21)]
open(f"{OUT}/ods_road_info.tsv","w").write("\n".join(rows))

# device_info: 12 cols
rows = []
for i in range(1, 31):
    rows.append(f"DEV{i:04d}\tDEV{i:04d}\t{random.choice(DTYPES)}\tV{random.randint(1,5)}.{random.randint(0,9)}\t{random.randint(1,20)}\t{random.randint(1,5)}\t2026-0{random.randint(1,6):02d}-{random.randint(1,28):02d}\t{random.choice(['海康','大华','华为','宇视'])}\tv{random.randint(1,4)}.{random.randint(0,9)}.{random.randint(0,9)}\t192.168.{random.randint(0,255)}.{random.randint(1,254)}\t{random.choice(['RUNNING','RUNNING','RUNNING','MAINTENANCE'])}\t{rand_ts()}")
open(f"{OUT}/ods_device_info.tsv","w").write("\n".join(rows))

# area_info: 10 cols
areas = [(1,"朝阳区","110105",1,"北京",1,"北京","DISTRICT","URBAN"),(2,"海淀区","110108",1,"北京",1,"北京","DISTRICT","URBAN"),(3,"东城区","110101",1,"北京",1,"北京","DISTRICT","URBAN"),(4,"西城区","110102",1,"北京",1,"北京","DISTRICT","URBAN"),(5,"丰台区","110106",1,"北京",1,"北京","DISTRICT","SUBURBAN")]
rows = [f"{a}\t{n}\t{ac}\t{ci}\t{cn}\t{pi}\t{pn}\t{nal}\t{at}\t{rand_ts()}" for a,n,ac,ci,cn,pi,pn,nal,at in areas]
open(f"{OUT}/ods_area_info.tsv","w").write("\n".join(rows))

# Verify
expected = {"ods_vehicle_pass_di":9,"ods_traffic_status_di":7,"ods_device_status_di":8,"ods_alarm_log_di":8,"ods_road_info":10,"ods_device_info":12,"ods_area_info":10}
all_ok = True
for name, exp in expected.items():
    cols = len(open(f"{OUT}/{name}.tsv").readline().strip().split("\t"))
    status = "OK" if cols == exp else f"FAIL(expected {exp}, got {cols})"
    if status != "OK": all_ok = False
    print(f"  {name}: {cols} cols [{status}]")
print(f"\nAll OK: {all_ok}")
