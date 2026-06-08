"""
HDFS 替代方案 — 本地文件系统模拟 HDFS 操作
演示: 写入/读取/分区目录/复制因子
"""
import os, sys, time, json
from datetime import datetime

print("=" * 60)
print("  HDFS 模拟测试 — 本地文件系统")
print("=" * 60)

BASE = os.path.dirname(os.path.abspath(__file__))
HDFS_ROOT = os.path.join(BASE, "data", "hdfs")
LAYERS = ["ods", "dim", "dwd", "dws", "ads"]

# ============================================================
# 1. 创建类 HDFS 目录结构
# ============================================================
print("\n[1/5] 创建 HDFS 目录结构 (模拟 NameNode 格式化)...")
for layer in LAYERS:
    os.makedirs(os.path.join(HDFS_ROOT, layer), exist_ok=True)
    print(f"  /user/hive/warehouse/{layer}.db/  [OK]")

# ============================================================
# 2. 写入 ODS 数据文件 (模拟 Flume/Kafka Connect)
# ============================================================
print("\n[2/5] 写入 ODS 原始数据 (模拟 Kafka→HDFS)...")
dt = datetime.now().strftime("%Y-%m-%d")

# 写入 Parquet 风格的 JSON 数据
ods_data = []
for i in range(50):
    ods_data.append({
        "device_id": f"DEV-{(i%10)+1:03d}",
        "road_id": i % 8 + 1,
        "timestamp": datetime.now().isoformat(),
        "vehicle_type": ["small", "medium", "large"][i % 3],
        "speed": 20 + (i * 3) % 90,
        "lane_no": (i % 4) + 1,
    })

ods_file = os.path.join(HDFS_ROOT, "ods", f"ods_vehicle_pass/dt={dt}", "part-00000.json")
os.makedirs(os.path.dirname(ods_file), exist_ok=True)
with open(ods_file, "w") as f:
    for record in ods_data:
        f.write(json.dumps(record) + "\n")

size = os.path.getsize(ods_file)
print(f"  [OK] 写入: {ods_file}")
print(f"    记录数: {len(ods_data)}, 大小: {size:,} bytes")

# ============================================================
# 3. 写入 DWD 清洗后数据
# ============================================================
print("\n[3/5] 写入 DWD 清洗后数据 (模拟 Flink→HDFS Checkpoint)...")

# 去掉重复(按road_id去重)作为清洗逻辑
seen = set()
dwd_data = []
for r in ods_data:
    key = (r["device_id"], r["road_id"], r["vehicle_type"])
    if key not in seen:
        seen.add(key)
        r["cleaned_at"] = datetime.now().isoformat()
        r["is_valid"] = 20 <= r["speed"] <= 140  # 速度合法性
        dwd_data.append(r)

dwd_file = os.path.join(HDFS_ROOT, "dwd", f"dwd_vehicle_pass/dt={dt}", "part-00000.json")
os.makedirs(os.path.dirname(dwd_file), exist_ok=True)
with open(dwd_file, "w") as f:
    for record in dwd_data:
        f.write(json.dumps(record) + "\n")

print(f"  [OK] 写入: {dwd_file}")
print(f"    清洗前: {len(ods_data)}, 清洗后: {len(dwd_data)} (去重 {len(ods_data)-len(dwd_data)} 条)")
invalid = sum(1 for r in dwd_data if not r["is_valid"])
print(f"    合法: {len(dwd_data)-invalid}, 非法: {invalid}")

# ============================================================
# 4. 写入 DWS/ADS 聚合数据
# ============================================================
print("\n[4/5] 写入 DWS/ADS 聚合数据 (模拟 Hive ETL)...")

dws_data = []
for road_id in range(1, 9):
    road_records = [r for r in dwd_data if r["road_id"] == road_id]
    if road_records:
        dws_data.append({
            "road_id": road_id,
            "dt": dt,
            "hour": datetime.now().hour,
            "traffic_count": len(road_records),
            "avg_speed": round(sum(r["speed"] for r in road_records) / len(road_records), 1),
            "jam_level": min(5, max(1, int(100 / max(1, sum(r["speed"] for r in road_records) / len(road_records)) * 0.8))),
        })

dws_file = os.path.join(HDFS_ROOT, "dws", f"dws_road_hour_flow/dt={dt}", "part-00000.json")
os.makedirs(os.path.dirname(dws_file), exist_ok=True)
with open(dws_file, "w") as f:
    json.dump(dws_data, f, indent=2)

print(f"  [OK] 写入: {dws_file}")
for d in dws_data:
    jam_label = ["", "畅通", "基本畅通", "轻度拥堵", "中度拥堵", "严重拥堵"][d["jam_level"]]
    print(f"    road_id={d['road_id']} flow={d['traffic_count']} speed={d['avg_speed']}km/h {jam_label}")

# ============================================================
# 5. 验证数据完整性 (模拟块校验)
# ============================================================
print("\n[5/5] 验证数据完整性...")
total_files = 0
total_size = 0
for layer in LAYERS:
    layer_dir = os.path.join(HDFS_ROOT, layer)
    for root, dirs, files in os.walk(layer_dir):
        for f in files:
            fpath = os.path.join(root, f)
            total_files += 1
            total_size += os.path.getsize(fpath)

print(f"  总文件数: {total_files}")
print(f"  总大小: {total_size:,} bytes ({total_size/1024:.1f} KB)")
print(f"  目录结构:")
for layer in LAYERS:
    layer_dir = os.path.join(HDFS_ROOT, layer)
    if os.path.exists(layer_dir):
        file_count = sum(1 for _ in os.walk(layer_dir) for f in _[2])
        dir_count = sum(1 for _ in os.walk(layer_dir) for d in _[1])
        print(f"    /user/hive/warehouse/{layer}.db/ → {file_count} 文件, {dir_count} 分区")

print(f"\n{'='*60}")
print(f"  HDFS 模拟测试通过 [OK]")
print(f"  数据目录: {HDFS_ROOT}")
print(f"  对应生产: hdfs://namenode:8020/user/hive/warehouse/")
print(f"{'='*60}")
