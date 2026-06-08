"""
Kafka 真实消息生产/消费测试
前提: WSL 中 Kafka 已启动 (python start_all.py)
"""
import sys, time, json, random
from datetime import datetime

print("=" * 60)
print("  Kafka 消息测试 — 模拟 ODS 数据采集")
print("=" * 60)

TOPICS = ["ods_vehicle_pass", "ods_device_status", "ods_traffic_status"]
BOOTSTRAP = "localhost:9092"

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import KafkaError, NoBrokersAvailable
except ImportError:
    print("\n[FAIL] kafka-python 未安装")
    print("  运行: pip install kafka-python")
    sys.exit(1)

# ============================================================
# 1. 检查连接
# ============================================================
print("\n[1/5] 检查 Kafka 连接...")
try:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=5000)
    print(f"  [OK] Kafka 连接成功, broker: {admin._client.cluster.brokers()}")
except NoBrokersAvailable:
    print(f"\n  [FAIL] 无法连接 Kafka ({BOOTSTRAP})")
    print("  请确认: cd pseudo_distributed && python start_all.py")
    sys.exit(1)

# ============================================================
# 2. 创建 Topics
# ============================================================
print("\n[2/5] 创建 Topics...")
existing = set(admin.list_topics())
for t in TOPICS:
    if t in existing:
        print(f"  [SKIP] Topic '{t}' 已存在")
    else:
        try:
            admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1)])
            print(f"  [OK] Topic '{t}' 创建成功 (3 partitions)")
        except Exception as e:
            print(f"  [WARN] {e}")

# ============================================================
# 3. 生产消息 — 模拟传感器数据
# ============================================================
print("\n[3/5] 生产测试消息...")
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    acks=1,
)

ROADS = ["京藏高速", "北四环路", "长安街", "二环路", "京通快速", "三环路", "京承高速", "五环路"]
DEVICE_TYPES = ["摄像头", "传感器", "红绿灯控制器", "雷达测速"]

# 发 20 条车辆通行消息
for i in range(20):
    msg = {
        "device_id": f"DEV-{random.randint(1,10):03d}",
        "road_id": random.randint(1, 8),
        "road_name": random.choice(ROADS),
        "timestamp": datetime.now().isoformat(),
        "vehicle_type": random.choice(["small", "medium", "large"]),
        "speed": round(random.uniform(20, 120), 1),
        "lane_no": random.randint(1, 4),
    }
    producer.send("ods_vehicle_pass", value=msg)
print(f"  [OK] 发送 20 条 → ods_vehicle_pass")

# 发 10 条设备状态消息
for i in range(10):
    msg = {
        "device_id": f"DEV-{random.randint(1,10):03d}",
        "device_type": random.choice(DEVICE_TYPES),
        "timestamp": datetime.now().isoformat(),
        "status": random.choice([0, 1, 1, 1]),  # 75% online
        "cpu_usage": round(random.uniform(10, 95), 1),
        "memory_usage": round(random.uniform(20, 90), 1),
        "temperature": round(random.uniform(30, 85), 1),
    }
    producer.send("ods_device_status", value=msg)
print(f"  [OK] 发送 10 条 → ods_device_status")

producer.flush()
producer.close()

# ============================================================
# 4. 消费消息 — 模拟 Flink 消费
# ============================================================
print("\n[4/5] 消费消息 (模拟 Flink Consumer)...")
consumer = KafkaConsumer(
    "ods_vehicle_pass",
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    group_id="test-group-vehicle",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=5000,
)

vehicle_msgs = []
print("  等待消息 (5s timeout)...")
for msg in consumer:
    vehicle_msgs.append(msg.value)
    print(f"    ← offset={msg.offset} partition={msg.partition} road={msg.value['road_name']} speed={msg.value['speed']}km/h 车辆={msg.value['vehicle_type']}")

consumer.close()
print(f"  [OK] 消费到 {len(vehicle_msgs)} 条车辆通行消息 (预期 ≥20)")

# 也消费设备消息
consumer2 = KafkaConsumer(
    "ods_device_status",
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    group_id="test-group-device",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=3000,
)
device_msgs = []
for msg in consumer2:
    device_msgs.append(msg.value)
    status = "在线" if msg.value["status"] == 1 else "离线"
    print(f"    ← device={msg.value['device_id']} status={status} cpu={msg.value['cpu_usage']}%")
consumer2.close()
print(f"  [OK] 消费到 {len(device_msgs)} 条设备状态消息 (预期 ≥10)")

# ============================================================
# 5. 验证结果
# ============================================================
print("\n[5/5] 验证结果")
passed = True

if len(vehicle_msgs) >= 20:
    print(f"  [PASS] 车辆消息生产/消费: {len(vehicle_msgs)}/20")
else:
    print(f"  [FAIL] 车辆消息数量不足: {len(vehicle_msgs)}/20")
    passed = False

if len(device_msgs) >= 10:
    print(f"  [PASS] 设备消息生产/消费: {len(device_msgs)}/10")
else:
    print(f"  [FAIL] 设备消息数量不足: {len(device_msgs)}/10")
    passed = False

# 验证消息格式
sample = vehicle_msgs[0] if vehicle_msgs else {}
for field in ["device_id", "road_name", "speed", "vehicle_type"]:
    if field in sample:
        print(f"  [PASS] 字段 '{field}' 存在: {sample[field]}")
    else:
        print(f"  [FAIL] 字段 '{field}' 缺失")

# Cleanup
admin.delete_topics(TOPICS)
admin.close()

print(f"\n{'='*60}")
if passed:
    print("  Kafka 测试全部通过! [OK]")
    print(f"  → 可继续运行 python test_pipeline.py 进行全链路测试")
else:
    print("  部分测试失败，请检查 Kafka 服务状态")
print("=" * 60)
