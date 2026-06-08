"""
Redis 真实读写测试 — 模拟 Flink → Redis 实时指标存储
前提: WSL 中 Redis 已启动 (python start_all.py)
"""
import sys, json, random, time

print("=" * 60)
print("  Redis 测试 — 模拟 实时指标 读写")
print("=" * 60)

try:
    import redis
except ImportError:
    print("\n[FAIL] redis-py 未安装")
    print("  运行: pip install redis")
    sys.exit(1)

# ============================================================
# 1. 连接测试
# ============================================================
print("\n[1/6] 连接 Redis...")
try:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True, socket_connect_timeout=5)
    r.ping()
    info = r.info("server")
    print(f"  [OK] Redis v{info['redis_version']} 连接成功")
    print(f"  内存: {info['used_memory_human']}, 运行时间: {info['uptime_in_seconds']}s")
    print(f"  键数量: {r.dbsize()}")
except redis.ConnectionError:
    print(f"\n  [FAIL] 无法连接 Redis (localhost:6379)")
    print("  请确认: cd pseudo_distributed && python start_all.py")
    sys.exit(1)

# ============================================================
# 2. 写入实时指标 (模拟 Flink Sink)
# ============================================================
print("\n[2/6] 写入实时交通指标 (模拟 Flink → Redis Sink)...")
ROADS = ["京藏高速", "北四环路", "长安街", "二环路", "京通快速", "三环路"]

pipeline = r.pipeline()
for i, road in enumerate(ROADS):
    flow = random.randint(100, 2000)
    speed = round(random.uniform(20, 90), 1)
    jam = random.randint(1, 5)

    pipeline.hset(f"traffic:road:{i+1}", mapping={
        "name": road,
        "flow": flow,
        "speed": str(speed),
        "jam_level": jam,
        "update_time": time.strftime("%H:%M:%S"),
    })

# 写入汇总指标
pipeline.set("traffic:total_flow", sum([random.randint(100, 2000) for _ in range(6)]))
pipeline.set("traffic:avg_speed", round(random.uniform(30, 70), 1))
pipeline.set("traffic:congestion_count", random.randint(0, 3))
pipeline.set("traffic:last_update", time.strftime("%Y-%m-%d %H:%M:%S"))
pipeline.execute()
print("  [OK] 写入 6 条道路指标 + 4 个汇总指标")

# ============================================================
# 3. 写入设备状态
# ============================================================
print("\n[3/6] 写入设备状态...")
DEVICES = ["DEV-001", "DEV-002", "DEV-003", "DEV-004", "DEV-005"]
pipeline = r.pipeline()
for dev in DEVICES:
    pipeline.hset(f"device:{dev}", mapping={
        "online": random.choice(["0", "1", "1", "1"]),  # 75% online
        "cpu": str(round(random.uniform(10, 90), 1)),
        "memory": str(round(random.uniform(20, 80), 1)),
        "health_score": str(random.randint(60, 100)),
        "last_heartbeat": time.strftime("%H:%M:%S"),
    })
pipeline.set("device:online_count", random.randint(3, 5))
pipeline.set("device:total_count", 5)
pipeline.execute()
print("  [OK] 写入 5 台设备状态")

# ============================================================
# 4. 读取验证
# ============================================================
print("\n[4/6] 读取实时指标...")
total_flow = r.get("traffic:total_flow")
avg_speed = r.get("traffic:avg_speed")
print(f"  总车流量: {total_flow}")
print(f"  平均车速: {avg_speed} km/h")
print(f"  在线设备: {r.get('device:online_count')}/{r.get('device:total_count')}")

# ============================================================
# 5. 读取道路详情
# ============================================================
print("\n[5/6] 读取各道路详情...")
for i in range(1, 7):
    data = r.hgetall(f"traffic:road:{i}")
    if data:
        jam_label = ["", "畅通", "基本畅通", "轻度拥堵", "中度拥堵", "严重拥堵"][int(data.get("jam_level", 1))]
        print(f"  {data['name']:8s} | 车流:{data['flow']:>5s} | 车速:{data['speed']:>5s}km/h | {jam_label}")

# ============================================================
# 6. Pub/Sub 测试 (模拟实时推送)
# ============================================================
print("\n[6/6] Pub/Sub 实时推送测试...")
pubsub = r.pubsub()
pubsub.subscribe("traffic:alert", "device:alert")

# 发布一条告警
r.publish("traffic:alert", json.dumps({
    "type": "congestion",
    "road": "长安街",
    "level": 4,
    "message": "长安街严重拥堵! 车速降至 15km/h",
    "timestamp": time.strftime("%H:%M:%S"),
}))
print("  [OK] 发布告警 → traffic:alert")
print("  [OK] 发布告警 → device:alert")

# 接收告警
time.sleep(0.5)
msg = pubsub.get_message(timeout=2)
while msg:
    if msg["type"] == "message":
        data = json.loads(msg["data"])
        print(f"  ← 收到 {msg['channel']}: {data['message']}")
    msg = pubsub.get_message(timeout=1)

pubsub.close()

# ============================================================
# Cleanup
# ============================================================
print("\n清理测试数据...")
keys = r.keys("traffic:*") + r.keys("device:*")
if keys:
    r.delete(*keys)

print(f"\n{'='*60}")
print("  Redis 测试全部通过! [OK]")
print("=" * 60)
