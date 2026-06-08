#!/usr/bin/env python3
"""
容器启动后初始化 — 创建 Kafka Topics + 验证 Redis + 检查 DB
"""
import json, os, sys, time, sqlite3

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
DB_PATH = os.environ.get("DB_PATH", "/app/data/traffic_data.db")
MAX_RETRIES = 30

def log(msg): print(f"[init] {msg}")

# ============================================================
# 1. 等待 Kafka
# ============================================================
log("等待 Kafka 就绪...")
for i in range(MAX_RETRIES):
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=5000)
        log(f"Kafka 已就绪 (brokers: {admin._client.cluster.brokers()})")
        break
    except Exception as e:
        if i % 5 == 0: log(f"  重试 {i+1}/{MAX_RETRIES}... ({e})")
        time.sleep(3)
else:
    log("WARN: Kafka 超时，跳过 topic 创建")
    admin = None

# 创建 Topics
if admin:
    from kafka.admin import NewTopic

    TOPICS_CONFIG = {
        "traffic_vehicle":   (8, "车辆通行数据"),
        "traffic_status":    (4, "路况监测数据"),
        "device_status":     (4, "设备状态数据"),
        "device_alarm":      (4, "故障告警数据"),
    }

    existing = set(admin.list_topics())
    for name, (partitions, desc) in TOPICS_CONFIG.items():
        if name in existing:
            log(f"  [SKIP] Topic '{name}' 已存在")
        else:
            try:
                admin.create_topics([NewTopic(name, partitions, 1)])
                log(f"  [OK] Topic '{name}' ({partitions}p) → {desc}")
            except Exception as e:
                log(f"  [FAIL] Topic '{name}': {e}")
    admin.close()

# ============================================================
# 2. 等待 Redis
# ============================================================
log("等待 Redis 就绪...")
for i in range(MAX_RETRIES):
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=6379, socket_connect_timeout=3)
        r.ping()
        log(f"Redis 已就绪 (v{r.info('server')['redis_version']})")
        break
    except Exception:
        if i % 5 == 0: log(f"  重试 {i+1}/{MAX_RETRIES}...")
        time.sleep(2)
else:
    log("WARN: Redis 超时")

# ============================================================
# 3. 检查 SQLite 数据库
# ============================================================
log("检查数据库...")
if os.path.exists(DB_PATH):
    size = os.path.getsize(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    log(f"数据库: {DB_PATH} ({size/1024/1024:.1f}MB, {len(tables)} 表)")
    for t in tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM [{t[0]}]").fetchone()[0]
        log(f"  {t[0]:<30s} {cnt:>6,d} 行")
    conn.close()
else:
    log("WARN: 数据库未挂载，请将 traffic_data.db 挂载到容器 /app/data/traffic_data.db")

log("初始化完成!")
