#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生产环境实时测试 — 全链路模拟数据生成器
同时向 Kafka 4个Topic + Redis 推送实时交通数据
让 Kafka UI / RedisInsight / Flink UI / Flask大屏 全部可见实时运行效果

用法: python init/production_test.py
"""
import json, os, random, sys, time, threading
from datetime import datetime

# ============================================================
# 数据池
# ============================================================
ROADS = [
    "长安街", "东三环路", "西二环路", "人民路", "解放路", "建设路",
    "中山路", "深南大道", "天府大道", "南京路", "北京路", "滨海大道",
    "环城路", "迎宾路", "学府路", "科技路", "工业大道", "东风路",
    "京藏高速", "北四环路", "京通快速", "三环路", "京承高速", "五环路",
]

DEVICES = [f"{pfx}-{i:04d}" for pfx in ["CAM", "SEN", "RSU", "VMS", "CT"] for i in range(1, 9)]

VEHICLE_TYPES = ["small", "medium", "large", "motorcycle"]
PLATE_PREFIXES = ["京", "津", "沪", "渝", "冀", "豫", "辽", "苏", "浙", "粤"]
DEVICE_TYPES = ["摄像头", "信号灯", "流量计", "雷达", "诱导屏"]

ALARM_DEFS = [
    ("CRITICAL", "设备离线超过2h"),
    ("CRITICAL", "CPU使用率>95%"),
    ("MAJOR", "车流量突增>50%"),
    ("MAJOR", "温度超过80C"),
    ("MAJOR", "Kafka Lag超过5000"),
    ("MINOR", "车速<15km/h"),
    ("MINOR", "拥堵等级5持续>30min"),
    ("WARNING", "Checkpoint耗时>10s"),
]

# ============================================================
# 生成函数
# ============================================================
def generate_plate():
    prefix = random.choice(PLATE_PREFIXES)
    letter = random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ")
    digits = "".join(str(random.randint(0, 9)) for _ in range(5))
    return f"{prefix}{letter}{digits}"

def generate_vehicle_pass():
    road = random.choice(ROADS)
    hour = datetime.now().hour
    is_peak = (7 <= hour <= 9) or (17 <= hour <= 19)
    speed = round(random.gauss(30 if is_peak else 65, 20), 1)
    speed = max(5, min(150, speed))
    jam = 1 if speed > 60 else (2 if speed > 40 else (3 if speed > 25 else (4 if speed > 15 else 5)))
    return {
        "ts": datetime.now().isoformat(),
        "road": road,
        "plate": generate_plate(),
        "speed_kmh": speed,
        "jam_level": jam,
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "lane_no": random.randint(1, 4),
    }

def generate_traffic_status():
    hour = datetime.now().hour
    is_peak = (7 <= hour <= 9) or (17 <= hour <= 19)
    flow = random.randint(300, 1500) if is_peak else random.randint(50, 500)
    return {
        "ts": datetime.now().isoformat(),
        "road_id": f"RD-{random.randint(10000, 99999)}",
        "road_name": random.choice(ROADS),
        "avg_speed": round(random.gauss(25 if is_peak else 55, 15), 1),
        "traffic_flow": flow,
        "jam_level": random.randint(3, 5) if is_peak else random.randint(1, 3),
        "congestion_index": round(random.uniform(0, 10), 2),
    }

def generate_device_status():
    online = random.random() > 0.05
    return {
        "ts": datetime.now().isoformat(),
        "device_id": random.choice(DEVICES),
        "device_type": random.choice(DEVICE_TYPES),
        "status": 1 if online else 0,
        "cpu_usage": round(random.gauss(45, 20) if online else random.gauss(5, 5), 1),
        "memory_usage": round(random.gauss(55, 18), 1),
        "temperature": round(random.gauss(42, 15), 1),
    }

def generate_alarm():
    severity, message = random.choice(ALARM_DEFS)
    return {
        "ts": datetime.now().isoformat(),
        "severity": severity,
        "message": message,
        "device_id": random.choice(DEVICES),
        "source": random.choice(ROADS + DEVICES),
    }


class ProductionTest:
    """全链路生产环境实时测试"""

    def __init__(self, kafka_bootstrap="kafka:9092", redis_host="redis", redis_port=6379):
        self.kafka_bootstrap = kafka_bootstrap
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.running = False
        self.threads = []
        self.stats = {
            "kafka_vehicle": 0, "kafka_status": 0,
            "kafka_device": 0, "kafka_alarm": 0,
            "redis_writes": 0, "errors": 0,
        }
        self.producer = None
        self.redis_client = None
        self.lock = threading.Lock()

    def start(self):
        """初始化连接并启动所有数据线程"""
        from kafka import KafkaProducer
        import redis

        # 连接 Kafka
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks=1,
                retries=3,
                max_block_ms=5000,
            )
            print(f"[TEST] ✅ Kafka 连接成功: {self.kafka_bootstrap}")
        except Exception as e:
            print(f"[TEST] ❌ Kafka 连接失败: {e}")
            return False

        # 连接 Redis
        try:
            self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=0, decode_responses=True)
            self.redis_client.ping()
            print(f"[TEST] ✅ Redis 连接成功: {self.redis_host}:{self.redis_port}")
        except Exception as e:
            print(f"[TEST] ⚠️ Redis 连接失败: {e}")
            self.redis_client = None

        self.running = True

        # 启动3个并发线程
        t1 = threading.Thread(target=self._kafka_loop, daemon=True, name="kafka-loop")
        t2 = threading.Thread(target=self._redis_loop, daemon=True, name="redis-loop")
        t3 = threading.Thread(target=self._stats_printer, daemon=True, name="stats-printer")
        self.threads = [t1, t2, t3]
        for t in self.threads:
            t.start()
        return True

    def stop(self):
        self.running = False
        for t in self.threads:
            t.join(timeout=3)
        if self.producer:
            self.producer.flush()
            self.producer.close()
        print(f"\n[TEST] ✅ 测试结束")

    def _incr_stat(self, key):
        with self.lock:
            self.stats[key] += 1

    def get_stats(self):
        with self.lock:
            return dict(self.stats)

    # ==================== Kafka 数据推送 ====================
    def _kafka_loop(self):
        """每秒批量发送 Kafka 消息"""
        while self.running:
            try:
                # 车辆通行 (高频)
                for _ in range(3):
                    self.producer.send("traffic_vehicle", value=generate_vehicle_pass())
                    self._incr_stat("kafka_vehicle")
                    self.producer.send("device_status", value=generate_device_status())
                    self._incr_stat("kafka_device")

                # 交通状态 (中频)
                self.producer.send("traffic_status", value=generate_traffic_status())
                self._incr_stat("kafka_status")

                # 告警 (低频 ~10%)
                if random.random() < 0.1:
                    self.producer.send("device_alarm", value=generate_alarm())
                    self._incr_stat("kafka_alarm")

                self.producer.flush()
            except Exception as e:
                self._incr_stat("errors")
            time.sleep(1.0)

    # ==================== Redis 数据推送 ====================
    def _redis_loop(self):
        """每秒更新 Redis 实时数据"""
        if not self.redis_client:
            return

        while self.running:
            try:
                now = datetime.now().strftime("%H:%M:%S")
                ts = datetime.now().isoformat()

                # 1. 实时交通流 key (每5秒更新)
                road = random.choice(ROADS)
                flow_data = generate_traffic_status()
                self.redis_client.setex(f"traffic:realtime:latest", 10, json.dumps(flow_data, ensure_ascii=False))

                # 2. 累计计数器
                self.redis_client.incr("traffic:total_vehicles")
                self.redis_client.incrby("traffic:total_flow", random.randint(50, 500))

                # 3. 拥堵排行 (sorted set)
                for road_name in random.sample(ROADS, 5):
                    jam = round(random.uniform(1.0, 5.0), 1)
                    self.redis_client.zadd("traffic:jam_ranking", {road_name: jam})

                # 4. 设备在线率
                online = sum(1 for _ in range(40) if random.random() > 0.05)
                total = 40
                self.redis_client.setex("traffic:device:online_rate", 30, f"{online/total*100:.1f}")
                self.redis_client.setex("traffic:device:online", 30, str(online))
                self.redis_client.setex("traffic:device:total", 300, str(total))

                # 5. 实时速度
                speed = round(random.gauss(45, 15), 1)
                self.redis_client.setex("traffic:realtime:speed", 10, str(speed))

                # 6. 告警队列 (list, 保留最新20条)
                if random.random() < 0.15:
                    alarm = generate_alarm()
                    self.redis_client.lpush("traffic:alarms", json.dumps(alarm, ensure_ascii=False))
                    self.redis_client.ltrim("traffic:alarms", 0, 19)

                # 7. 数据质量得分
                score = round(random.uniform(0.88, 0.99), 2)
                self.redis_client.setex("traffic:quality:score", 30, str(score))

                # 8. 心跳
                self.redis_client.set("traffic:heartbeat", ts)

                self._incr_stat("redis_writes")
            except Exception as e:
                self._incr_stat("errors")
            time.sleep(2.0)

    # ==================== 状态打印 ====================
    def _stats_printer(self):
        """每5秒打印一次统计"""
        while self.running:
            time.sleep(5)
            s = self.get_stats()
            total = sum(v for k, v in s.items() if k != "errors")
            errors = s.get("errors", 0)
            sys.stdout.write(
                f"\r[TEST] 🚗Kafka车辆:{s['kafka_vehicle']} "
                f"📊状态:{s['kafka_status']} "
                f"📡设备:{s['kafka_device']} "
                f"🔔告警:{s['kafka_alarm']} "
                f"💾Redis:{s['redis_writes']} "
                f"❌错误:{errors} "
                f"总计:{total}条     "
            )
            sys.stdout.flush()


# ============================================================
# 主入口
# ============================================================
if __name__ == "__main__":
    duration = 60  # 运行60秒

    print("=" * 60)
    print("  智慧城市交通 — 生产环境全链路实时测试")
    print("=" * 60)
    print(f"  运行时长: {duration}秒")
    print(f"  Kafka:   kafka:9092")
    print(f"  Redis:   redis:6379")
    print()
    print("  📌 请打开以下界面观看实时效果:")
    print("     📊 Flask 大屏:   http://localhost:8088")
    print("     📨 Kafka UI:     http://localhost:8082")
    print("     🗄️ RedisInsight: http://localhost:5540")
    print("     ⚡ Flink UI:     http://localhost:8081")
    print()
    print("  数据生成中...")

    test = ProductionTest(
        kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        redis_host=os.environ.get("REDIS_HOST", "redis"),
        redis_port=int(os.environ.get("REDIS_PORT", "6379")),
    )

    if not test.start():
        print("[TEST] ❌ 启动失败，退出")
        sys.exit(1)

    try:
        for remaining in range(duration, 0, -1):
            time.sleep(1)
            if remaining % 10 == 0:
                print(f"\n[TEST] ⏱️ 剩余 {remaining}s ...")
    except KeyboardInterrupt:
        print("\n[TEST] ⏹️ 用户中断")

    test.stop()

    # 最终统计
    print()
    print("=" * 60)
    print("  📊 最终统计")
    print("=" * 60)
    s = test.get_stats()
    print(f"  Kafka 车辆通行:   {s['kafka_vehicle']} 条")
    print(f"  Kafka 交通状态:   {s['kafka_status']} 条")
    print(f"  Kafka 设备状态:   {s['kafka_device']} 条")
    print(f"  Kafka 故障告警:   {s['kafka_alarm']} 条")
    print(f"  Redis 写入次数:   {s['redis_writes']} 次")
    print(f"  错误数:           {s['errors']} 次")
    print(f"  总数据量:         {sum(v for k,v in s.items() if k!='errors')} 条")
    print("=" * 60)
    print()
    print("  ✅ 生产环境测试完成！")
    print("  📌 所有 Web UI 仍可查看数据:")
    print("     http://localhost:8088  (Flask 大屏)")
    print("     http://localhost:8082  (Kafka UI)")
    print("     http://localhost:5540  (RedisInsight)")
    print("     http://localhost:8081  (Flink UI)")
    print("=" * 60)
