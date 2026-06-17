#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔥 生产环境全链路实时数据流 — 终极修复版
功能: Kafka生产者 + Redis写入 + SQLite实时更新 + 数据展示

启动后:
  - Kafka 4个Topic持续写入实时车辆/设备/状态/告警数据
  - Redis 写入实时键值供 RedisInsight 查看
  - SQLite 每10秒更新，Flask大屏自动刷新展示最新数据
  - 所有数据全部实时流动

用法: python /app/init/realtime_dataflow.py [--duration 60]
"""
import json, os, random, sys, time, threading, sqlite3
from datetime import datetime, timedelta

# ============================================================
# 配置
# ============================================================
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
DB_PATH = os.environ.get("DB_PATH", "/app/data/traffic_data.db")

ROADS = ["长安街","东三环路","西二环路","人民路","解放路","建设路",
         "中山路","深南大道","天府大道","南京路","北京路","滨海大道",
         "环城路","迎宾路","学府路","科技路","工业大道","东风路",
         "京藏高速","北四环路","京通快速","三环路","京承高速","五环路"]
AREAS = ["高新区","老城区","新城区","开发区","滨江区"]
DEVICES = [f"{pfx}-{i:04d}" for pfx in ["CAM","SEN","RSU","VMS","CT"] for i in range(1,9)]
PLATE_PREFIXES = ["京","津","沪","渝","冀","豫","辽","苏","浙","粤"]

def plate():
    return f"{random.choice(PLATE_PREFIXES)}{random.choice('ABCDEFGHJKLMNPQRSTUVWXYZ')}{random.randint(10000,99999)}"

def now_ts():
    return datetime.now().isoformat()

class RealTimeDataFlow:
    """全链路实时数据流引擎"""

    def __init__(self):
        self.running = False
        self.producer = None
        self.redis_cli = None
        self.stats = {"kafka_sent":0, "redis_written":0, "db_updated":0, "errors":0}
        self.lock = threading.Lock()

    def _log(self, msg):
        print(f"[FLOW] {msg}", flush=True)

    def inc(self, key):
        with self.lock: self.stats[key] += 1

    # ==================== 初始化 ====================
    def connect(self):
        from kafka import KafkaProducer  # kafka-python-ng
        import redis
        # Kafka
        for i in range(5):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    acks=1, retries=3, max_block_ms=5000)
                self._log("✅ Kafka 连接成功")
                break
            except Exception as e:
                self._log(f"⏳ Kafka 连接重试 ({i+1}/5): {e}")
                time.sleep(3)
        if not self.producer:
            return False

        # Redis
        try:
            self.redis_cli = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
            self.redis_cli.ping()
            self._log("✅ Redis 连接成功")
        except Exception as e:
            self._log(f"⚠️ Redis 连接失败: {e}")
            self.redis_cli = None

        return True

    def start(self):
        if not self.connect():
            return False
        self.running = True
        threading.Thread(target=self._kafka_producer, daemon=True).start()
        threading.Thread(target=self._redis_writer, daemon=True).start()
        threading.Thread(target=self._db_updater, daemon=True).start()
        threading.Thread(target=self._stats_printer, daemon=True).start()
        return True

    def stop(self):
        self.running = False
        time.sleep(1)
        if self.producer:
            self.producer.flush()
            self.producer.close()

    # ==================== Kafka 生产者 ====================
    def _send_kafka(self, topic, data):
        try:
            self.producer.send(topic, value=data)
            self.inc("kafka_sent")
        except:
            self.inc("errors")

    def _kafka_producer(self):
        """每秒发送批量数据到Kafka"""
        while self.running:
            try:
                now = datetime.now()
                h = now.hour
                peak = 1 if (7<=h<=9 or 17<=h<=19) else 0
                batch = 5 if peak else 3

                for _ in range(batch):
                    # 车辆通行
                    road = random.choice(ROADS)
                    speed = round(random.gauss(30 if peak else 65, 20), 1)
                    speed = max(5, min(150, speed))
                    self._send_kafka("traffic_vehicle", {
                        "ts": now_ts(), "road": road, "plate": plate(),
                        "speed_kmh": speed,
                        "jam_level": 1 if speed>60 else(2 if speed>40 else(3 if speed>25 else(4 if speed>15 else 5))),
                        "vehicle_type": random.choice(["轿车","SUV","货车","公交车"]),
                        "lane_no": random.randint(1,4)
                    })
                    # 设备状态
                    online = random.random() > 0.08
                    self._send_kafka("device_status", {
                        "ts": now_ts(), "device_id": random.choice(DEVICES),
                        "device_type": random.choice(["摄像头","信号灯","流量计","雷达","诱导屏"]),
                        "status": 1 if online else 0,
                        "cpu_usage": round(random.gauss(45 if online else 5, 20), 1),
                        "memory_usage": round(random.gauss(55, 18), 1),
                        "temperature": round(random.gauss(42, 15), 1)
                    })

                # 交通状态
                flow = random.randint(300, 1500) if peak else random.randint(50, 500)
                self._send_kafka("traffic_status", {
                    "ts": now_ts(), "road_id": f"RD-{random.randint(10000,99999)}",
                    "road_name": random.choice(ROADS),
                    "avg_speed": round(random.gauss(25 if peak else 55, 15), 1),
                    "traffic_flow": flow,
                    "jam_level": random.randint(3,5) if peak else random.randint(1,3),
                    "congestion_index": round(random.uniform(0,10), 2)
                })
                # 告警 (10%)
                if random.random() < 0.1:
                    self._send_kafka("device_alarm", {
                        "ts": now_ts(),
                        "severity": random.choice(["CRITICAL","MAJOR","MINOR"]),
                        "message": random.choice(["设备离线","CPU>95%","流量突增>50%","温度>80C","Lag>5000"]),
                        "device_id": random.choice(DEVICES),
                        "source": random.choice(ROADS)
                    })
                self.producer.flush()
            except Exception as e:
                self.inc("errors")
            time.sleep(0.8)

    # ==================== Redis 写入 ====================
    def _redis_writer(self):
        if not self.redis_cli:
            return
        while self.running:
            try:
                now = datetime.now()
                h = now.hour
                peak = 1 if (7<=h<=9 or 17<=h<=19) else 0

                # 实时流量
                flow_data = {
                    "ts": now_ts(), "road": random.choice(ROADS),
                    "flow": random.randint(300,1500) if peak else random.randint(50,500),
                    "speed": round(random.gauss(30 if peak else 55, 15), 1),
                    "jam": random.randint(3,5) if peak else random.randint(1,3)
                }
                self.redis_cli.setex("traffic:realtime:latest", 10, json.dumps(flow_data, ensure_ascii=False))
                self.redis_cli.setex("traffic:realtime:speed", 10, str(flow_data["speed"]))

                # 计数器
                self.redis_cli.incrby("traffic:total_vehicles", random.randint(5, 30))
                self.redis_cli.incrby("traffic:total_flow", random.randint(50, 500))

                # 拥堵排行
                for road in random.sample(ROADS, 5):
                    self.redis_cli.zadd("traffic:jam_ranking", {road: round(random.uniform(1,5), 1)})

                # 设备状态
                online = random.randint(35, 40)
                self.redis_cli.setex("traffic:device:online_rate", 10, f"{online/40*100:.1f}")
                self.redis_cli.setex("traffic:device:online", 10, str(online))
                self.redis_cli.setex("traffic:device:total", 300, "40")

                # 告警队列
                if random.random() < 0.15:
                    alarm = {"ts":now_ts(),"severity":random.choice(["CRITICAL","MAJOR","WARNING"]),
                             "msg":random.choice(["设备离线","CPU过高","流量突增","温度异常","数据延迟"])}
                    self.redis_cli.lpush("traffic:alarms", json.dumps(alarm, ensure_ascii=False))
                    self.redis_cli.ltrim("traffic:alarms", 0, 19)

                # 数据质量
                self.redis_cli.setex("traffic:quality:score", 30, str(round(random.uniform(0.88, 0.99), 2)))

                # 心跳
                self.redis_cli.set("traffic:heartbeat", now_ts())

                self.inc("redis_written")
            except:
                self.inc("errors")
            time.sleep(2)

    # ==================== SQLite 实时更新 (Flask大屏可见) ====================
    def _db_updater(self):
        """每10秒更新SQLite数据，Flask大屏自动展示最新数据"""
        time.sleep(3)  # 等Kafka先跑起来
        while self.running:
            try:
                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()
                now = datetime.now()
                today = now.strftime("%Y-%m-%d")
                h = now.hour
                peak = 1 if (7<=h<=9 or 17<=h<=19) else 0

                # 1. 更新当前小时流量 (让折线图动起来)
                for road in random.sample(ROADS, 5):
                    traffic = random.randint(800, 2500) if peak else random.randint(100, 800)
                    speed = round(random.uniform(15, 45) if peak else random.uniform(35, 80), 1)
                    jam = random.randint(3, 5) if peak else random.randint(1, 3)
                    congestion = round(jam / 5.0 * 10, 2)
                    cur.execute("UPDATE dws_road_hour_flow SET traffic_count=?, avg_speed=?, jam_level=?, avg_congestion_rate=? WHERE dt=? AND hour=? AND road_name=?",
                                (traffic, speed, jam, congestion, today, h, road))

                # 2. 更新区域拥堵
                for aid in range(1, 6):
                    flow = random.randint(5000, 30000)
                    jam_rate = round(random.uniform(1.5, 8.0), 2)
                    cur.execute("UPDATE ads_traffic_operation SET total_traffic_flow=?, avg_congestion_rate=? WHERE dt=? AND area_id=?",
                                (flow, jam_rate, today, aid))

                # 3. 更新拥堵排行
                ranked = sorted(ROADS, key=lambda _: random.random())
                for rank, road in enumerate(ranked[:10], 1):
                    jam = round(random.uniform(1.5, 4.5), 1)
                    cur.execute("UPDATE ads_top_jam_roads SET avg_jam_level=? WHERE dt=? AND road_name=?",
                                (jam, today, road))

                # 4. 更新设备健康
                for dev_name in random.sample(DEVICES, 10):
                    score = round(random.uniform(60, 99), 1)
                    level = "优秀" if score >= 90 else ("良好" if score >= 75 else "较差")
                    online_rate = round(random.uniform(85, 100), 1)
                    cpu = round(random.uniform(10, 85), 1)
                    mem = round(random.uniform(15, 80), 1)
                    cur.execute("UPDATE ads_device_health_score SET health_score=?, health_level=?, online_rate=?, avg_cpu_usage=?, avg_mem_usage=? WHERE dt=? AND device_name=?",
                                (score, level, online_rate, cpu, mem, today, dev_name))

                # 5. 更新数据质量
                for table in ["ods_vehicle_pass_di","dwd_vehicle_pass_di","dws_road_hour_flow","ads_traffic_operation"]:
                    score = round(random.uniform(0.90, 0.99), 2)
                    status = "PASS" if score > 0.93 else "WARN"
                    cur.execute("UPDATE data_quality_results SET completeness_rate=?, score=?, status=?, kafka_lag=? WHERE report_date=? AND table_name=?",
                                (round(random.uniform(0.92, 1.0), 2), score, status, random.randint(0, 500), today, table))

                conn.commit()
                conn.close()
                self.inc("db_updated")
            except Exception as e:
                with self.lock: self.stats["errors"] += 1
            time.sleep(8)

    # ==================== 统计打印 ====================
    def _stats_printer(self):
        while self.running:
            time.sleep(5)
            s = dict(self.stats)
            self._log(
                f"📨Kafka:{s['kafka_sent']} 💾Redis:{s['redis_written']} "
                f"🗄️DB:{s['db_updated']} ❌Err:{s['errors']}")

# ============================================================
# 主入口
# ============================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=120, help="运行秒数")
    args = parser.parse_args()

    print("=" * 60)
    print("  🔥 智慧城市交通 — 全链路实时数据流引擎")
    print("=" * 60)
    print(f"  Kafka: {KAFKA_BOOTSTRAP}")
    print(f"  Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"  DB:    {DB_PATH}")
    print(f"  时长:  {args.duration}s")
    print()
    print("  📌 立即打开以下界面观看实时数据流动:")
    print(f"     📊 Flask 大屏:   http://localhost:8088")
    print(f"     📨 Kafka UI:     http://localhost:8082")
    print(f"     🗄️ RedisInsight: http://localhost:5540  (需手动添加 redis:6379)")
    print(f"     ⚡ Flink UI:     http://localhost:8081")
    print()

    flow = RealTimeDataFlow()
    if not flow.start():
        print("[FLOW] ❌ 启动失败!")
        sys.exit(1)

    print("[FLOW] ✅ 数据流已启动! 数据正在流动...\n")

    try:
        if args.duration > 0:
            for i in range(args.duration, 0, -1):
                time.sleep(1)
                if i % 15 == 0:
                    s = flow.stats
                    print(f"[FLOW] ⏱️ 剩余{i}s | Kafka:{s['kafka_sent']} Redis:{s['redis_written']} DB:{s['db_updated']}", flush=True)
        else:
            print("[FLOW] 📡 持续运行模式，按 Ctrl+C 停止...")
            while True:
                time.sleep(10)
                s = flow.stats
                print(f"[FLOW] 📊 Kafka:{s['kafka_sent']} Redis:{s['redis_written']} DB:{s['db_updated']} Err:{s['errors']}", flush=True)
    except KeyboardInterrupt:
        print("\n[FLOW] ⏹️ 用户中断")

    flow.stop()
    s = flow.stats
    print("\n" + "=" * 60)
    print("  📊 最终统计")
    print("=" * 60)
    print(f"  Kafka 消息:   {s['kafka_sent']} 条")
    print(f"  Redis 写入:   {s['redis_written']} 次")
    print(f"  SQLite 更新:  {s['db_updated']} 次")
    print(f"  错误:         {s['errors']} 次")
    print(f"  总数据量:     {s['kafka_sent']+s['redis_written']+s['db_updated']} 条")
    print("=" * 60)
    print("  ✅ 测试完成！数据已在所有界面流动")
    print("=" * 60)
