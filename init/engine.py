#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔥 生产环境强力数据流引擎 — 让所有界面数据肉眼可见跳动
每秒大幅更新：Flask大屏 + Kafka + Redis + Flink
启动后数字会像心跳一样不断变化
用法: python /app/init/engine.py [--duration 300]
"""
import json, os, random, sys, time, threading, sqlite3, signal
from datetime import datetime

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
DB_PATH = os.environ.get("DB_PATH", "/app/data/traffic_data.db")
RUNNING = True

ROADS = ["长安街","东三环路","西二环路","人民路","解放路","建设路",
         "中山路","深南大道","天府大道","南京路","北京路","滨海大道",
         "环城路","迎宾路","学府路","科技路","工业大道","东风路",
         "京藏高速","北四环路","京通快速","三环路","京承高速","五环路"]
DEVICES = [f"{pfx}-{i:04d}" for pfx in ["CAM","SEN","RSU","VMS","CT"] for i in range(1,9)]

def log(msg): print(f"[ENGINE] {msg}", flush=True)

def db_update():
    """大幅度更新DB数据 — 每5秒让数字跳变10-30%"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")
        h = datetime.now().hour
        peak = 1 if (7<=h<=9 or 17<=h<=19) else 0

        # 1. 更新所有小时流量数据 — 幅度大
        multiplier = random.uniform(0.8, 1.3)
        cur.execute("""UPDATE dws_road_hour_flow
            SET traffic_count = CAST(traffic_count * ? AS INTEGER),
                avg_speed = ROUND(avg_speed * ?, 1),
                jam_level = CAST(ROUND(jam_level * ?) AS INTEGER),
                avg_congestion_rate = ROUND(avg_congestion_rate * ?, 2)
            WHERE dt = ?""", (multiplier, random.uniform(0.85,1.15),
                              random.uniform(0.8,1.2), random.uniform(0.8,1.2), today))
        rows_affected = cur.rowcount

        # 2. 更新区域流量
        for aid in range(1, 6):
            flow = random.randint(15000, 35000)
            jam = round(random.uniform(2.0, 9.0), 2)
            cur.execute("UPDATE ads_traffic_operation SET total_traffic_flow=?, avg_congestion_rate=? WHERE dt=? AND area_id=?",
                       (flow, jam, today, aid))

        # 3. 更新拥堵排行 — 道路名轮换
        ranked = sorted(ROADS, key=lambda _: random.random())
        for rank, road in enumerate(ranked[:10], 1):
            jam = round(random.uniform(1.0, 5.0), 1)
            cur.execute("UPDATE ads_top_jam_roads SET avg_jam_level=? WHERE dt=? AND road_name=?",
                       (jam, today, road))

        # 4. 更新设备健康
        for dev in random.sample(DEVICES, 15):
            score = round(random.uniform(55, 99), 1)
            level = "优秀" if score >= 90 else ("良好" if score >= 75 else "较差")
            cur.execute("UPDATE ads_device_health_score SET health_score=?, health_level=?, online_rate=?, avg_cpu_usage=?, avg_mem_usage=? WHERE dt=? AND device_name=?",
                       (score, level, round(random.uniform(80,100),1),
                        round(random.uniform(10,95),1), round(random.uniform(15,90),1), today, dev))

        conn.commit()
        conn.close()
        return rows_affected
    except Exception as e:
        log(f"DB错误: {e}")
        return 0

def kafka_send(producer):
    """发送一批Kafka消息"""
    try:
        h = datetime.now().hour
        peak = 1 if (7<=h<=9 or 17<=h<=19) else 0
        batch = 8 if peak else 5
        for _ in range(batch):
            road = random.choice(ROADS)
            speed = round(random.gauss(30 if peak else 65, 25), 1)
            speed = max(5, min(150, speed))
            producer.send("traffic_vehicle", value={
                "ts": datetime.now().isoformat(), "road": road,
                "plate": f"{random.choice('京津冀沪渝')}{random.choice('ABCDEFGHJKLMN')}{random.randint(10000,99999)}",
                "speed_kmh": speed,
                "jam_level": 1 if speed>60 else(2 if speed>40 else(3 if speed>25 else(4 if speed>15 else 5))),
                "vehicle_type": random.choice(["轿车","SUV","货车","公交车"]),
                "lane_no": random.randint(1,4)
            })
            producer.send("device_status", value={
                "ts": datetime.now().isoformat(),
                "device_id": random.choice(DEVICES),
                "device_type": random.choice(["摄像头","信号灯","流量计","雷达","诱导屏"]),
                "status": 1 if random.random()>0.08 else 0,
                "cpu_usage": round(random.gauss(45, 20),1),
                "memory_usage": round(random.gauss(55, 18),1)
            })
        producer.send("traffic_status", value={
            "ts": datetime.now().isoformat(), "road_name": random.choice(ROADS),
            "traffic_flow": random.randint(100, 2000),
            "jam_level": random.randint(1,5)
        })
        if random.random() < 0.15:
            producer.send("device_alarm", value={
                "ts": datetime.now().isoformat(),
                "severity": random.choice(["CRITICAL","MAJOR","MINOR"]),
                "message": random.choice(["设备离线","CPU>95%","流量突增>50%","温度>80C","Lag>5000"]),
                "device_id": random.choice(DEVICES)
            })
        producer.flush()
        return batch + 2
    except:
        return 0

def redis_write(rc):
    """写入Redis"""
    try:
        h = datetime.now().hour
        peak = 1 if (7<=h<=9 or 17<=h<=19) else 0
        rc.setex("traffic:realtime:latest", 10, json.dumps({
            "flow": random.randint(300,1500) if peak else random.randint(50,500),
            "speed": round(random.gauss(30 if peak else 55, 15),1),
            "jam": random.randint(3,5) if peak else random.randint(1,3)
        }))
        rc.setex("traffic:realtime:speed", 10, str(round(random.gauss(40, 20),1)))
        rc.incrby("traffic:total_vehicles", random.randint(10, 80))
        rc.incrby("traffic:total_flow", random.randint(100, 1000))
        for road in random.sample(ROADS, 5):
            rc.zadd("traffic:jam_ranking", {road: round(random.uniform(1,5),1)})
        online = random.randint(32, 40)
        rc.setex("traffic:device:online_rate", 10, f"{online/40*100:.1f}")
        rc.setex("traffic:device:online", 10, str(online))
        rc.set("traffic:heartbeat", datetime.now().isoformat())
        rc.setex("traffic:quality:score", 30, str(round(random.uniform(0.85, 0.99), 2)))
        if random.random() < 0.2:
            rc.lpush("traffic:alarms", json.dumps({
                "ts":datetime.now().isoformat(),"severity":random.choice(["CRITICAL","MAJOR","WARNING"]),
                "msg":random.choice(["设备离线","CPU过高","流量突增","温度异常","数据延迟"])
            }))
            rc.ltrim("traffic:alarms", 0, 19)
        return 1
    except:
        return 0

def main_loop(duration):
    global RUNNING
    # 连接Kafka
    from kafka import KafkaProducer
    import redis as redis_mod

    producer = None
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks=1, retries=3, max_block_ms=5000)
            log("✅ Kafka连接成功")
            break
        except Exception as e:
            log(f"⏳ Kafka重试({i+1}/10): {e}")
            time.sleep(3)
    if not producer:
        log("❌ Kafka连接失败，用DB-only模式")
        producer = None

    rc = None
    try:
        rc = redis_mod.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        rc.ping()
        log("✅ Redis连接成功")
    except:
        log("⚠️ Redis不可用，跳过Redis")

    log("🔥 数据引擎启动！每5秒大幅更新一次")
    log("📌 请刷新 http://localhost:8088 查看数字跳动")
    log("")

    kafka_cnt = 0
    redis_cnt = 0
    db_cnt = 0
    errors = 0
    start = time.time()

    while RUNNING and (duration <= 0 or time.time() - start < duration):
        # DB — 大幅度更新
        for _ in range(3):  # 每次循环更新3次DB让变化更大
            r = db_update()
            if r: db_cnt += 1
            time.sleep(0.3)

        # Kafka
        if producer:
            kafka_cnt += kafka_send(producer)

        # Redis
        if rc:
            redis_cnt += redis_write(rc)

        elapsed = int(time.time() - start)
        remaining = max(0, duration - elapsed) if duration > 0 else 0

        # 取当前大屏数据看看
        try:
            conn2 = sqlite3.connect(DB_PATH)
            total = conn2.execute("SELECT SUM(traffic_count) FROM dws_road_hour_flow WHERE dt=date('now')").fetchone()[0] or 0
            speed = conn2.execute("SELECT ROUND(AVG(avg_speed),1) FROM dws_road_hour_flow WHERE dt=date('now')").fetchone()[0] or 0
            conn2.close()
        except:
            total = "?"
            speed = "?"

        log(f"⏱️ {elapsed}s | 大屏:流量={total} 速度={speed} | Kafka:{kafka_cnt} Redis:{redis_cnt} DB:{db_cnt}次 错误:{errors}")
        time.sleep(5)

    # 清理
    if producer:
        producer.flush()
        producer.close()
    log(f"✅ 完成！Kafka:{kafka_cnt} Redis:{redis_cnt} DB:{db_cnt} 错误:{errors}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=600)
    args = parser.parse_args()

    # 信号处理
    def handler(signum, frame):
        global RUNNING
        RUNNING = False
        log("收到停止信号，正在退出...")
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    main_loop(args.duration)
