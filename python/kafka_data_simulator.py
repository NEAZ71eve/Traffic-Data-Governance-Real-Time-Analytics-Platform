#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 实时数据采集模拟器
向真实 Kafka 集群持续发送车辆通行/交通状态/设备状态/告警数据
用法: python kafka_data_simulator.py [--bootstrap localhost:9092] [--interval 1.0] [--duration 60]
"""
import json
import os
import random
import sys
import threading
import time
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

PLATE_PREFIXES = ["京", "津", "沪", "渝", "冀", "豫", "辽", "苏", "浙", "粤", "川", "鄂"]

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
# Topic 定义
# ============================================================
TOPICS_CONFIG = {
    "traffic_vehicle":  {"partitions": 8, "description": "车辆通行记录"},
    "traffic_status":   {"partitions": 4, "description": "交通状态记录"},
    "device_status":    {"partitions": 4, "description": "设备状态记录"},
    "device_alarm":     {"partitions": 2, "description": "故障告警记录"},
}


def generate_plate():
    prefix = random.choice(PLATE_PREFIXES)
    letter = random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ")
    digits = "".join(str(random.randint(0, 9)) for _ in range(5))
    return f"{prefix}{letter}{digits}"


class KafkaDataSimulator:
    """向 Kafka 生产实时交通模拟数据"""

    def __init__(self, bootstrap_servers="localhost:9092", interval_sec=1.0):
        self.bootstrap = bootstrap_servers
        self.interval = interval_sec
        self.producer = None
        self.running = False
        self.thread = None
        self.stats = {t: {"sent": 0, "errors": 0, "last_ts": None} for t in TOPICS_CONFIG}
        self.lock = threading.Lock()

    def _get_producer(self):
        from kafka import KafkaProducer
        return KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks=1,
            retries=3,
            max_block_ms=5000,
        )

    def start(self):
        try:
            self.producer = self._get_producer()
            self.running = True
            self.thread = threading.Thread(target=self._produce_loop, daemon=True)
            self.thread.start()
            return True
        except Exception as e:
            print(f"[SIMULATOR] Kafka 连接失败: {e}")
            return False

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        if self.producer:
            self.producer.flush()
            self.producer.close()

    def get_stats(self):
        with self.lock:
            return {
                topic: {"sent": s["sent"], "errors": s["errors"], "last_ts": s["last_ts"]}
                for topic, s in self.stats.items()
            }

    def _send(self, topic, data):
        try:
            self.producer.send(topic, value=data)
            with self.lock:
                self.stats[topic]["sent"] += 1
                self.stats[topic]["last_ts"] = datetime.now().isoformat()
        except Exception:
            with self.lock:
                self.stats[topic]["errors"] += 1

    def _generate_vehicle_pass(self):
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

    def _generate_traffic_status(self):
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

    def _generate_device_status(self):
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

    def _generate_alarm(self):
        severity, message = random.choice(ALARM_DEFS)
        return {
            "ts": datetime.now().isoformat(),
            "severity": severity,
            "message": message,
            "device_id": random.choice(DEVICES),
            "source": random.choice(ROADS + DEVICES),
        }

    def _produce_loop(self):
        tick = 0
        while self.running:
            # 高峰时段更多数据
            hour = datetime.now().hour
            is_peak = (7 <= hour <= 9) or (17 <= hour <= 19)
            batch = 3 if is_peak else 2

            for _ in range(batch):
                self._send("traffic_vehicle", self._generate_vehicle_pass())
                self._send("device_status", self._generate_device_status())

            self._send("traffic_status", self._generate_traffic_status())

            # 告警较低频率 (~10%)
            if random.random() < 0.1:
                self._send("device_alarm", self._generate_alarm())

            tick += 1
            if tick % 60 == 0:
                stats = self.get_stats()
                total = sum(s["sent"] for s in stats.values())
                print(f"[SIMULATOR] tick={tick} total_sent={total}")

            time.sleep(self.interval)

    @staticmethod
    def ensure_topics(bootstrap_servers="localhost:9092"):
        """确保所需 Topic 存在"""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, request_timeout_ms=5000)
            existing = set(admin.list_topics())
            for topic, cfg in TOPICS_CONFIG.items():
                if topic not in existing:
                    admin.create_topics([
                        NewTopic(topic, num_partitions=cfg["partitions"], replication_factor=1)
                    ])
                    print(f"[SIMULATOR] Topic '{topic}' 已创建 ({cfg['partitions']} partitions)")
                else:
                    print(f"[SIMULATOR] Topic '{topic}' 已存在")
            admin.close()
        except Exception as e:
            print(f"[SIMULATOR] Topic 初始化: {e}")


# ============================================================
# CLI Entry
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Kafka 实时数据采集模拟器")
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                        help="Kafka bootstrap servers")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="发送间隔(秒)")
    parser.add_argument("--duration", type=int, default=0,
                        help="运行时长(秒), 0=持续运行")
    parser.add_argument("--ensure-topics", action="store_true",
                        help="自动创建缺失的 Topic")
    args = parser.parse_args()

    print(f"[SIMULATOR] 连接 Kafka: {args.bootstrap}")
    if args.ensure_topics:
        KafkaDataSimulator.ensure_topics(args.bootstrap)

    sim = KafkaDataSimulator(bootstrap_servers=args.bootstrap, interval_sec=args.interval)
    if not sim.start():
        print("[SIMULATOR] 启动失败")
        sys.exit(1)

    print("[SIMULATOR] 已启动, 按 Ctrl+C 停止")
    print(f"  Topics: {', '.join(TOPICS_CONFIG)}")
    print(f"  Interval: {args.interval}s")

    try:
        if args.duration > 0:
            time.sleep(args.duration)
            sim.stop()
            print(f"\n[SIMULATOR] 完成 ({args.duration}s)")
            stats = sim.get_stats()
            for topic, s in stats.items():
                print(f"  {topic}: sent={s['sent']} errors={s['errors']}")
        else:
            while True:
                time.sleep(5)
                stats = sim.get_stats()
                total = sum(s["sent"] for s in stats.values())
                errors = sum(s["errors"] for s in stats.values())
                print(f"\r[SIMULATOR] total={total} errors={errors}  ", end="", flush=True)
    except KeyboardInterrupt:
        print("\n[SIMULATOR] 停止中...")
        sim.stop()
        stats = sim.get_stats()
        print(f"\n[SIMULATOR] 最终统计:")
        for topic, s in stats.items():
            print(f"  {topic}: sent={s['sent']} errors={s['errors']}")
    print("[SIMULATOR] 已退出")
