# -*- coding: utf-8 -*-
"""实时数据管道模拟器 — Kafka→Flink→Hive 全链路实时可视化

启动后自动生成连续流数据，通过SSE推送至浏览器，展示：
- Kafka 4个Topic实时吞吐/积压 (动态柱状图)
- Flink 3个Job流处理速率/P50P99延迟 (实时刷新卡片)
- Hive ETL五层批处理周期状态 (进度条+阶段流转)
- 实时数据记录滚动 + 告警弹窗
- 全链路拓扑实时数据流动画
"""
import time, json, random, threading, queue, os, sys
from datetime import datetime, timedelta
from collections import deque
from flask import Flask, jsonify, render_template_string, request, Response

app = Flask(__name__)

# ============================================================
# Global State — shared between generator thread and Flask
# ============================================================
STATE_LOCK = threading.Lock()

# Kafka topics state
KAFKA_TOPICS = {
    "traffic_vehicle":    {"msgs_per_sec": 0, "total_msgs": 0, "lag": 0, "partitions": 8, "color": "#42a5f5"},
    "traffic_status":     {"msgs_per_sec": 0, "total_msgs": 0, "lag": 0, "partitions": 4, "color": "#66bb6a"},
    "device_status":      {"msgs_per_sec": 0, "total_msgs": 0, "lag": 0, "partitions": 4, "color": "#ffa726"},
    "device_alarm":       {"msgs_per_sec": 0, "total_msgs": 0, "lag": 0, "partitions": 2, "color": "#ef5350"},
}

# Flink jobs state
FLINK_JOBS = {
    "TrafficVehicleCount":          {"events_per_sec": 0, "p50_ms": 0, "p99_ms": 0, "checkpoint_mb": 0, "backlog": 0, "status": "RUNNING"},
    "TrafficCongestionDetection":   {"events_per_sec": 0, "p50_ms": 0, "p99_ms": 0, "checkpoint_mb": 0, "backlog": 0, "status": "RUNNING"},
    "DeviceStatusCEP":              {"events_per_sec": 0, "p50_ms": 0, "p99_ms": 0, "checkpoint_mb": 0, "backlog": 0, "status": "RUNNING"},
}

# Hive ETL state
ETL_PHASES = ["ODS_Landing", "DWD_Cleaning", "DWS_Aggregation", "ADS_Indicators", "Quality_Check"]
ETL_STATE = {"current_phase": 0, "phase_progress": 0, "cycle": 0, "total_rows": 0, "phase_start": time.time()}

# Recent records (rolling buffer for display)
RECENT_RECORDS = deque(maxlen=50)
RECENT_ALERTS = deque(maxlen=20)

# Global counters
COUNTERS = {"total_events": 0, "throughput_mbps": 0, "anomalies_detected": 0, "etl_cycles_completed": 0,
            "data_volume_gb": 0.0, "uptime_seconds": 0, "active_alerts": 0}

# Pipeline health
PIPELINE_HEALTH = 100.0

# Vehicle/device pools for realistic data
ROADS = ["长安街", "东三环路", "西二环路", "人民路", "解放路", "建设路", "中山路", "深南大道", "天府大道", "南京路"]
DEVICES = [f"CT-{i:04d}" for i in range(1, 31)]
AREAS = ["高新区", "老城区", "新城区", "开发区", "滨江区"]
DEVICE_TYPES = ["摄像头", "信号灯", "流量计", "雷达", "诱导屏"]

start_time = time.time()
stop_event = threading.Event()

# ============================================================
# Kafka 连接探测 — 如果有真实 Kafka 则读取消息计数
# ============================================================
KAFKA_ENABLED = False
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

try:
    from kafka import KafkaConsumer
    test_consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, consumer_timeout_ms=2000)
    test_consumer.close()
    KAFKA_ENABLED = True
except Exception:
    pass


def kafka_consumer_thread():
    """从真实 Kafka Topic 读取消息，更新 KAFKA_TOPICS 状态"""
    if not KAFKA_ENABLED:
        return
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="realtime-pipeline-viz",
            consumer_timeout_ms=3000,
        )
        topic_names = list(KAFKA_TOPICS.keys())
        # 获取每个 topic 的 partition 分配
        partitions = []
        for topic in topic_names:
            try:
                pts = consumer.partitions_for_topic(topic)
                if pts:
                    for p in pts:
                        partitions.append(TopicPartition(topic, p))
            except Exception:
                pass
        if partitions:
            consumer.assign(partitions)
            # 从最新开始 (不回溯历史)
            consumer.seek_to_end()

        while not stop_event.is_set():
            batch = consumer.poll(timeout_ms=1000, max_records=100)
            if batch:
                with STATE_LOCK:
                    for tp, msgs in batch.items():
                        topic = tp.topic
                        if topic in KAFKA_TOPICS:
                            KAFKA_TOPICS[topic]["total_msgs"] += len(msgs)
                            KAFKA_TOPICS[topic]["msgs_per_sec"] = len(msgs)
            else:
                # 无消息时清零速率
                with STATE_LOCK:
                    for t in topic_names:
                        if KAFKA_TOPICS[t]["msgs_per_sec"] == 0:
                            pass  # 保持上次值
        consumer.close()
    except Exception as e:
        pass  # Kafka 不可用时静默跳过


# ============================================================
# Simulator Threads
# ============================================================

def generate_vehicle_pass():
    """Generate a single vehicle pass record"""
    road = random.choice(ROADS)
    speed = max(0, min(180, random.gauss(45, 25)))
    jam_level = 1 if speed > 60 else (2 if speed > 40 else (3 if speed > 25 else (4 if speed > 15 else 5)))
    return {
        "ts": datetime.now().strftime("%H:%M:%S.%f")[:-3],
        "type": "vehicle",
        "road": road,
        "plate": f"{random.choice('京津冀沪渝')}{chr(random.randint(65,90))}{random.randint(10000,99999)}",
        "speed": round(speed, 1),
        "jam": jam_level,
        "lane": random.randint(1, 4)
    }

def generate_device_status():
    """Generate a single device status record"""
    device = random.choice(DEVICES)
    cpu = round(random.gauss(45, 20), 1)
    mem = round(random.gauss(55, 18), 1)
    temp = round(random.gauss(42, 15), 1)
    online = random.random() > 0.05
    return {
        "ts": datetime.now().strftime("%H:%M:%S.%f")[:-3],
        "type": "device",
        "device": device,
        "dev_type": random.choice(DEVICE_TYPES),
        "cpu": max(0, min(100, cpu)),
        "mem": max(0, min(100, mem)),
        "temp": max(10, min(95, temp)),
        "online": online
    }

def generate_alarm():
    """Generate an alarm record"""
    alarm_types = [
        ("CRITICAL", "拥堵等级5持续>30min"),
        ("MAJOR", "车流量突增>50%"),
        ("MAJOR", "设备离线超过2h"),
        ("MINOR", "车速<15km/h"),
        ("MINOR", "Kafka Lag超过5000"),
        ("CRITICAL", "CPU使用率>95%"),
        ("MAJOR", "温度超过80°C"),
        ("WARNING", "Checkpoint耗时>10s"),
    ]
    sev, msg = random.choice(alarm_types)
    return {
        "ts": datetime.now().strftime("%H:%M:%S"),
        "severity": sev,
        "message": msg,
        "source": random.choice(ROADS + DEVICES)
    }

def kafka_simulator():
    """Simulate Kafka broker: each iteration = ~1秒的消息吞吐"""
    while not stop_event.is_set():
        with STATE_LOCK:
            # Generate per-topic message rates with realistic patterns
            hour = datetime.now().hour
            rush_mult = 2.5 if (7 <= hour <= 9) or (17 <= hour <= 19) else 1.5 if (10 <= hour <= 16) else 0.8

            topics_data = {
                "traffic_vehicle":  int(random.randint(3000, 8000) * rush_mult),
                "traffic_status":   int(random.randint(2000, 6000) * rush_mult),
                "device_status":    int(random.randint(1000, 4000) * rush_mult),
                "device_alarm":     int(random.randint(100, 800) * rush_mult),
            }

            total_msgs = 0
            for topic, rate in topics_data.items():
                KAFKA_TOPICS[topic]["msgs_per_sec"] = rate
                KAFKA_TOPICS[topic]["total_msgs"] += rate
                KAFKA_TOPICS[topic]["lag"] = max(0, KAFKA_TOPICS[topic]["lag"] +
                    random.randint(-200, 500))
                total_msgs += rate

            COUNTERS["total_events"] += total_msgs
            COUNTERS["throughput_mbps"] = round(total_msgs * 0.5 / 1024 / 1024, 2)  # ~0.5KB per msg
            COUNTERS["data_volume_gb"] += total_msgs * 0.5 / 1024 / 1024 / 1024
        time.sleep(1)

def flink_simulator():
    """Simulate Flink streaming jobs processing"""
    while not stop_event.is_set():
        with STATE_LOCK:
            for job_name, job in FLINK_JOBS.items():
                if random.random() < 0.02:  # 2% chance of restart
                    job["status"] = "RESTARTING"
                else:
                    job["status"] = "RUNNING"

                job["events_per_sec"] = random.randint(2000, 12000)
                job["p50_ms"] = random.randint(5, 40)
                job["p99_ms"] = random.randint(30, 300)
                job["checkpoint_mb"] = round(random.uniform(10, 150), 1)
                job["backlog"] = max(0, random.randint(-50, 200))

            COUNTERS["uptime_seconds"] = int(time.time() - start_time)
        time.sleep(2)

def hive_etl_simulator():
    """Simulate Hive batch ETL cycles: ODS→DWD→DWS→ADS→Quality"""
    while not stop_event.is_set():
        for phase_idx, phase_name in enumerate(ETL_PHASES):
            ETL_STATE["current_phase"] = phase_idx
            ETL_STATE["phase_progress"] = 0
            ETL_STATE["phase_start"] = time.time()

            # Simulate processing progress
            duration = random.uniform(5, 15)  # seconds per phase
            steps = int(duration * 10)
            for i in range(steps):
                if stop_event.is_set():
                    return
                ETL_STATE["phase_progress"] = min(100, (i + 1) / steps * 100)
                ETL_STATE["total_rows"] += random.randint(5000, 50000)
                time.sleep(0.1)

        ETL_STATE["cycle"] += 1
        COUNTERS["etl_cycles_completed"] += 1

def data_generator():
    """Generate individual data records for the live feed"""
    while not stop_event.is_set():
        with STATE_LOCK:
            # Vehicle passes (high frequency)
            for _ in range(random.randint(3, 8)):
                RECENT_RECORDS.append(generate_vehicle_pass())

            # Device statuses (medium frequency)
            for _ in range(random.randint(1, 3)):
                RECENT_RECORDS.append(generate_device_status())

            # Alarms (low frequency)
            if random.random() < 0.3:
                alarm = generate_alarm()
                RECENT_ALERTS.append(alarm)
                RECENT_RECORDS.append({"ts": alarm["ts"], "type": "alarm",
                    "severity": alarm["severity"], "message": alarm["message"]})

            COUNTERS["active_alerts"] = len(RECENT_ALERTS)
            COUNTERS["anomalies_detected"] += random.randint(0, 1)

            # Pipeline health
            alg_count = len(RECENT_ALERTS)
            PIPELINE_HEALTH = max(50, 100 - alg_count * 2 - random.uniform(0, 5))

        time.sleep(random.uniform(0.3, 0.8))

# ============================================================
# Flask SSE & API Endpoints
# ============================================================

@app.route("/")
def index():
    return render_template_string(REALTIME_HTML)

@app.route("/api/state")
def api_state():
    """Snapshot of all current state (fallback for non-SSE clients)"""
    with STATE_LOCK:
        return jsonify({
            "kafka": KAFKA_TOPICS,
            "flink": FLINK_JOBS,
            "etl": ETL_STATE,
            "counters": COUNTERS,
            "recent_records": list(RECENT_RECORDS)[-15:],
            "recent_alerts": list(RECENT_ALERTS)[-10:],
            "health": round(PIPELINE_HEALTH, 1),
            "uptime": int(time.time() - start_time),
            "ts": datetime.now().strftime("%H:%M:%S")
        })

@app.route("/stream")
def stream():
    """SSE endpoint pushing live metrics every second"""
    def event_stream():
        while True:
            with STATE_LOCK:
                data = {
                    "kafka": {t: {"msgs_per_sec": v["msgs_per_sec"], "lag": v["lag"],
                                   "total_msgs": v["total_msgs"], "color": v["color"]}
                              for t, v in KAFKA_TOPICS.items()},
                    "flink": FLINK_JOBS,
                    "etl": {"current_phase": ETL_STATE["current_phase"],
                            "phase_progress": ETL_STATE["phase_progress"],
                            "cycle": ETL_STATE["cycle"],
                            "phase_name": ETL_PHASES[ETL_STATE["current_phase"]]},
                    "counters": COUNTERS,
                    "health": round(PIPELINE_HEALTH, 1),
                    "new_records": list(RECENT_RECORDS)[-8:],
                    "new_alerts": list(RECENT_ALERTS)[-3:],
                    "ts": datetime.now().strftime("%H:%M:%S")
                }
            yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            time.sleep(1)
    return Response(event_stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# ============================================================
# HTML Template
# ============================================================
REALTIME_HTML = r'''<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>实时数据管道 — Kafka→Flink→Hive 全链路监控</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#080c1a;color:#b0bec5;font-family:"Microsoft YaHei","PingFang SC",monospace;font-size:13px;overflow-x:hidden}
.hdr{background:linear-gradient(90deg,#0a0f2c,#141e3a);padding:10px 20px;display:flex;justify-content:space-between;align-items:center;border-bottom:2px solid #1e3a5f}
.hdr h1{font-size:17px;color:#4fc3f7;letter-spacing:1px}
.hdr .live{display:flex;align-items:center;gap:8px}
.hdr .live .dot{width:10px;height:10px;border-radius:50%;background:#66bb6a;animation:pulse 1.5s infinite}
.hdr .live span{color:#66bb6a;font-size:12px}
.hdr .ts{color:#546e7a;font-size:12px}
@keyframes pulse{0%,100%{opacity:1;box-shadow:0 0 6px #66bb6a}50%{opacity:.3;box-shadow:0 0 20px #66bb6a}}

.grid{display:grid;grid-template-columns:1.2fr 1fr 1fr;gap:10px;padding:10px}
@media(max-width:1200px){.grid{grid-template-columns:1fr 1fr}}
@media(max-width:768px){.grid{grid-template-columns:1fr}}

.panel{background:#0d1525;border:1px solid #1a2a44;border-radius:8px;padding:12px;overflow:hidden}
.panel h2{font-size:13px;color:#546e7a;text-transform:uppercase;letter-spacing:2px;margin-bottom:10px;border-bottom:1px solid #1a2a44;padding-bottom:6px}
.panel.full{grid-column:1/-1}

/* Kafka topic bars */
.topic-row{display:flex;align-items:center;margin:4px 0;gap:8px}
.topic-row .label{width:100px;font-size:11px;color:#90a4ae;text-align:right;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.topic-row .bar-wrap{flex:1;height:22px;background:#0f1a30;border-radius:3px;position:relative;overflow:hidden}
.topic-row .bar-fill{height:100%;border-radius:3px;transition:width .5s ease;position:relative;display:flex;align-items:center;padding-left:6px;font-size:10px;color:#fff;white-space:nowrap}
.topic-row .val{width:90px;font-size:11px;text-align:left;color:#78909c}
.topic-row .lag{width:70px;font-size:10px;text-align:right}

/* Flink job cards */
.flink-cards{display:flex;flex-direction:column;gap:6px}
.flink-card{background:#0f1a30;border-radius:6px;padding:8px 10px;border-left:3px solid #42a5f5;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:4px}
.flink-card .jname{font-size:12px;color:#e0e0e0;font-weight:bold;min-width:120px}
.flink-card .jmet{font-size:10px;color:#78909c;display:flex;gap:10px}
.flink-card .jmet span{color:#4fc3f7;font-weight:bold}
.flink-card .status{font-size:10px;padding:2px 8px;border-radius:10px}
.flink-card .status.RUNNING{background:#1b3a1b;color:#66bb6a}
.flink-card .status.RESTARTING{background:#3a2a1b;color:#ffa726}

/* ETL progress */
.etl-phases{display:flex;gap:4px;margin:8px 0}
.etl-phase{flex:1;text-align:center;padding:8px 4px;border-radius:6px;font-size:10px;background:#0f1a30;opacity:.5;transition:all .3s}
.etl-phase.active{opacity:1;background:#1a3a5a;border:1px solid #4fc3f7}
.etl-phase.done{opacity:.7;background:#1a3a2a;border:1px solid #66bb6a}
.etl-phase .pn{font-weight:bold;font-size:11px}
.progress-bar{height:6px;background:#0f1a30;border-radius:3px;margin:8px 0;overflow:hidden}
.progress-bar .fill{height:100%;background:linear-gradient(90deg,#42a5f5,#4fc3f7);border-radius:3px;transition:width .5s ease}

/* Live data feed */
.data-feed{max-height:280px;overflow-y:auto;font-size:10px;font-family:"Consolas","Courier New",monospace}
.data-feed .row{display:flex;gap:8px;padding:2px 4px;border-bottom:1px solid #0f1a30;animation:fadeIn .3s}
.data-feed .row .ts{color:#546e7a;width:70px;flex-shrink:0}
.data-feed .row .tp{width:50px;flex-shrink:0;font-weight:bold;text-align:center;border-radius:3px;padding:0 4px}
.data-feed .row .tp.vehicle{background:#153555;color:#42a5f5}
.data-feed .row .tp.device{background:#154515;color:#66bb6a}
.data-feed .row .tp.alarm{background:#551515;color:#ef5350}
.data-feed .row .detail{color:#90a4ae;flex:1}
@keyframes fadeIn{from{opacity:0;transform:translateY(-10px)}to{opacity:1;transform:translateY(0)}}

/* Alert list */
.alerts{max-height:150px;overflow-y:auto}
.alert-row{padding:4px 8px;margin:2px 0;border-radius:4px;font-size:11px;display:flex;gap:8px;animation:fadeIn .3s}
.alert-row.CRITICAL{background:#3a1515;border-left:3px solid #ef5350;color:#ef5350}
.alert-row.MAJOR{background:#3a2a15;border-left:3px solid #ffa726;color:#ffa726}
.alert-row.MINOR,.alert-row.WARNING{background:#151a3a;border-left:3px solid #42a5f5;color:#42a5f5}

/* Pipeline topology */
.topo{display:flex;align-items:center;justify-content:center;gap:6px;flex-wrap:wrap;padding:10px 0}
.topo-node{background:#0f1a30;border:2px solid #1e3a5f;border-radius:10px;padding:8px 12px;text-align:center;font-size:10px;min-width:70px;transition:all .3s}
.topo-node .nicon{font-size:20px;display:block}
.topo-node .nlabel{color:#90a4ae;margin-top:2px}
.topo-node .nrate{color:#4fc3f7;font-weight:bold;font-size:11px}
.topo-node.active{border-color:#4fc3f7;box-shadow:0 0 12px rgba(79,195,247,.3)}
.topo-arrow{color:#546e7a;font-size:16px}
@keyframes flow{0%{color:#546e7a}50%{color:#4fc3f7}100%{color:#546e7a}}
.topo-arrow{animation:flow 2s infinite}

/* KPI cards */
.kpi-row{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:8px}
.kpi{flex:1;min-width:100px;background:#0f1a30;border-radius:8px;padding:10px;text-align:center}
.kpi .kval{font-size:26px;font-weight:bold}
.kpi .klab{font-size:10px;color:#546e7a;margin-top:4px}
.kpi .kval.g{color:#66bb6a}.kpi .kval.b{color:#42a5f5}.kpi .kval.o{color:#ffa726}.kpi .kval.r{color:#ef5350}.kpi .kval.c{color:#26c6da}

/* Scrollbar */
::-webkit-scrollbar{width:4px}
::-webkit-scrollbar-track{background:#080c1a}
::-webkit-scrollbar-thumb{background:#1e3a5f;border-radius:2px}
</style>
</head>
<body>
<div class="hdr">
  <h1>⚡ 实时数据管道 — Kafka → Flink → Hive 全链路监控</h1>
  <div class="live"><div class="dot"></div><span>LIVE</span></div>
  <div class="ts" id="clock">--:--:--</div>
</div>

<div class="grid">
  <!-- KPI Row -->
  <div class="panel full">
    <div class="kpi-row">
      <div class="kpi"><div class="kval b" id="kpi_events">0</div><div class="klab">总事件数</div></div>
      <div class="kpi"><div class="kval c" id="kpi_throughput">0</div><div class="klab">吞吐量 MB/s</div></div>
      <div class="kpi"><div class="kval g" id="kpi_etl">0</div><div class="klab">ETL周期</div></div>
      <div class="kpi"><div class="kval o" id="kpi_alerts">0</div><div class="klab">活跃告警</div></div>
      <div class="kpi"><div class="kval" style="color:#ab47bc" id="kpi_health">100</div><div class="klab">管道健康度 %</div></div>
      <div class="kpi"><div class="kval" style="color:#78909c" id="kpi_uptime">0s</div><div class="klab">运行时间</div></div>
    </div>
  </div>

  <!-- Kafka Topics -->
  <div class="panel">
    <h2>📨 Kafka — Topic 实时吞吐量</h2>
    <div id="kafka_bars"></div>
    <div style="margin-top:8px;font-size:10px;color:#546e7a">
      总吞吐: <span id="kafka_total" style="color:#4fc3f7">0</span> msgs/s
    </div>
  </div>

  <!-- Flink Jobs -->
  <div class="panel">
    <h2>⚡ Flink — 流处理作业</h2>
    <div class="flink-cards" id="flink_cards"></div>
  </div>

  <!-- Hive ETL -->
  <div class="panel">
    <h2>🐝 Hive — ETL批处理管道</h2>
    <div class="etl-phases" id="etl_phases">
      <div class="etl-phase"><div class="pn">ODS</div>Landing</div>
      <div class="etl-phase"><div class="pn">DWD</div>Cleaning</div>
      <div class="etl-phase"><div class="pn">DWS</div>Aggregation</div>
      <div class="etl-phase"><div class="pn">ADS</div>Indicators</div>
      <div class="etl-phase"><div class="pn">Quality</div>Check</div>
    </div>
    <div class="progress-bar"><div class="fill" id="etl_progress" style="width:0%"></div></div>
    <div style="font-size:10px;color:#546e7a;text-align:center">
      阶段: <span id="etl_phase_name" style="color:#4fc3f7">ODS_Landing</span> |
      周期: <span id="etl_cycle" style="color:#66bb6a">#0</span> |
      累计行数: <span id="etl_rows" style="color:#4fc3f7">0</span>
    </div>
  </div>

  <!-- Pipeline Topology -->
  <div class="panel full">
    <h2>🔗 全链路拓扑 — 实时数据流动画</h2>
    <div class="topo" id="topology">
      <div class="topo-node"><span class="nicon">📡</span><span class="nlabel">传感器</span><span class="nrate">→</span></div>
      <div class="topo-arrow">▸</div>
      <div class="topo-node" id="tn_kafka"><span class="nicon">📨</span><span class="nlabel">Kafka</span><span class="nrate" id="tn_kafka_rate">0/s</span></div>
      <div class="topo-arrow">▸</div>
      <div class="topo-node" id="tn_flink"><span class="nicon">⚡</span><span class="nlabel">Flink</span><span class="nrate" id="tn_flink_rate">0/s</span></div>
      <div class="topo-arrow">▸</div>
      <div class="topo-node"><span class="nicon">🗄️</span><span class="nlabel">Hive ETL</span><span class="nrate" id="tn_hive_phase">ODS</span></div>
      <div class="topo-arrow">▸</div>
      <div class="topo-node"><span class="nicon">✅</span><span class="nlabel">数据质量</span><span class="nrate" id="tn_quality">--</span></div>
      <div class="topo-arrow">▸</div>
      <div class="topo-node"><span class="nicon">📊</span><span class="nlabel">仪表盘</span><span class="nrate">LIVE</span></div>
    </div>
  </div>

  <!-- Live Data Feed -->
  <div class="panel">
    <h2>📋 实时数据记录</h2>
    <div class="data-feed" id="data_feed"><div style="color:#546e7a">等待数据...</div></div>
  </div>

  <!-- Alerts -->
  <div class="panel">
    <h2>🚨 实时告警</h2>
    <div class="alerts" id="alerts_panel"><div style="color:#546e7a">暂无告警</div></div>
  </div>
</div>

<script>
const ETL_NAMES = ["ODS_Landing","DWD_Cleaning","DWS_Aggregation","ADS_Indicators","Quality_Check"];

// Connect to SSE stream
const es = new EventSource("/stream");
es.onmessage = function(e) {
  const d = JSON.parse(e.data);
  updateKPI(d);
  updateKafka(d.kafka);
  updateFlink(d.flink);
  updateETL(d.etl);
  updateFeed(d.new_records || []);
  updateAlerts(d.new_alerts || []);
  updateTopology(d);
  document.getElementById("clock").textContent = d.ts;
};

function updateKPI(d) {
  const c = d.counters;
  document.getElementById("kpi_events").textContent = fmtNum(c.total_events);
  document.getElementById("kpi_throughput").textContent = c.throughput_mbps;
  document.getElementById("kpi_etl").textContent = c.etl_cycles_completed;
  document.getElementById("kpi_alerts").textContent = c.active_alerts;
  document.getElementById("kpi_health").textContent = d.health;
  const s = d.counters.uptime_seconds;
  document.getElementById("kpi_uptime").textContent =
    s >= 3600 ? Math.floor(s/3600)+"h"+Math.floor(s%3600/60)+"m" :
    s >= 60 ? Math.floor(s/60)+"m"+s%60+"s" : s+"s";
}

function fmtNum(n) {
  if (n >= 1e6) return (n/1e6).toFixed(1)+"M";
  if (n >= 1e3) return (n/1e3).toFixed(1)+"K";
  return n.toString();
}

function updateKafka(k) {
  let html = "";
  let total = 0;
  const maxRate = Math.max(...Object.values(k).map(v=>v.msgs_per_sec), 1);
  for (const [name, v] of Object.entries(k)) {
    const pct = Math.round(v.msgs_per_sec / maxRate * 100);
    total += v.msgs_per_sec;
    html += `<div class="topic-row">
      <div class="label">${name}</div>
      <div class="bar-wrap">
        <div class="bar-fill" style="width:${pct}%;background:${v.color}">
          ${v.msgs_per_sec.toLocaleString()} msg/s
        </div>
      </div>
      <div class="val">${(v.msgs_per_sec/1000).toFixed(1)}K/s</div>
      <div class="lag" style="color:${v.lag>3000?'#ef5350':'#78909c'}">Lag:${v.lag}</div>
    </div>`;
    // Animate lag
    if (v.lag > 3000) {
      document.getElementById(`lag_${name}`)?.classList.add('r');
    }
  }
  document.getElementById("kafka_bars").innerHTML = html;
  document.getElementById("kafka_total").textContent = total.toLocaleString();
}

function updateFlink(f) {
  let html = "";
  for (const [name, j] of Object.entries(f)) {
    html += `<div class="flink-card">
      <div class="jname">${name}</div>
      <div class="jmet">
        <span>${(j.events_per_sec/1000).toFixed(1)}K</span> evt/s |
        P50:<span>${j.p50_ms}ms</span> |
        P99:<span>${j.p99_ms}ms</span> |
        CP:<span>${j.checkpoint_mb}MB</span>
      </div>
      <div class="status ${j.status}">${j.status}</div>
      <div class="jmet">
        Backlog:<span style="color:${j.backlog>100?'#ef5350':'inherit'}">${j.backlog}</span>
      </div>
    </div>`;
  }
  document.getElementById("flink_cards").innerHTML = html;
}

function updateETL(e) {
  // Phase indicators
  const phases = document.querySelectorAll(".etl-phase");
  phases.forEach((p, i) => {
    p.className = "etl-phase";
    if (i < e.current_phase) p.classList.add("done");
    if (i === e.current_phase) p.classList.add("active");
  });

  // Progress bar
  document.getElementById("etl_progress").style.width = e.phase_progress + "%";
  document.getElementById("etl_phase_name").textContent = e.phase_name;
  document.getElementById("etl_cycle").textContent = "#" + e.cycle;
}

function updateFeed(records) {
  const feed = document.getElementById("data_feed");
  if (records.length === 0) return;

  let html = "";
  for (const r of records) {
    html += `<div class="row">
      <span class="ts">${r.ts}</span>
      <span class="tp ${r.type}">${r.type.toUpperCase()}</span>
      <span class="detail">${formatRecord(r)}</span>
    </div>`;
  }
  // Prepend to show newest first
  const existing = feed.innerHTML;
  const existingRows = existing.split('<div class="row">').length - 1;
  feed.innerHTML = html + (existingRows > 30 ? '' : existing.replace('<div style="color:#546e7a">等待数据...</div>',''));
}

function formatRecord(r) {
  if (r.type === "vehicle") return `${r.road} | 车速:${r.speed}km/h | 拥堵等级:${r.jam} | 车道:${r.lane}`;
  if (r.type === "device") return `${r.device}(${r.dev_type}) | CPU:${r.cpu}% MEM:${r.mem}% TEMP:${r.temp}°C | ${r.online?"在线":"离线"}`;
  if (r.type === "alarm") return `[${r.severity}] ${r.message}`;
  return JSON.stringify(r);
}

function updateAlerts(alerts) {
  if (!alerts || alerts.length === 0) return;
  let html = "";
  for (const a of alerts) {
    html += `<div class="alert-row ${a.severity}">
      <span>${a.ts}</span>
      <span>[${a.severity}]</span>
      <span>${a.message}</span>
      <span style="color:#546e7a">@${a.source}</span>
    </div>`;
  }
  document.getElementById("alerts_panel").innerHTML = html;
}

function updateTopology(d) {
  // Animate active nodes
  const totalRate = Object.values(d.kafka).reduce((s,v)=>s+v.msgs_per_sec, 0);
  document.getElementById("tn_kafka_rate").textContent = (totalRate/1000).toFixed(0)+"K/s";
  const flinkRate = Object.values(d.flink).reduce((s,j)=>s+j.events_per_sec, 0);
  document.getElementById("tn_flink_rate").textContent = (flinkRate/1000).toFixed(0)+"K/s";
  document.getElementById("tn_hive_phase").textContent = d.etl.phase_name.replace("_"," ");
  document.getElementById("tn_quality").textContent = d.health+"%";

  // Pulse active ETL node
  const hiveNode = document.getElementById("tn_hive_phase").parentElement;
  hiveNode.classList.add("active");
  setTimeout(() => hiveNode.classList.remove("active"), 500);
}

// Initial load via API (before SSE connects)
fetch("/api/state").then(r=>r.json()).then(d=>{
  updateKPI({counters:d.counters, health:d.health});
  updateKafka(d.kafka);
  updateFlink(d.flink);
  updateETL(d.etl);
  updateFeed(d.recent_records || []);
  updateAlerts(d.recent_alerts || []);
  updateTopology({kafka:d.kafka, flink:d.flink, etl:d.etl, health:d.health});
});
</script>
</body></html>'''

# ============================================================
# Main Entry
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("  实时数据管道模拟器 — Kafka→Flink→Hive 全链路")
    print("  SSE实时推送 | 端口 8090")
    print("  http://127.0.0.1:8090")
    print("=" * 60)

    # Start simulator threads
    threads = [
        threading.Thread(target=kafka_simulator, daemon=True, name="kafka-sim"),
        threading.Thread(target=flink_simulator, daemon=True, name="flink-sim"),
        threading.Thread(target=hive_etl_simulator, daemon=True, name="hive-sim"),
        threading.Thread(target=data_generator, daemon=True, name="data-gen"),
    ]
    # 如果检测到真实 Kafka，启动消费者线程
    if KAFKA_ENABLED:
        threads.append(threading.Thread(target=kafka_consumer_thread, daemon=True, name="kafka-consumer"))
        print(f"  检测到 Kafka ({KAFKA_BOOTSTRAP}), 实时消费模式")
    for t in threads:
        t.start()
    print(f"  已启动 {len(threads)} 个模拟器线程")
    print(f"  Kafka: 4 Topics | Flink: 3 Jobs | Hive: 5-Phase ETL")
    print("=" * 60)

    try:
        from waitress import serve
        serve(app, host="0.0.0.0", port=8090, threads=8)
    except ImportError:
        app.run(host="0.0.0.0", port=8090, threaded=True)
