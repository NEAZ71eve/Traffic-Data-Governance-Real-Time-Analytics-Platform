"""数据服务可视化仪表盘 — 7大服务监控面板，零依赖，纯Flask+内联HTML/CSS/JS"""
from flask import Flask, jsonify, render_template_string, request
import sqlite3, json, random, time, os, sys
from datetime import datetime, timedelta
from collections import defaultdict

# Add python/ to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "python"))

app = Flask(__name__)
DB = os.path.join(os.path.dirname(__file__), "traffic_data.db")
start_time = time.time()
DT = "2026-06-08"

# ============================================================
# Helpers
# ============================================================
def query(sql, params=()):
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def seed_for_hour():
    """Seed random based on current hour for stable mock data"""
    random.seed(int(time.time() / 3600))

# ============================================================
# Mock Data Generators
# ============================================================
def mock_kafka_metrics():
    seed_for_hour()
    topics = [
        {"name": "traffic_vehicle", "partitions": 3, "replication": 2,
         "msgs_per_sec": random.randint(3000, 8000), "bytes_per_sec": random.randint(200000, 600000),
         "consumer_lag": random.randint(0, 3000), "status": "running"},
        {"name": "traffic_status", "partitions": 3, "replication": 2,
         "msgs_per_sec": random.randint(2000, 6000), "bytes_per_sec": random.randint(150000, 450000),
         "consumer_lag": random.randint(0, 2000), "status": "running"},
        {"name": "device_status", "partitions": 3, "replication": 2,
         "msgs_per_sec": random.randint(1000, 4000), "bytes_per_sec": random.randint(100000, 300000),
         "consumer_lag": random.randint(0, 5000), "status": "running"},
        {"name": "device_alarm", "partitions": 2, "replication": 2,
         "msgs_per_sec": random.randint(100, 800), "bytes_per_sec": random.randint(10000, 80000),
         "consumer_lag": random.randint(0, 500), "status": "running"},
    ]
    return {
        "topics": topics,
        "total_msgs_today": random.randint(5000000, 50000000),
        "total_throughput": sum(t["msgs_per_sec"] for t in topics),
        "brokers": 3,
        "brokers_online": 3,
        "status": "running"
    }

def mock_flume_metrics():
    seed_for_hour()
    return {
        "agents": [
            {"name": "flume-agent-01", "type": "spooldir", "source": "/data/logs/vehicle", "status": "running", "events_per_sec": random.randint(500, 2000)},
            {"name": "flume-agent-02", "type": "spooldir", "source": "/data/logs/device", "status": "running", "events_per_sec": random.randint(300, 1500)},
        ],
        "channels": [{"name": "memory-channel", "type": "memory", "capacity": 100000, "used": random.randint(1000, 50000), "status": "running"}],
        "sinks": [{"name": "kafka-sink", "type": "kafka", "target": "Kafka brokers", "status": "running", "batch_size": 1000}]
    }

def mock_datax_metrics():
    seed_for_hour()
    return {
        "jobs": [
            {"name": "mysql_to_hdfs_vehicle", "source": "MySQL", "target": "HDFS", "status": "success", "rows": random.randint(100000, 500000), "speed": f"{random.randint(5,20)}MB/s"},
            {"name": "mysql_to_hdfs_device", "source": "MySQL", "target": "HDFS", "status": "success", "rows": random.randint(50000, 200000), "speed": f"{random.randint(3,15)}MB/s"},
        ],
        "total_rows_today": random.randint(200000, 800000)
    }

def mock_maxwell_metrics():
    seed_for_hour()
    return {
        "status": "running",
        "binlog_position": f"mysql-bin.{random.randint(100,999):06d}:{random.randint(1000000,9999999)}",
        "tables_monitored": 6,
        "events_processed": random.randint(50000, 200000),
        "latency_ms": random.randint(10, 200)
    }

def mock_flink_metrics():
    seed_for_hour()
    jobs = [
        {"name": "TrafficVehicleCount", "status": "RUNNING", "events_per_sec": random.randint(2000, 8000),
         "latency_p50_ms": random.randint(5, 30), "latency_p99_ms": random.randint(30, 200),
         "checkpoint_size_mb": round(random.uniform(20, 150), 1), "checkpoint_duration_ms": random.randint(200, 2000),
         "backlog": random.randint(0, 500), "parallelism": 4, "uptime_h": random.randint(1, 720),
         "task_managers": 2, "slots_used": 4},
        {"name": "TrafficCongestionDetection", "status": "RUNNING", "events_per_sec": random.randint(1000, 5000),
         "latency_p50_ms": random.randint(8, 40), "latency_p99_ms": random.randint(50, 300),
         "checkpoint_size_mb": round(random.uniform(15, 120), 1), "checkpoint_duration_ms": random.randint(150, 1800),
         "backlog": random.randint(0, 300), "parallelism": 4, "uptime_h": random.randint(1, 720),
         "task_managers": 2, "slots_used": 4},
        {"name": "DeviceStatusCEP", "status": random.choice(["RUNNING"]*4 + ["RESTARTING"]), "events_per_sec": random.randint(500, 3000),
         "latency_p50_ms": random.randint(10, 50), "latency_p99_ms": random.randint(80, 400),
         "checkpoint_size_mb": round(random.uniform(10, 80), 1), "checkpoint_duration_ms": random.randint(100, 1500),
         "backlog": random.randint(0, 200), "parallelism": 4, "uptime_h": random.randint(1, 720),
         "task_managers": 2, "slots_used": 4},
    ]
    return {
        "jobs": jobs,
        "total_slots": 16,
        "used_slots": sum(j["slots_used"] for j in jobs),
        "task_managers_total": 4,
        "checkpoint_success_rate": round(random.uniform(95, 100), 1)
    }

def mock_etl_status():
    seed_for_hour()
    tasks = [
        {"name": "ODS_Ingestion", "status": "success", "duration_s": random.randint(60, 300), "rows": random.randint(500000, 2000000), "start": "01:00:00"},
        {"name": "DWD_Cleaning", "status": "success", "duration_s": random.randint(120, 600), "rows": random.randint(400000, 1800000), "start": "02:00:00"},
        {"name": "DWS_Aggregation", "status": "success", "duration_s": random.randint(60, 300), "rows": random.randint(100000, 500000), "start": "03:00:00"},
        {"name": "ADS_Indicators", "status": random.choice(["success"]*9 + ["running"]), "duration_s": random.randint(30, 180), "rows": random.randint(50000, 200000), "start": "04:00:00"},
        {"name": "Data_Quality", "status": random.choice(["success"]*8 + ["running"]), "duration_s": random.randint(30, 120), "rows": random.randint(10000, 50000), "start": "05:00:00"},
    ]
    success_count = sum(1 for t in tasks if t["status"] == "success")
    return {
        "tasks": tasks,
        "success_rate": round(success_count / len(tasks) * 100, 1),
        "total_rows_today": sum(t["rows"] for t in tasks)
    }

def mock_hdfs_metrics():
    seed_for_hour()
    return {
        "status": "running",
        "data_nodes": 3,
        "capacity_tb": 5.0,
        "used_tb": round(random.uniform(1.2, 2.5), 1),
        "datanodes_online": 3,
        "blocks_total": random.randint(50000, 200000),
        "under_replicated": random.randint(0, 100)
    }

def mock_alerts():
    seed_for_hour()
    alert_types = [
        ("P0严重", "CRITICAL", "中山路拥堵等级=5持续>30min", "14:32:01"),
        ("P1", "MAJOR", "高新区车流量突增>50%", "14:30:15"),
        ("P2", "MINOR", "老城区均速<15km/h", "14:28:40"),
        ("P1", "MAJOR", "设备CT-0234离线超过2h", "14:25:10"),
        ("P2", "MINOR", "Kafka Lag超过5000", "14:20:00"),
    ]
    return [{"level": a[0], "severity": a[1], "message": a[2], "time": a[3]} for a in alert_types]

def dot_to_json(dot_string):
    """Parse DOT format into nodes and edges JSON"""
    nodes_set = set()
    edges = []
    for line in dot_string.strip().split('\n'):
        line = line.strip()
        if '->' in line:
            parts = line.replace('"', '').split('->')
            src = parts[0].strip()
            tgt = parts[1].strip().rstrip(';')
            nodes_set.add(src)
            nodes_set.add(tgt)
            edges.append({'source': src, 'target': tgt})

    def get_layer(table_id):
        prefix = table_id.split('_')[0].upper()
        if prefix in ('ODS', 'DIM', 'DWD', 'DWS', 'ADS'):
            return prefix
        return 'OTHER'

    def format_label(table_id):
        layer = get_layer(table_id)
        short = table_id.replace('ods_', '').replace('dim_', '').replace('dwd_', '').replace('dws_', '').replace('ads_', '').replace('_di', '').replace('_zip', '').replace('_day', '').replace('_hour', '')
        return f"{layer}\n{short[:14]}"

    nodes = [{'id': n, 'label': format_label(n), 'layer': get_layer(n)} for n in sorted(nodes_set)]

    layers = {'ODS': [], 'DIM': [], 'DWD': [], 'DWS': [], 'ADS': [], 'OTHER': []}
    for n in nodes:
        layers[n['layer']].append(n['id'])

    return {'nodes': nodes, 'edges': edges, 'layers': layers}

# ============================================================
# API Endpoints
# ============================================================

# --- Tab 1: Data Collection ---
@app.route("/api/data_collection")
def api_data_collection():
    kafka = mock_kafka_metrics()
    flume = mock_flume_metrics()
    datax = mock_datax_metrics()
    maxwell = mock_maxwell_metrics()

    sources = [
        {"name": "Kafka", "type": "消息队列", "status": "running", "detail": f"{kafka['brokers_online']}/{kafka['brokers']} Brokers, {len(kafka['topics'])} Topics"},
        {"name": "Flume", "type": "日志采集", "status": "running", "detail": f"{len(flume['agents'])} Agents, {len(flume['sinks'])} Sinks"},
        {"name": "DataX", "type": "数据同步", "status": "running", "detail": f"{len(datax['jobs'])} Jobs, {datax['total_rows_today']:,} rows today"},
        {"name": "Maxwell", "type": "CDC采集", "status": maxwell["status"], "detail": f"Binlog: {maxwell['binlog_position']}, {maxwell['tables_monitored']} tables"},
    ]

    hourly = []
    for h in range(24):
        base = 5000 if (7 <= h <= 9) or (17 <= h <= 19) else 2000 if (10 <= h <= 16) else 500
        hourly.append({"h": h, "v": base + random.randint(-500, 1000)})

    return jsonify({
        "kafka": kafka,
        "sources": sources,
        "hourly": hourly,
        "total_sources": 4,
        "active_sources": 4
    })

# --- Tab 2: Stream Processing ---
@app.route("/api/stream_processing")
def api_stream_processing():
    flink = mock_flink_metrics()
    return jsonify(flink)

# --- Tab 3: Data Warehouse ---
@app.route("/api/data_warehouse")
def api_data_warehouse():
    etl = mock_etl_status()
    hdfs = mock_hdfs_metrics()

    try:
        flow = query("SELECT SUM(traffic_count) as v FROM dws_road_hour_flow WHERE dt=?", (DT,))
        total_flow = flow[0]["v"] or 0 if flow else 0
    except:
        total_flow = random.randint(2000000, 5000000)

    catalog = {
        "ODS": [t.replace('.sql', '') for t in os.listdir(os.path.join(os.path.dirname(__file__), 'sql', 'ods')) if t.endswith('.sql')],
        "DIM": [t.replace('.sql', '') for t in os.listdir(os.path.join(os.path.dirname(__file__), 'sql', 'dim')) if t.endswith('.sql')],
        "DWD": [t.replace('.sql', '') for t in os.listdir(os.path.join(os.path.dirname(__file__), 'sql', 'dwd')) if t.endswith('.sql')],
        "DWS": [t.replace('.sql', '') for t in os.listdir(os.path.join(os.path.dirname(__file__), 'sql', 'dws')) if t.endswith('.sql')],
        "ADS": [t.replace('.sql', '') for t in os.listdir(os.path.join(os.path.dirname(__file__), 'sql', 'ads')) if t.endswith('.sql')],
    }
    total_tables = sum(len(v) for v in catalog.values())

    return jsonify({
        "etl": etl,
        "hdfs": hdfs,
        "catalog": catalog,
        "total_tables": total_tables,
        "total_flow": total_flow,
        "query_count": random.randint(500, 2000),
        "data_volume_gb": round(random.uniform(50, 200), 1)
    })

# --- Tab 4: Data Quality ---
@app.route("/api/data_quality")
def api_data_quality():
    try:
        q = query("SELECT report_date as d, table_name as n, completeness_rate as c, uniqueness_rate as u, validity_rate as v, kafka_lag as l, status as st, score as s FROM data_quality_results ORDER BY report_date DESC, table_name")
        trend = query("SELECT report_date as d, AVG(score) as s, AVG(kafka_lag) as l FROM data_quality_results GROUP BY report_date ORDER BY report_date")

        if q:
            score = round(sum(x["c"] + x["u"] + x["v"] for x in q) / max(len(q), 1) / 3, 1)
            comp_avg = round(sum(x["c"] for x in q) / max(len(q), 1), 1)
            uniq_avg = round(sum(x["u"] for x in q) / max(len(q), 1), 1)
            valid_avg = round(sum(x["v"] for x in q) / max(len(q), 1), 1)
            return jsonify({
                "score": score, "comp_avg": comp_avg, "uniq_avg": uniq_avg, "valid_avg": valid_avg,
                "tables": [dict(x) for x in q],
                "trend": [{"d": t["d"], "s": round(t["s"], 1), "l": t["l"] or 0} for t in trend],
                "has_data": True
            })
    except Exception as e:
        pass

    # Mock fallback
    seed_for_hour()
    tables = ["ods_vehicle_pass_di", "ods_traffic_status_di", "ods_device_status_di", "ods_alarm_log_di",
              "dwd_vehicle_pass_di", "dwd_traffic_status_di", "dwd_device_status_di"]
    mock_tables = [{"d": DT, "n": t, "c": round(random.uniform(96, 100), 1), "u": round(random.uniform(97, 100), 1),
                     "v": round(random.uniform(96, 100), 1), "l": random.randint(100, 8000), "st": random.choice(["PASS"]*8+["WARN"]),
                     "s": round(random.uniform(85, 100), 1)} for t in tables]
    mock_trend = [{"d": (datetime.strptime(DT, "%Y-%m-%d") - timedelta(days=i)).strftime("%Y-%m-%d"),
                   "s": round(random.uniform(88, 98), 1), "l": random.randint(500, 9000)} for i in range(6, -1, -1)]
    return jsonify({
        "score": round(sum(t["s"] for t in mock_tables) / len(mock_tables), 1),
        "comp_avg": round(sum(t["c"] for t in mock_tables) / len(mock_tables), 1),
        "uniq_avg": round(sum(t["u"] for t in mock_tables) / len(mock_tables), 1),
        "valid_avg": round(sum(t["v"] for t in mock_tables) / len(mock_tables), 1),
        "tables": mock_tables, "trend": mock_trend, "has_data": False
    })

# --- Tab 5: Data Lineage ---
@app.route("/api/data_lineage")
def api_data_lineage():
    try:
        from data_lineage import DataLineageManager
        mgr = DataLineageManager()
        dot = mgr.visualize_lineage()
        data = dot_to_json(dot)
        data["total_edges"] = len(data["edges"])
        data["total_nodes"] = len(data["nodes"])
        data["ods_count"] = len(data["layers"].get("ODS", []))
        data["ads_count"] = len(data["layers"].get("ADS", []))
        data["all_table_ids"] = sorted([n["id"] for n in data["nodes"]])
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e), "nodes": [], "edges": [], "layers": {}}), 500

@app.route("/api/data_lineage/table/<name>")
def api_data_lineage_table(name):
    try:
        from data_lineage import DataLineageManager
        mgr = DataLineageManager()
        upstream = mgr.get_upstream_tables(name)
        downstream = mgr.get_downstream_tables(name)
        impact = mgr.detect_impact(name)
        return jsonify({
            "table": name,
            "upstream": sorted(upstream),
            "downstream": sorted(downstream),
            "impact": impact
        })
    except Exception as e:
        return jsonify({"error": str(e), "table": name, "upstream": [], "downstream": [], "impact": {}}), 500

# --- Tab 6: AI Assistant ---
@app.route("/api/ai_assistant")
def api_ai_assistant():
    try:
        from ai_assistant import AIAssistant
        ai = AIAssistant()
        ollama_ok = ai.ollama_online
    except:
        ollama_ok = False

    try:
        from ai_anomaly_detector import TrafficAnomalyDetector
        detector = TrafficAnomalyDetector()
        anomalies = detector.generate_anomaly_report()
    except:
        anomalies = {"total_anomalies": 0, "anomalies": []}

    return jsonify({
        "ollama_online": ollama_ok,
        "ollama_model": "qwen3:8b" if ollama_ok else "offline",
        "fallback_engine": "NL2SQL规则引擎 (在线)",
        "nl2sql_queries_today": random.randint(10, 100),
        "anomalies_detected": anomalies.get("total_anomalies", random.randint(0, 15)),
        "anomaly_types": ["设备异常", "流量异常", "速度异常", "时序断层"],
        "query_types": [
            {"intent": "流量查询", "example": "今天长安街的车流量是多少"},
            {"intent": "拥堵查询", "example": "昨天最拥堵的5条路"},
            {"intent": "设备查询", "example": "设备CT0001的健康评分"},
            {"intent": "排行查询", "example": "车流量TOP10的道路"},
            {"intent": "趋势查询", "example": "最近7天长安街的车速趋势"},
            {"intent": "对比查询", "example": "对比新城区和老城区的拥堵率"},
            {"intent": "汇总查询", "example": "今天全市总车流量"},
            {"intent": "告警查询", "example": "最近24小时有哪些设备告警"},
        ]
    })

def _detect_nl2sql_intent(query_text):
    """Detect NL2SQL intent without importing external modules"""
    import re
    patterns = [
        (r'拥堵.*道路|拥堵.*排行|拥堵.*TOP|最.*堵|拥堵.*路段|哪些.*路.*堵', 'road_jam_rank'),
        (r'车流量|通行.*统计|.*流量.*排行|.*流量.*多少|.*多少.*车', 'road_flow'),
        (r'设备.*健康|健康.*评分|健康.*排行|设备.*评分', 'device_health'),
        (r'故障.*统计|告警.*统计|故障.*设备|告警.*设备|故障.*最多|温度过高|CPU.*高', 'device_fault'),
        (r'区域.*拥堵|哪个区|各区.*拥堵|哪个.*区域.*堵', 'area_congestion'),
        (r'离线.*设备|断连|不在线|设备.*离线|哪些设备.*离线', 'device_offline'),
        (r'高峰|早晚高峰|高峰.*分析|哪个.*时段.*堵', 'peak_analysis'),
        (r'数据质量|质量.*检查|质量.*报表|质量.*多少', 'quality_check'),
    ]
    for pattern, intent in patterns:
        if re.search(pattern, query_text):
            return intent
    return 'unknown'

def _extract_limit(query_text):
    """Extract limit number from query"""
    import re
    num_match = re.search(r'(\d+)条|(\d+)个|(\d+)台|前(\d+)|TOP\s*(\d+)', query_text, re.IGNORECASE)
    if num_match:
        return int([g for g in num_match.groups() if g][0])
    return 5

def _extract_order(query_text):
    """Extract sort order from query"""
    if any(w in query_text for w in ['最低', '最少', '最小', '最差', '最不']):
        return 'ASC'
    return 'DESC'

@app.route("/api/ai_assistant/query", methods=["POST"])
def api_ai_assistant_query():
    data = request.get_json(force=True)
    question = data.get("question", "").strip()
    if not question:
        return jsonify({"error": "问题不能为空"}), 400

    # Use inline intent detection + SQL builder (always works, no external deps)
    intent = _detect_nl2sql_intent(question)
    limit = _extract_limit(question)
    order = _extract_order(question)

    if intent == "road_jam_rank":
        sql = f"""SELECT road_name, avg_jam_level, avg_congestion_rate
FROM ads_top_jam_roads
WHERE dt = '${{yesterday}}'
ORDER BY rank_num
LIMIT {limit};"""
    elif intent == "road_flow":
        sql = f"""SELECT r.road_name, SUM(f.traffic_count) as total_flow, ROUND(AVG(f.avg_speed),1) as avg_speed
FROM dws_road_hour_flow f JOIN dim_road r ON f.road_id = r.road_id
WHERE f.dt = '${{today}}'
GROUP BY r.road_name
ORDER BY total_flow {order}
LIMIT {limit};"""
    elif intent == "device_health":
        sql = f"""SELECT device_name, health_score, health_level, online_rate, avg_cpu_usage, avg_mem_usage
FROM ads_device_health_score
WHERE dt = '${{today}}'
ORDER BY health_score {order}
LIMIT {limit};"""
    elif intent == "device_fault":
        sql = f"""SELECT device_name, fault_rate
FROM ads_device_fault_top
WHERE dt = '${{today}}'
ORDER BY rank_num
LIMIT {limit};"""
    elif intent == "device_offline":
        sql = f"""SELECT device_id, device_name, device_type, online_rate
FROM ads_device_health_score
WHERE dt = '${{today}}' AND online_rate < 100
ORDER BY online_rate ASC
LIMIT {limit};"""
    elif intent == "area_congestion":
        sql = """SELECT a.area_name, ROUND(AVG(j.jam_level),1) as avg_jam, COUNT(*) as samples
FROM dws_area_jam_hour j
JOIN dim_area a ON j.area_id = a.area_id
WHERE j.dt = '${today}'
GROUP BY a.area_name
ORDER BY avg_jam DESC;"""
    elif intent == "peak_analysis":
        sql = """SELECT f.hour, SUM(f.traffic_count) as total_flow, ROUND(AVG(f.avg_speed),1) as avg_speed
FROM dws_road_hour_flow f
WHERE f.dt = '${today}' AND f.hour BETWEEN 7 AND 19
GROUP BY f.hour ORDER BY total_flow DESC;"""
    elif intent == "quality_check":
        sql = """SELECT table_name, completeness_rate, uniqueness_rate, validity_rate, status, score
FROM data_quality_results
WHERE report_date = '${today}'
ORDER BY score ASC;"""
    else:
        sql = f"-- 意图: {intent} | 问题: {question}\n-- 支持的查询类型:\n-- 拥堵排行、车流量、设备健康、故障统计、离线设备、区域拥堵、高峰分析、数据质量\nSELECT '请尝试以上查询类型' as hint;"

    # Mock execution result with appropriate columns based on intent
    if intent in ("road_jam_rank",):
        mock_result = {
            "columns": ["road_name", "avg_jam_level", "avg_congestion_rate"],
            "rows": [
                ["中山路", 4.8, 85.2], ["人民路", 4.2, 78.5], ["解放路", 3.9, 72.1],
                ["建设路", 3.5, 65.8], ["长安街", 3.2, 58.3],
            ][:limit],
            "execution_time_ms": random.randint(30, 300),
            "row_count": min(limit, 5)
        }
    elif intent in ("device_health",):
        mock_result = {
            "columns": ["device_name", "health_score", "health_level", "online_rate", "avg_cpu_usage", "avg_mem_usage"],
            "rows": [
                ["CT-0234", 45.2, "较差", 85.0, 92.5, 78.3],
                ["CM-0156", 58.7, "较差", 90.0, 88.2, 85.1],
                ["SG-0089", 72.3, "良好", 95.0, 75.0, 65.0],
            ][:limit],
            "execution_time_ms": random.randint(30, 300),
            "row_count": min(limit, 3)
        }
    elif intent in ("device_offline",):
        mock_result = {
            "columns": ["device_id", "device_name", "device_type", "online_rate"],
            "rows": [
                ["CT-0234", "中山路摄像头#3", "摄像头", 65.0],
                ["SG-0089", "人民路信号灯#1", "信号灯", 72.0],
                ["CM-0156", "解放路流量计#2", "流量计", 78.0],
            ][:limit],
            "execution_time_ms": random.randint(30, 300),
            "row_count": min(limit, 3)
        }
    elif intent in ("device_fault",):
        mock_result = {
            "columns": ["device_name", "fault_rate", "fault_count", "last_fault_time"],
            "rows": [
                ["CT-0234", 12.5, 5, "2026-06-08 14:30"],
                ["CM-0156", 8.2, 3, "2026-06-08 10:15"],
                ["SG-0089", 5.1, 2, "2026-06-07 22:00"],
            ][:limit],
            "execution_time_ms": random.randint(30, 300),
            "row_count": min(limit, 3)
        }
    elif intent in ("road_flow",):
        mock_result = {
            "columns": ["road_name", "total_flow", "avg_speed"],
            "rows": [
                ["长安街", 8523, 45.2],
                ["东三环路", 7931, 38.7],
                ["深南大道", 6512, 42.1],
            ][:limit],
            "execution_time_ms": random.randint(30, 300),
            "row_count": min(limit, 3)
        }
    elif intent in ("area_congestion",):
        mock_result = {
            "columns": ["area_name", "avg_jam", "samples"],
            "rows": [
                ["高新区", 4.2, 24],
                ["老城区", 3.8, 24],
                ["新城区", 2.5, 24],
            ],
            "execution_time_ms": random.randint(30, 300),
            "row_count": 3
        }
    elif intent in ("peak_analysis",):
        mock_result = {
            "columns": ["hour", "total_flow", "avg_speed"],
            "rows": [
                [8, 12500, 28.5],
                [9, 11800, 32.1],
                [18, 11200, 29.8],
            ],
            "execution_time_ms": random.randint(30, 300),
            "row_count": 3
        }
    elif intent in ("quality_check",):
        mock_result = {
            "columns": ["table_name", "completeness_rate", "uniqueness_rate", "validity_rate", "status", "score"],
            "rows": [
                ["ods_vehicle_pass_di", 99.8, 99.5, 99.2, "PASS", 98.5],
                ["dwd_vehicle_pass_di", 98.5, 97.2, 96.8, "PASS", 95.2],
                ["dws_road_hour_flow", 97.1, 95.5, 94.2, "WARN", 91.8],
            ],
            "execution_time_ms": random.randint(30, 300),
            "row_count": 3
        }
    else:
        mock_result = {
            "columns": ["name", "value1", "value2"],
            "rows": [
                ["结果1", random.randint(5000, 10000), round(random.uniform(25, 55), 1)],
                ["结果2", random.randint(4000, 8000), round(random.uniform(30, 60), 1)],
                ["结果3", random.randint(3000, 7000), round(random.uniform(35, 65), 1)],
            ],
            "execution_time_ms": random.randint(50, 500),
            "row_count": 3
        }

    return jsonify({
        "question": question,
        "intent": intent,
        "sql": sql,
        "result": mock_result
    })

# --- Tab 7: Full Chain ---
@app.route("/api/full_chain")
def api_full_chain():
    kafka = mock_kafka_metrics()
    flink = mock_flink_metrics()
    hdfs = mock_hdfs_metrics()
    etl = mock_etl_status()

    components = [
        {"name": "传感器/终端", "icon": "📡", "status": "running", "detail": "10台设备在线", "uptime": "720h"},
        {"name": "Apache Kafka", "icon": "📨", "status": kafka["status"], "detail": f'{kafka["brokers_online"]} Brokers, {len(kafka["topics"])} Topics', "uptime": "720h"},
        {"name": "Apache Flink", "icon": "⚡", "status": "running", "detail": f'{len(flink["jobs"])} Jobs, {flink["used_slots"]}/{flink["total_slots"]} Slots', "uptime": f'{flink["jobs"][0]["uptime_h"]}h'},
        {"name": "HDFS", "icon": "🗄️", "status": hdfs["status"], "detail": f'{hdfs["used_tb"]}TB/{hdfs["capacity_tb"]}TB', "uptime": "720h"},
        {"name": "Apache Hive", "icon": "🐝", "status": "running", "detail": "5层数仓, 24张表", "uptime": "720h"},
        {"name": "数据质量", "icon": "✅", "status": "running", "detail": f'ETL成功率: {etl["success_rate"]}%', "uptime": "168h"},
        {"name": "仪表盘", "icon": "📊", "status": "running", "detail": "7服务面板, 端口8089", "uptime": f'{int((time.time()-start_time)/3600)}h'},
    ]

    running = sum(1 for c in components if c["status"] == "running")
    health = round(running / len(components) * 100, 1)

    pipeline = [
        {"stage": "传感器", "icon": "📡", "status": "running", "desc": "10台设备"},
        {"stage": "Kafka", "icon": "📨", "status": "running", "desc": "4 Topics"},
        {"stage": "Flink", "icon": "⚡", "status": "running", "desc": "3 Jobs"},
        {"stage": "HDFS/Hive", "icon": "🗄️", "status": "running", "desc": "5层数仓"},
        {"stage": "数据质量", "icon": "✅", "status": "running", "desc": "4维度检查"},
        {"stage": "仪表盘", "icon": "📊", "status": "running", "desc": "实时呈现"},
    ]

    return jsonify({
        "health": health,
        "running": running,
        "total": len(components),
        "components": components,
        "pipeline": pipeline,
        "data_freshness_s": random.randint(3, 15),
        "alerts": mock_alerts()
    })

# ============================================================
# HTML Template
# ============================================================
HTML = r'''<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><title>数据服务可视化 — 7大服务监控面板</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0e27;color:#ccc;font-family:"Microsoft YaHei","PingFang SC",sans-serif;font-size:13px}
.hdr{background:linear-gradient(90deg,#0d1b3e,#142850);padding:10px 20px;display:flex;justify-content:space-between;align-items:center;border-bottom:2px solid #1e3a5f}
.hdr h1{font-size:18px;color:#4fc3f7}.hdr span{color:#78909c;font-size:12px}
.nav{display:flex;gap:2px;padding:0 20px;background:#0c1530;border-bottom:1px solid #1e3a5f;flex-wrap:wrap}
.nav button{background:none;border:none;color:#78909c;padding:10px 14px;cursor:pointer;font-size:12px;border-bottom:2px solid transparent;white-space:nowrap;transition:all .2s}
.nav button:hover,.nav button.on{color:#4fc3f7;border-color:#4fc3f7}
.tab{display:none;padding:14px 20px}.tab.active{display:block}
.row{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:12px}
.c{background:#111a33;border:1px solid #1e3a5f;border-radius:6px;padding:12px;flex:1;min-width:200px}
.c.w2{flex:2;min-width:380px}.c.w3{flex:3;min-width:500px}.c.fw{flex:1 1 100%}
.c h3{font-size:12px;color:#78909c;margin-bottom:8px;text-transform:uppercase;letter-spacing:1px}
.bn{font-size:36px;font-weight:bold;text-align:center;padding:6px 0}
.bn .u{font-size:13px;color:#78909c;font-weight:normal}
.bn.g{color:#66bb6a}.bn.b{color:#42a5f5}.bn.o{color:#ffa726}.bn.r{color:#ef5350}.bn.c{color:#26c6da}.bn.p{color:#ab47bc}.bn.y{color:#ffd54f}
table{width:100%;border-collapse:collapse;font-size:12px}
th{text-align:left;padding:6px 8px;background:#1a2744;color:#78909c;font-weight:normal;font-size:11px}
td{padding:5px 8px;border-bottom:1px solid #1a2744}
tr:hover{background:rgba(79,195,247,.05)}
.tag{padding:1px 6px;border-radius:2px;font-size:10px}.tag.g{background:#1b5e20;color:#66bb6a}.tag.o{background:#5d4037;color:#ffa726}.tag.r{background:#4a1a1a;color:#ef5350}.tag.b{background:#0d3b5e;color:#42a5f5}.tag.p{background:#3a1a5e;color:#ab47bc}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px}.dot.g{background:#66bb6a;box-shadow:0 0 5px #66bb6a}.dot.r{background:#ef5350}.dot.o{background:#ffa726}
.bar{height:18px;background:#1a2744;border-radius:3px;overflow:hidden;margin:2px 0}.bar div{height:100%;border-radius:3px;transition:width .3s}
.g2{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}
.sc{background:#111a33;border:1px solid #1e3a5f;border-radius:6px;padding:12px}
.sc h3{font-size:13px;color:#4fc3f7;margin-bottom:8px;display:flex;align-items:center}
.sc .st{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #1a2744;font-size:12px}
.sc .st .lb{color:#78909c}.sc .st .vl{color:#ccc;font-weight:bold}
@keyframes p{0%,100%{opacity:1}50%{opacity:.6}}.pulse{animation:p 2s infinite}
.reset{font-size:11px;color:#546e7a;text-align:right;padding:4px 20px}
.chain{display:flex;align-items:center;justify-content:center;flex-wrap:wrap;gap:0;padding:8px 0;margin-bottom:10px}
.chain .node{text-align:center;padding:8px 10px;background:#1a2744;border-radius:5px;min-width:60px}
.chain .node .icon{font-size:16px}.chain .node .lbl{font-size:10px;color:#78909c;margin-top:2px}.chain .node .st{font-size:9px;color:#66bb6a;margin-top:1px}
.chain .arrow{color:#4fc3f7;font-size:14px}

/* Layer flow */
.layer-flow{display:flex;align-items:center;justify-content:center;gap:6px;padding:12px 0;flex-wrap:wrap}
.layer-box{text-align:center;background:#1a2744;border:2px solid #1e3a5f;border-radius:8px;padding:10px 14px;min-width:100px}
.layer-box .ly{font-size:14px;font-weight:bold;margin-bottom:3px}
.layer-box .cnt{font-size:11px;color:#78909c}.layer-box .tbl{font-size:9px;color:#546e7a;margin-top:2px;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.layer-arrow{color:#4fc3f7;font-size:18px;font-weight:bold}

/* Lineage */
.lg{width:100%;min-height:480px;background:#0d1525;border:1px solid #1e3a5f;border-radius:6px;position:relative;overflow:auto}
.lg svg{width:100%;height:100%}
.lnode rect{cursor:pointer;transition:stroke .2s}
.lnode:hover rect{stroke-width:2.5}
.linfo{background:#111a33;border:1px solid #1e3a5f;border-radius:6px;padding:10px;margin-top:10px;min-height:60px}
.linfo h4{color:#4fc3f7;font-size:13px;margin-bottom:6px}
.linfo p{font-size:11px;color:#78909c;margin:2px 0}
.lselect{background:#0d1525;border:1px solid #1e3a5f;color:#ccc;padding:6px 10px;border-radius:4px;font-size:12px;margin-right:8px}

/* Query area */
.qbox{display:flex;gap:8px;padding:4px 0}
.qbox input{flex:1;background:#0d1525;border:1px solid #1e3a5f;color:#ccc;padding:8px 12px;border-radius:4px;font-size:13px}
.qbox input:focus{outline:none;border-color:#4fc3f7}
.qbox button{background:#1e3a5f;color:#4fc3f7;border:1px solid #4fc3f7;padding:8px 16px;border-radius:4px;cursor:pointer;font-size:12px;white-space:nowrap}
.qbox button:hover{background:#2a5080}
.qres{margin-top:8px;background:#0d1525;border:1px solid #1e3a5f;border-radius:4px;padding:10px;max-height:300px;overflow-y:auto}
.qres pre{font-size:11px;color:#66bb6a;white-space:pre-wrap;word-break:break-all}
.qres .sql-label{color:#ffa726;font-size:10px;margin-bottom:3px}

/* Expandable tree */
.tree-item{padding:2px 0;cursor:pointer;user-select:none}
.tree-item .expand{display:inline-block;width:14px;font-size:10px;color:#78909c}
.tree-children{padding-left:16px;display:none}
.tree-children.open{display:block}

/* KPI mini trend */
.kpi-mini{font-size:11px;margin-top:4px;color:#78909c}

/* Tab accent borders */
.tab[data-accent] .c{border-left:3px solid transparent}
#t1 .c{border-left-color:#42a5f5}#t2 .c{border-left-color:#ffa726}#t3 .c{border-left-color:#66bb6a}
#t4 .c{border-left-color:#ab47bc}#t5 .c{border-left-color:#26c6da}#t6 .c{border-left-color:#ef5350}#t7 .c{border-left-color:#ffd54f}
</style></head><body>
<div class="hdr"><h1>&#x1f4e1; 数据服务可视化监控平台</h1><div><span class="pulse">&#x26a1; LIVE</span> &nbsp; <span id="clock">--</span></div></div>
<div class="nav">
<button class="on" onclick="sw(this,'#t1')">&#x1f4e8; 数据采集</button>
<button onclick="sw(this,'#t2')">&#x26a1; 流计算</button>
<button onclick="sw(this,'#t3')">&#x1f5c4; 离线数仓</button>
<button onclick="sw(this,'#t4')">&#x2705; 数据质量</button>
<button onclick="sw(this,'#t5')">&#x1f517; 数据血缘</button>
<button onclick="sw(this,'#t6')">&#x1f916; AI助手</button>
<button onclick="sw(this,'#t7')">&#x1f310; 全链路</button>
</div>
<div class="reset">&#x26a1; 数据刷新: 15秒 | 数据源: SQLite + Mock | 7服务面板 | 端口8089</div>

<!-- TAB 1: Data Collection -->
<div id="t1" class="tab active" data-accent="b">
<div class="row">
<div class="c"><h3>&#x1f4e8; 今日总消息量</h3><div class="bn b" id="d1_total">--<br><span class="u">条</span></div></div>
<div class="c"><h3>&#x26a1; 实时吞吐量</h3><div class="bn g" id="d1_tput">--<br><span class="u">msg/s</span></div></div>
<div class="c"><h3>&#x1f5c2; 活跃数据源</h3><div class="bn b" id="d1_src">--<br><span class="u">个</span></div></div>
<div class="c"><h3>&#x23f3; Consumer Lag</h3><div class="bn o" id="d1_lag">--<br><span class="u">条</span></div></div>
</div>
<div class="row">
<div class="c w2"><h3>&#x1f4ca; 各Topic消息分布 (条/秒)</h3><div id="d1_topics"></div></div>
<div class="c"><h3>&#x1f4c8; 24小时采集速率趋势</h3><div id="d1_hourly"></div></div>
</div>
<div class="row">
<div class="c fw"><h3>&#x1f5c2; 数据源状态总览</h3><table id="d1_sources"></table></div>
</div>
</div>

<!-- TAB 2: Stream Processing -->
<div id="t2" class="tab" data-accent="o">
<div class="row">
<div class="c"><h3>&#x2699; 活跃Job数</h3><div class="bn b" id="d2_jobs">--</div></div>
<div class="c"><h3>&#x26a1; 总吞吐量</h3><div class="bn g" id="d2_tput">--<br><span class="u">events/s</span></div></div>
<div class="c"><h3>&#x23f1; 平均延迟(P50)</h3><div class="bn o" id="d2_lat">--<br><span class="u">ms</span></div></div>
<div class="c"><h3>&#x1f4be; Checkpoint成功率</h3><div class="bn g" id="d2_ck">--<br><span class="u">%</span></div></div>
</div>
<div class="row"><div class="c fw"><h3>&#x1f4ca; 各Job吞吐量对比</h3><div id="d2_jobbars"></div></div></div>
<div class="g2" id="d2_cards"></div>
</div>

<!-- TAB 3: Data Warehouse -->
<div id="t3" class="tab" data-accent="g">
<div class="row">
<div class="c"><h3>&#x1f4ca; 数仓总表数</h3><div class="bn b" id="d3_tbl">--</div></div>
<div class="c"><h3>&#x2705; ETL成功率</h3><div class="bn g" id="d3_etl">--<br><span class="u">%</span></div></div>
<div class="c"><h3>&#x1f4e6; 今日处理数据量</h3><div class="bn b" id="d3_vol">--<br><span class="u">GB</span></div></div>
<div class="c"><h3>&#x1f50d; 今日查询数</h3><div class="bn g" id="d3_qry">--</div></div>
</div>
<div class="row"><div class="c fw"><h3>&#x1f3d7; 数仓分层架构 (ODS → DIM → DWD → DWS → ADS)</h3><div class="layer-flow" id="d3_layers"></div></div></div>
<div class="row">
<div class="c w2"><h3>&#x2699; ETL任务状态</h3><table id="d3_etltbl"></table></div>
<div class="c"><h3>&#x1f4c1; HDFS存储</h3><div id="d3_hdfs"></div></div>
</div>
<div class="row"><div class="c fw"><h3>&#x1f4da; 表目录 (按层展开)</h3><div id="d3_catalog"></div></div></div>
</div>

<!-- TAB 4: Data Quality -->
<div id="t4" class="tab" data-accent="p">
<div class="row">
<div class="c"><h3>&#x2b50; 质量总分</h3><div class="bn g" id="d4_score">--</div></div>
<div class="c"><h3>&#x1f4cb; 完整率均值</h3><div class="bn b" id="d4_comp">--<br><span class="u">%</span></div></div>
<div class="c"><h3>&#x1f300; 唯一率均值</h3><div class="bn p" id="d4_uniq">--<br><span class="u">%</span></div></div>
<div class="c"><h3>&#x2705; 合法性均值</h3><div class="bn g" id="d4_valid">--<br><span class="u">%</span></div></div>
</div>
<div class="row">
<div class="c w2"><h3>&#x1f4c8; 7天质量评分趋势</h3><div id="d4_trend"></div></div>
<div class="c"><h3>&#x23f3; Kafka Lag趋势</h3><div id="d4_lag"></div></div>
</div>
<div class="row"><div class="c fw"><h3>&#x1f4cb; 各表质量详情</h3><table id="d4_tbl"></table></div></div>
</div>

<!-- TAB 5: Data Lineage -->
<div id="t5" class="tab" data-accent="c">
<div class="row">
<div class="c"><h3>&#x1f517; 血缘边数</h3><div class="bn c" id="d5_edges">--</div></div>
<div class="c"><h3>&#x1f4ca; 涉及表数</h3><div class="bn b" id="d5_nodes">--</div></div>
<div class="c"><h3>&#x1f4e5; ODS层表</h3><div class="bn b" id="d5_ods">--</div></div>
<div class="c"><h3>&#x1f4e4; ADS层表</h3><div class="bn r" id="d5_ads">--</div></div>
</div>
<div class="row">
<div class="c fw"><h3>&#x1f30a; 数据血缘图谱 (点击节点查看上下游) &nbsp; <select class="lselect" id="d5_sel" onchange="selectNode(this.value)"><option value="">-- 搜索/选择表 --</option></select></h3><div class="lg" id="d5_graph"></div></div>
</div>
<div class="row">
<div class="c w2"><h3>&#x2b06; 上游依赖 (Upstream)</h3><div id="d5_up"></div></div>
<div class="c w2"><h3>&#x2b07; 下游影响 (Downstream)</h3><div id="d5_down"></div></div>
</div>
</div>

<!-- TAB 6: AI Assistant -->
<div id="t6" class="tab" data-accent="r">
<div class="row">
<div class="c"><h3>&#x1f4ac; NL2SQL查询</h3><div class="bn b" id="d6_nlq">--<br><span class="u">今日</span></div></div>
<div class="c"><h3>&#x1f50d; 异常检测</h3><div class="bn r" id="d6_ano">--<br><span class="u">个异常</span></div></div>
<div class="c"><h3>&#x1f3ae; Ollama状态</h3><div class="bn g" id="d6_oll">--</div></div>
<div class="c"><h3>&#x1f9e0; 规则引擎</h3><div class="bn b" id="d6_fb">--</div></div>
</div>
<div class="row"><div class="c fw">
<h3>&#x1f4ac; 自然语言查询 (NL2SQL)</h3>
<div class="qbox"><input id="d6_qin" placeholder="输入查询问题，例如: 昨天最拥堵的5条路" onkeydown="if(event.keyCode==13)doQuery()"><button onclick="doQuery()">&#x1f50d; 查询</button></div>
<div style="margin-top:4px"><span style="font-size:10px;color:#78909c">示例: </span><a href="javascript:" onclick="setQuery('昨天最拥堵的5条路')" style="color:#4fc3f7;font-size:10px;margin-right:8px">拥堵排行</a><a href="javascript:" onclick="setQuery('今天长安街的车流量')" style="color:#4fc3f7;font-size:10px;margin-right:8px">流量查询</a><a href="javascript:" onclick="setQuery('设备健康评分最低的3台设备')" style="color:#4fc3f7;font-size:10px">设备查询</a></div>
<div class="qres" id="d6_qres" style="display:none"></div>
</div></div>
<div class="row">
<div class="c w2"><h3>&#x1f4cb; 支持的查询类型</h3><table id="d6_types"></table></div>
<div class="c"><h3>&#x26a0; 最近异常检测</h3><div id="d6_alist"></div></div>
</div>
</div>

<!-- TAB 7: Full Chain -->
<div id="t7" class="tab" data-accent="y">
<div class="row">
<div class="c"><h3>&#x2764; 链路健康分</h3><div class="bn g" id="d7_health">--</div></div>
<div class="c"><h3>&#x1f514; 活跃告警</h3><div class="bn r" id="d7_alerts">--</div></div>
<div class="c"><h3>&#x2705; 运行组件</h3><div class="bn b" id="d7_run">--<br><span class="u">/ 7</span></div></div>
<div class="c"><h3>&#x23f1; 数据新鲜度</h3><div class="bn g" id="d7_fresh">--<br><span class="u">秒前</span></div></div>
</div>
<div class="row"><div class="c fw"><h3>&#x1f310; 全链路拓扑</h3><div class="chain" id="d7_chain"></div></div></div>
<div class="row">
<div class="c w2"><h3>&#x2699; 组件状态总览</h3><table id="d7_comps"></table></div>
<div class="c"><h3>&#x1f514; 最近告警</h3><table id="d7_altbl"></table></div>
</div>
</div>

<script>
// ==================== Globals ====================
function sw(b,id){document.querySelectorAll('.nav button').forEach(function(x){x.classList.remove('on')});b.classList.add('on');document.querySelectorAll('.tab').forEach(function(x){x.classList.remove('active')});var t=document.querySelector(id);t.classList.add('active');var fn=window['load'+id.substring(2)];if(fn)fn()}
function bar(val,max,cls){var p=Math.min(100,val/max*100);return'<div class="bar"><div class="'+cls+'" style="width:'+p.toFixed(1)+'%"></div></div><span style="font-size:10px;color:#78909c">'+val+'</span>'}
function fmt(n){return n!=null?Number(n).toLocaleString():'--'}
function clockTick(){var n=new Date();document.getElementById('clock').textContent=n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0')+'-'+String(n.getDate()).padStart(2,'0')+' '+String(n.getHours()).padStart(2,'0')+':'+String(n.getMinutes()).padStart(2,'0')+':'+String(n.getSeconds()).padStart(2,'0')}
setInterval(clockTick,1000);clockTick();

// ==================== Tab 1: Data Collection ====================
async function load1(){
var d=await fetch('/api/data_collection').then(function(r){return r.json()});
document.getElementById('d1_total').innerHTML=fmt(d.kafka.total_msgs_today)+'<br><span class="u">条</span>';
document.getElementById('d1_tput').innerHTML=fmt(d.kafka.total_throughput)+'<br><span class="u">msg/s</span>';
document.getElementById('d1_src').innerHTML=d.active_sources+'<br><span class="u">/ '+d.total_sources+' 个</span>';
var lag=d.kafka.topics.reduce(function(s,t){return s+t.consumer_lag},0);
document.getElementById('d1_lag').innerHTML=fmt(lag)+'<br><span class="u">条</span>';

var maxT=Math.max.apply(null,d.kafka.topics.map(function(t){return t.msgs_per_sec}));
document.getElementById('d1_topics').innerHTML=d.kafka.topics.map(function(t){
return'<div style="display:flex;align-items:center;gap:8px;padding:3px 0"><span style="width:120px;font-size:11px;color:#ccc">'+t.name+'</span>'+bar(t.msgs_per_sec,maxT,'b')+'<span style="font-size:10px;color:#546e7a;margin-left:4px">Lag:'+fmt(t.consumer_lag)+'</span></div>';
}).join('');

var maxH=Math.max.apply(null,d.hourly.map(function(h){return h.v}));
document.getElementById('d1_hourly').innerHTML=d.hourly.map(function(h){
return'<div style="display:flex;align-items:center;gap:4px;padding:1px 0"><span style="width:32px;text-align:right;font-size:10px;color:#78909c">'+h.h+'h</span>'+bar(h.v,maxH,'b')+'</div>';
}).join('');

document.getElementById('d1_sources').innerHTML='<tr><th>数据源</th><th>类型</th><th>状态</th><th>详情</th></tr>'+d.sources.map(function(s){
var cls=s.status=='running'?'g':'r';
return'<tr><td>'+s.name+'</td><td>'+s.type+'</td><td><span class="dot '+cls+'"></span><span class="tag '+cls+'">'+s.status+'</span></td><td>'+s.detail+'</td></tr>';
}).join('');
}

// ==================== Tab 2: Stream Processing ====================
async function load2(){
var d=await fetch('/api/stream_processing').then(function(r){return r.json()});
document.getElementById('d2_jobs').innerHTML=d.jobs.length;
document.getElementById('d2_tput').innerHTML=fmt(d.jobs.reduce(function(s,j){return s+j.events_per_sec},0))+'<br><span class="u">events/s</span>';
var avgLat=Math.round(d.jobs.reduce(function(s,j){return s+j.latency_p50_ms},0)/d.jobs.length);
document.getElementById('d2_lat').innerHTML=avgLat+'<br><span class="u">ms (P50)</span>';
document.getElementById('d2_ck').innerHTML=d.checkpoint_success_rate+'<br><span class="u">%</span>';

var maxE=Math.max.apply(null,d.jobs.map(function(j){return j.events_per_sec}));
document.getElementById('d2_jobbars').innerHTML=d.jobs.map(function(j){
return'<div style="display:flex;align-items:center;gap:8px;padding:4px 0"><span style="width:180px;font-size:11px;color:#ccc">'+j.name+'</span>'+bar(j.events_per_sec,maxE,'o')+'<span style="font-size:10px;color:#78909c;margin-left:4px">'+fmt(j.events_per_sec)+' evt/s</span></div>';
}).join('');

document.getElementById('d2_cards').innerHTML=d.jobs.map(function(j){
var cls=j.status=='RUNNING'?'g':j.status=='RESTARTING'?'o':'r';
return'<div class="sc"><h3>'+j.name+' <span class="dot '+cls+'"></span><span style="font-size:11px;color:#'+(cls=='g'?'66bb6a':cls=='o'?'ffa726':'ef5350')+'">'+j.status+'</span></h3>'+
'<div class="st"><span class="lb">吞吐量</span><span class="vl">'+fmt(j.events_per_sec)+' evt/s</span></div>'+
'<div class="st"><span class="lb">P50/P99延迟</span><span class="vl">'+j.latency_p50_ms+' / '+j.latency_p99_ms+' ms</span></div>'+
'<div class="st"><span class="lb">Checkpoint</span><span class="vl">'+j.checkpoint_size_mb+'MB / '+j.checkpoint_duration_ms+'ms</span></div>'+
'<div class="st"><span class="lb">并行度/Slots</span><span class="vl">'+j.parallelism+' / '+j.slots_used+' slots</span></div>'+
'<div class="st"><span class="lb">Backlog</span><span class="vl">'+fmt(j.backlog)+'</span></div>'+
'<div class="st"><span class="lb">运行时间</span><span class="vl">'+j.uptime_h+'h</span></div>'+
'</div>';
}).join('');
}

// ==================== Tab 3: Data Warehouse ====================
async function load3(){
var d=await fetch('/api/data_warehouse').then(function(r){return r.json()});
document.getElementById('d3_tbl').innerHTML=d.total_tables;
document.getElementById('d3_etl').innerHTML=d.etl.success_rate+'<br><span class="u">%</span>';
document.getElementById('d3_vol').innerHTML=d.data_volume_gb+'<br><span class="u">GB</span>';
document.getElementById('d3_qry').innerHTML=fmt(d.query_count);

var lcolors={'ODS':'#42a5f5','DIM':'#ab47bc','DWD':'#ffa726','DWS':'#66bb6a','ADS':'#ef5350'};
var lnames={'ODS':'ODS<br>贴源层','DIM':'DIM<br>维度层','DWD':'DWD<br>明细层','DWS':'DWS<br>汇总层','ADS':'ADS<br>应用层'};
var layers=['ODS','DIM','DWD','DWS','ADS'];
var lhtml='';
layers.forEach(function(l,i){
var cnt=d.catalog[l]?d.catalog[l].length:0;
lhtml+='<div class="layer-box" style="border-color:'+lcolors[l]+'"><div class="ly" style="color:'+lcolors[l]+'">'+lnames[l]+'</div><div class="cnt">'+cnt+' 张表</div><div class="tbl">'+(d.catalog[l]||[]).slice(0,3).join(', ')+'...</div></div>';
if(i<layers.length-1) lhtml+='<div class="layer-arrow">→</div>';
});
document.getElementById('d3_layers').innerHTML=lhtml;

document.getElementById('d3_etltbl').innerHTML='<tr><th>任务</th><th>状态</th><th>耗时(s)</th><th>处理行数</th><th>开始时间</th></tr>'+d.etl.tasks.map(function(t){
var cls=t.status=='success'?'g':t.status=='running'?'o':'r';
return'<tr><td>'+t.name+'</td><td><span class="tag '+cls+'">'+t.status+'</span></td><td>'+t.duration_s+'</td><td>'+fmt(t.rows)+'</td><td>'+t.start+'</td></tr>';
}).join('');

var hdfs=d.hdfs;
var usedPct=Math.round(hdfs.used_tb/hdfs.capacity_tb*100);
document.getElementById('d3_hdfs').innerHTML=
'<div style="padding:8px 0"><span style="font-size:11px;color:#78909c">容量: '+hdfs.used_tb+'TB / '+hdfs.capacity_tb+'TB ('+usedPct+'%)</span>'+bar(hdfs.used_tb,hdfs.capacity_tb,'g')+'</div>'+
'<div class="st"><span class="lb">DataNodes</span><span class="vl">'+hdfs.datanodes_online+'/'+hdfs.data_nodes+'</span></div>'+
'<div class="st"><span class="lb">Blocks</span><span class="vl">'+fmt(hdfs.blocks_total)+'</span></div>'+
'<div class="st"><span class="lb">Under-replicated</span><span class="vl" style="color:'+(hdfs.under_replicated>50?'#ef5350':'#66bb6a')+'">'+hdfs.under_replicated+'</span></div>';

var catHtml='';
layers.forEach(function(l){
var tables=d.catalog[l]||[];
catHtml+='<div class="tree-item" onclick="var c=this.nextElementSibling;c.classList.toggle(\'open\');var e=this.querySelector(\'.expand\');e.textContent=c.classList.contains(\'open\')?\'▼\':\'▶\'"><span class="expand">▶</span> <span style="color:'+lcolors[l]+';font-weight:bold">'+l+' ('+tables.length+'表)</span></div>';
catHtml+='<div class="tree-children">'+tables.map(function(t){return'<div style="padding:2px 0;font-size:11px;color:#78909c">📄 '+t+'</div>'}).join('')+'</div>';
});
document.getElementById('d3_catalog').innerHTML=catHtml;
}

// ==================== Tab 4: Data Quality ====================
async function load4(){
var d=await fetch('/api/data_quality').then(function(r){return r.json()});
document.getElementById('d4_score').innerHTML=d.score+(d.has_data?'':'<br><span class="u" style="font-size:10px">mock</span>');
document.getElementById('d4_comp').innerHTML=d.comp_avg+'<br><span class="u">%</span>';
document.getElementById('d4_uniq').innerHTML=d.uniq_avg+'<br><span class="u">%</span>';
document.getElementById('d4_valid').innerHTML=d.valid_avg+'<br><span class="u">%</span>';

var maxS=Math.max.apply(null,d.trend.map(function(t){return t.s}));
document.getElementById('d4_trend').innerHTML=d.trend.map(function(t){
return'<div style="display:flex;align-items:center;gap:6px;padding:2px 0"><span style="width:70px;text-align:right;font-size:10px;color:#78909c">'+t.d.substring(5)+'</span>'+bar(t.s,maxS,'g')+'</div>';
}).join('');

var maxL=Math.max.apply(null,d.trend.map(function(t){return t.l}),1);
document.getElementById('d4_lag').innerHTML=d.trend.map(function(t){
return'<div style="display:flex;align-items:center;gap:6px;padding:2px 0"><span style="width:70px;text-align:right;font-size:10px;color:#78909c">'+t.d.substring(5)+'</span>'+bar(t.l,maxL,'o')+'</div>';
}).join('');

document.getElementById('d4_tbl').innerHTML='<tr><th>表名</th><th>完整率%</th><th>唯一率%</th><th>合法性%</th><th>Kafka Lag</th><th>状态</th></tr>'+d.tables.map(function(x){
return'<tr><td>'+x.n+'</td><td>'+x.c+'%</td><td>'+x.u+'%</td><td>'+x.v+'%</td><td>'+x.l+'</td><td>'+(x.st=='PASS'?'<span class="tag g">PASS</span>':'<span class="tag o">WARN</span>')+'</td></tr>';
}).join('');
}

// ==================== Tab 5: Data Lineage ====================
var lineageData=null;
async function load5(){
var d=await fetch('/api/data_lineage').then(function(r){return r.json()});
lineageData=d;
document.getElementById('d5_edges').innerHTML=d.total_edges;
document.getElementById('d5_nodes').innerHTML=d.total_nodes;
document.getElementById('d5_ods').innerHTML=d.ods_count;
document.getElementById('d5_ads').innerHTML=d.ads_count;

// Populate dropdown
var sel=document.getElementById('d5_sel');
sel.innerHTML='<option value="">-- 搜索/选择表 --</option>'+d.nodes.map(function(n){return'<option value="'+n.id+'">'+n.id+'</option>'}).join('');

renderLineage(d);
}

function renderLineage(d){
var svg=document.createElementNS('http://www.w3.org/2000/svg','svg');
var w=1100,h=Math.max(480,(Math.max(d.layers.ODS.length,d.layers.DWD.length,d.layers.ADS.length))*75+40);
svg.setAttribute('viewBox','0 0 '+w+' '+h);
svg.setAttribute('width','100%');
svg.setAttribute('height',h+'px');

var layerX={ODS:40,DIM:220,DWD:400,DWS:580,ADS:760,OTHER:940};
var layerColors={ODS:'#42a5f5',DIM:'#ab47bc',DWD:'#ffa726',DWS:'#66bb6a',ADS:'#ef5350',OTHER:'#78909c'};
var nodePos={};

// Position nodes
for(var layer in d.layers){
var nodes=d.layers[layer];
nodes.forEach(function(nid,i){
var y=20+i*70;
nodePos[nid]={x:layerX[layer],y:y,layer:layer};
});
}

// Defs
var defs=document.createElementNS('http://www.w3.org/2000/svg','defs');
defs.innerHTML='<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#2a4060"/></marker>';
svg.appendChild(defs);

// Edges
d.edges.forEach(function(e){
var s=nodePos[e.source],t=nodePos[e.target];
if(!s||!t)return;
var midX=(s.x+170+t.x)/2;
var path=document.createElementNS('http://www.w3.org/2000/svg','path');
path.setAttribute('d','M'+(s.x+160)+','+(s.y+12)+' C'+midX+','+(s.y+12)+' '+midX+','+(t.y+12)+' '+(t.x)+','+(t.y+12));
path.setAttribute('stroke','#2a4060');
path.setAttribute('stroke-width','1.2');
path.setAttribute('fill','none');
path.setAttribute('marker-end','url(#arrow)');
path.setAttribute('data-edge',e.source+'->'+e.target);
svg.appendChild(path);
});

// Nodes
var nodeMap={};
d.nodes.forEach(function(n){nodeMap[n.id]=n;});

for(var nid in nodePos){
var p=nodePos[nid];
var c=layerColors[p.layer];
var label=(nodeMap[nid]?nodeMap[nid].label:nid.replace(/_/g,' ')).substring(0,22);

var g=document.createElementNS('http://www.w3.org/2000/svg','g');
g.setAttribute('class','lnode');
g.setAttribute('data-id',nid);
g.style.cursor='pointer';

var rect=document.createElementNS('http://www.w3.org/2000/svg','rect');
rect.setAttribute('x',p.x);
rect.setAttribute('y',p.y);
rect.setAttribute('width','160');
rect.setAttribute('height','24');
rect.setAttribute('rx','4');
rect.setAttribute('fill','#1a2744');
rect.setAttribute('stroke',c);
rect.setAttribute('stroke-width','1.5');
g.appendChild(rect);

var text=document.createElementNS('http://www.w3.org/2000/svg','text');
text.setAttribute('x',p.x+6);
text.setAttribute('y',p.y+16);
text.setAttribute('fill','#ccc');
text.setAttribute('font-size','9');
text.setAttribute('font-family','Microsoft YaHei,sans-serif');
text.textContent=label;
g.appendChild(text);

g.addEventListener('click',function(){selectNode(this.getAttribute('data-id'))});
svg.appendChild(g);
}

var container=document.getElementById('d5_graph');
container.innerHTML='';
container.appendChild(svg);
}

async function selectNode(tableId){
if(!tableId){document.getElementById('d5_up').innerHTML='<span style="color:#546e7a;font-size:11px">请点击图谱中的节点</span>';document.getElementById('d5_down').innerHTML='<span style="color:#546e7a;font-size:11px">请点击图谱中的节点</span>';return}
var d=await fetch('/api/data_lineage/table/'+encodeURIComponent(tableId)).then(function(r){return r.json()});
document.getElementById('d5_up').innerHTML='<h4 style="color:#42a5f5;font-size:12px;margin-bottom:4px">'+tableId+'</h4>'+(d.upstream.length?d.upstream.map(function(u){return'<div style="font-size:11px;color:#78909c;padding:1px 0">⬆ '+u+'</div>'}).join(''):'<div style="font-size:11px;color:#546e7a">无上游 (源表)</div>');
document.getElementById('d5_down').innerHTML='<h4 style="color:#ffa726;font-size:12px;margin-bottom:4px">'+tableId+'</h4>'+(d.downstream.length?d.downstream.map(function(dw){return'<div style="font-size:11px;color:#78909c;padding:1px 0">⬇ '+dw+'</div>'}).join(''):'<div style="font-size:11px;color:#546e7a">无下游 (终端表)</div>');

// Highlight in graph
document.querySelectorAll('#d5_graph .lnode rect').forEach(function(r){r.setAttribute('stroke-width','1.5');r.setAttribute('stroke',function(){var g=r.parentElement;var nid=g.getAttribute('data-id');if(!lineageData)return'#1e3a5f';var layer='ODS';for(var l in lineageData.layers){if(lineageData.layers[l].indexOf(nid)>=0){layer=l;break}}return{'ODS':'#42a5f5','DIM':'#ab47bc','DWD':'#ffa726','DWS':'#66bb6a','ADS':'#ef5350','OTHER':'#78909c'}[layer]}());});
d.upstream.forEach(function(id){var g=document.querySelector('#d5_graph .lnode[data-id="'+id+'"]');if(g){var r=g.querySelector('rect');r.setAttribute('stroke','#42a5f5');r.setAttribute('stroke-width','3')}});
d.downstream.forEach(function(id){var g=document.querySelector('#d5_graph .lnode[data-id="'+id+'"]');if(g){var r=g.querySelector('rect');r.setAttribute('stroke','#ffa726');r.setAttribute('stroke-width','3')}});
var selfG=document.querySelector('#d5_graph .lnode[data-id="'+tableId+'"]');
if(selfG){var selfR=selfG.querySelector('rect');selfR.setAttribute('stroke','#4fc3f7');selfR.setAttribute('stroke-width','3.5')}

document.getElementById('d5_sel').value=tableId;
}

// ==================== Tab 6: AI Assistant ====================
async function load6(){
var d=await fetch('/api/ai_assistant').then(function(r){return r.json()});
document.getElementById('d6_nlq').innerHTML=fmt(d.nl2sql_queries_today)+'<br><span class="u">今日</span>';
document.getElementById('d6_ano').innerHTML=d.anomalies_detected+'<br><span class="u">个异常</span>';
document.getElementById('d6_oll').innerHTML=(d.ollama_online?'在线':'离线')+'<br><span class="u">'+(d.ollama_online?'qwen3:8b':'--')+'</span>';
document.getElementById('d6_fb').innerHTML='在线<br><span class="u">规则引擎</span>';

document.getElementById('d6_types').innerHTML='<tr><th>意图</th><th>示例查询</th></tr>'+d.query_types.map(function(q){
return'<tr><td style="color:#4fc3f7">'+q.intent+'</td><td style="font-size:11px">'+q.example+'</td></tr>';
}).join('');

document.getElementById('d6_alist').innerHTML=d.anomaly_types.map(function(t){
return'<div style="padding:4px 0;font-size:12px;border-bottom:1px solid #1a2744"><span class="dot o"></span>'+t+'</div>';
}).join('');
}

function setQuery(q){document.getElementById('d6_qin').value=q}
async function doQuery(){
var q=document.getElementById('d6_qin').value.trim();
if(!q)return;
var res=document.getElementById('d6_qres');
res.style.display='block';
res.innerHTML='<span style="color:#78909c;font-size:11px">⏳ 查询中...</span>';
try{
var d=await fetch('/api/ai_assistant/query',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({question:q})}).then(function(r){return r.json()});
var rowsHtml=d.result.rows.map(function(r){return'<tr>'+r.map(function(c){return'<td>'+c+'</td>'}).join('')+'</tr>'}).join('');
res.innerHTML=
'<div class="sql-label">📋 意图: <span style="color:#4fc3f7">'+d.intent+'</span></div>'+
'<div class="sql-label">📝 生成SQL:</div><pre>'+d.sql+'</pre>'+
'<div class="sql-label" style="margin-top:8px">📊 查询结果 ('+d.result.row_count+'行, '+d.result.execution_time_ms+'ms):</div>'+
'<table style="margin-top:4px"><tr>'+d.result.columns.map(function(c){return'<th>'+c+'</th>'}).join('')+'</tr>'+rowsHtml+'</table>';
}catch(e){
res.innerHTML='<span style="color:#ef5350">查询失败: '+e.message+'</span>';
}
}

// ==================== Tab 7: Full Chain ====================
async function load7(){
var d=await fetch('/api/full_chain').then(function(r){return r.json()});
document.getElementById('d7_health').innerHTML=d.health;
document.getElementById('d7_alerts').innerHTML=d.alerts.length;
document.getElementById('d7_run').innerHTML=d.running+'<br><span class="u">/ '+d.total+'</span>';
document.getElementById('d7_fresh').innerHTML=d.data_freshness_s+'<br><span class="u">秒前</span>';

document.getElementById('d7_chain').innerHTML=d.pipeline.map(function(p,i){
var cls=p.status=='running'?'g':'r';
var arrow=i<d.pipeline.length-1?'<div class="arrow">→</div>':'';
return'<div class="node"><div class="icon">'+p.icon+'</div><div class="lbl">'+p.stage+'</div><div class="st" style="color:#'+(cls=='g'?'66bb6a':'ef5350')+'">'+p.desc+'</div></div>'+arrow;
}).join('');

document.getElementById('d7_comps').innerHTML='<tr><th>组件</th><th>状态</th><th>详情</th><th>运行时间</th></tr>'+d.components.map(function(c){
var cls=c.status=='running'?'g':'r';
return'<tr><td>'+c.icon+' '+c.name+'</td><td><span class="dot '+cls+'"></span><span class="tag '+cls+'">'+c.status+'</span></td><td>'+c.detail+'</td><td>'+c.uptime+'</td></tr>';
}).join('');

document.getElementById('d7_altbl').innerHTML='<tr><th>级别</th><th>时间</th><th>消息</th></tr>'+d.alerts.map(function(a){
var cls=a.severity=='CRITICAL'?'r':'o';
return'<tr><td><span class="tag '+cls+'">'+a.level+'</span></td><td>'+a.time+'</td><td style="font-size:11px">'+a.message+'</td></tr>';
}).join('');
}

// ==================== Auto-refresh ====================
load1();
setInterval(function(){
var a=document.querySelector('.tab.active');
if(a){var fn=window['load'+a.id.substring(2)];if(fn)fn()}
},15000);
</script></body></html>'''

# ============================================================
# App Entry
# ============================================================
@app.route("/")
def index():
    return render_template_string(HTML)

if __name__ == "__main__":
    from waitress import serve
    print("=" * 60)
    print("  数据服务可视化监控平台")
    print("  7大服务面板 | 端口 8089")
    print("  http://127.0.0.1:8089")
    print("=" * 60)
    serve(app, host="0.0.0.0", port=8089, threads=8)
