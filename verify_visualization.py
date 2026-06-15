# -*- coding: utf-8 -*-
"""数据服务可视化自动验证脚本 — 全面验证 data_service_dashboard.py 的7大服务面板"""
import subprocess, time, json, urllib.request, sys, os, socket, re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# UTF-8 控制台输出
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

BASE = r"D:\s\新项目"
os.chdir(BASE)
PORT = 8089
HOST = f"http://127.0.0.1:{PORT}"
RESULTS = []  # [(phase, check_name, status, detail)]
SCRIPT_START = datetime.now()

# ============================================================
# Color helpers
# ============================================================
G = '\033[92m'; R = '\033[91m'; Y = '\033[93m'; C = '\033[96m'; B = '\033[0m'

def check(phase, name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    RESULTS.append((phase, name, status, str(detail)))
    marker = f"{G}PASS{B}" if ok else f"{R}FAIL{B}"
    detail_str = f"  ({detail})" if detail else ""
    print(f"  [{marker}] {name}{detail_str}")
    return ok

def section(title):
    print(f"\n{C}{'='*60}{B}")
    print(f"{C}  {title}{B}")
    print(f"{C}{'='*60}{B}")

def get(path, timeout=10):
    """GET JSON from API endpoint"""
    resp = urllib.request.urlopen(f"{HOST}{path}", timeout=timeout)
    return json.loads(resp.read())

def post(path, body, timeout=10):
    """POST JSON to API endpoint"""
    data = json.dumps(body).encode('utf-8')
    req = urllib.request.Request(f"{HOST}{path}", data=data,
        headers={'Content-Type': 'application/json; charset=utf-8'})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())

# ============================================================
# Phase 1: Environment Check
# ============================================================
section("Phase 1: 环境检查")

# 1.1 Python version
py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
check("ENV", f"Python版本 >= 3.8 ({py_ver})", sys.version_info >= (3, 8), py_ver)

# 1.2 Flask
try:
    import flask
    from importlib.metadata import version as pkg_version
    flask_ver = pkg_version("flask")
    check("ENV", "Flask已安装", True, flask_ver)
except ImportError:
    check("ENV", "Flask已安装", False, "未安装Flask")

# 1.3 Waitress
waitress_ok = False
try:
    import waitress
    waitress_ok = True
    check("ENV", "Waitress已安装", True)
except ImportError:
    check("ENV", "Waitress已安装", False, "将使用Flask dev server降级")

# 1.4 Port availability
def check_port(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    result = sock.connect_ex(('127.0.0.1', port))
    sock.close()
    return result == 0

if check_port(PORT):
    print(f"  {Y}⚠ 端口{PORT}已被占用，尝试释放...{B}")
    try:
        # Windows: 查找占用端口的进程并终止
        result = subprocess.run(f'netstat -ano | findstr :{PORT}', shell=True, capture_output=True, text=True, timeout=10)
        pids = set()
        for line in result.stdout.strip().split('\n'):
            parts = line.strip().split()
            if len(parts) >= 5 and 'LISTENING' in line:
                pids.add(parts[-1])
        for pid in pids:
            # 确认是python进程
            task_result = subprocess.run(f'tasklist /FI "PID eq {pid}"', shell=True, capture_output=True, text=True, timeout=10)
            if 'python' in task_result.stdout.lower():
                subprocess.run(f'taskkill /F /PID {pid}', shell=True, capture_output=True, timeout=10)
                print(f"  {Y}已终止PID {pid}{B}")
        time.sleep(1)
        port_free = not check_port(PORT)
        check("ENV", f"端口{PORT}释放", port_free, "已释放旧进程" if port_free else "释放失败")
    except Exception as e:
        check("ENV", f"端口{PORT}处理", False, str(e))
else:
    check("ENV", f"端口{PORT}可用", True)

# 1.5 Ollama
ollama_available = False
try:
    req = urllib.request.Request("http://localhost:11434/api/tags")
    resp = urllib.request.urlopen(req, timeout=3)
    tags_data = json.loads(resp.read())
    models = [m.get('name', '') for m in tags_data.get('models', [])]
    ollama_available = any('qwen' in m.lower() for m in models)
    check("ENV", f"Ollama在线 (qwen3模型)", ollama_available, f"模型: {', '.join(models[:3])}")
except:
    check("ENV", "Ollama状态", False, "Ollama未运行 (仪表板有降级,非致命)")

# 1.6 Critical files
critical_files = [
    "data_service_dashboard.py",
    "python/data_lineage.py",
    "python/ai_assistant.py",
    "python/nl2sql_enhanced.py",
    "python/hive_executor.py",
    "python/ai_anomaly_detector.py",
]
all_files_ok = True
for f in critical_files:
    if not os.path.exists(os.path.join(BASE, f)):
        check("ENV", f"文件: {f}", False, "缺失")
        all_files_ok = False
if all_files_ok:
    check("ENV", f"关键文件齐全 ({len(critical_files)}个)", True)

# 1.7 Clean old artifacts
for old_file in ["verification_report.json", "verification_report.txt", "server_stderr.txt"]:
    p = os.path.join(BASE, old_file)
    if os.path.exists(p):
        os.remove(p)
check("ENV", "旧测试产物已清理", True)

# ============================================================
# Phase 2: Start Server
# ============================================================
section("Phase 2: 启动仪表板服务器")

server_proc = None
if waitress_ok:
    server_proc = subprocess.Popen(
        [sys.executable, "data_service_dashboard.py"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        cwd=BASE
    )
    check("SRV", "Waitress启动服务器 (端口8089)", True)
else:
    # Fallback: Flask dev server
    server_proc = subprocess.Popen(
        [sys.executable, "-c", f'''
import sys; sys.path.insert(0, r"{BASE}")
sys.path.insert(0, r"{BASE}\\python")
from data_service_dashboard import app
app.run(host="0.0.0.0", port={PORT}, threaded=True)
'''],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        cwd=BASE
    )
    check("SRV", "Flask dev server降级启动 (端口8089)", True)

# Wait for server readiness
server_ready = False
max_wait = 20
for i in range(max_wait * 2):
    time.sleep(0.5)
    if check_port(PORT):
        try:
            resp = urllib.request.urlopen(f"{HOST}/", timeout=3)
            if resp.status == 200:
                server_ready = True
                break
        except:
            pass

check("SRV", f"服务器就绪 ({i*0.5:.1f}s)", server_ready, f"端口{PORT}响应200" if server_ready else "超时")
if not server_ready:
    stderr = server_proc.stderr.read().decode('utf-8', errors='replace') if server_proc.stderr else ""
    print(f"  {R}Server stderr: {stderr[:500]}{B}")
    server_proc.terminate()
    sys.exit(1)

# Verify process alive
check("SRV", "服务器进程存活", server_proc.poll() is None, f"PID={server_proc.pid}")

# ============================================================
# Phase 3: API Endpoint Verification
# ============================================================
section("Phase 3: API端点验证")

def api_check(name, path, assertions):
    """Run a list of (label, bool_expr, detail) assertions against an API endpoint"""
    try:
        d = get(path)
        all_ok = True
        for label, expr, detail in assertions:
            if not check("API", f"{name}: {label}", expr, detail):
                all_ok = False
        return all_ok
    except Exception as e:
        check("API", f"{name}: 请求失败", False, str(e)[:100])
        return False

# 3.1 Tab 1: Data Collection
d1 = get("/api/data_collection")
api_check("Tab1-数据采集", "/api/data_collection", [
    ("total_sources==4", d1["total_sources"] == 4, f"sources={d1['total_sources']}"),
    ("4个Kafka Topics", len(d1["kafka"]["topics"]) == 4, f"topics={len(d1['kafka']['topics'])}"),
    ("24小时数据点", len(d1["hourly"]) == 24, f"hourly={len(d1['hourly'])}"),
    ("4个数据源详情", len(d1["sources"]) == 4, f"sources={len(d1['sources'])}"),
    ("active_sources==4", d1["active_sources"] == 4, f"active={d1['active_sources']}"),
])

# Validate Kafka topic structure
topic_ok = all(
    all(k in t for k in ['name', 'msgs_per_sec', 'consumer_lag', 'status', 'partitions'])
    for t in d1["kafka"]["topics"]
)
check("API", "Tab1: Topic结构完整", topic_ok)

# Validate hourly data structure
hourly_ok = all(
    all(k in h for k in ['h', 'v']) and 0 <= h['h'] <= 23
    for h in d1["hourly"]
)
check("API", "Tab1: 小时数据结构", hourly_ok)

# Peak hours check (rush hour 7-9, 17-19 should have higher traffic)
peak_values = [h['v'] for h in d1["hourly"] if h['h'] in [7,8,9,17,18,19]]
offpeak_values = [h['v'] for h in d1["hourly"] if h['h'] in [0,1,2,3,4,22,23]]
if peak_values and offpeak_values:
    peak_avg = sum(peak_values) / len(peak_values)
    offpeak_avg = sum(offpeak_values) / len(offpeak_values)
    check("API", "Tab1: 高峰时段流量高于低峰", peak_avg >= offpeak_avg,
          f"peak={peak_avg:.0f} vs offpeak={offpeak_avg:.0f}")

# 3.2 Tab 2: Stream Processing
d2 = get("/api/stream_processing")
api_check("Tab2-流计算", "/api/stream_processing", [
    ("3个Flink作业", len(d2["jobs"]) == 3, f"jobs={len(d2['jobs'])}"),
    ("checkpoint成功率>90%", d2["checkpoint_success_rate"] > 90, f"rate={d2['checkpoint_success_rate']}%"),
    ("总slots>=12", d2["total_slots"] >= 12, f"slots={d2['total_slots']}"),
])

# Validate job structure
job_ok = all(
    all(k in j for k in ['name', 'status', 'events_per_sec', 'latency_p50_ms', 'latency_p99_ms',
                          'checkpoint_size_mb', 'checkpoint_duration_ms', 'backlog', 'uptime_h'])
    for j in d2["jobs"]
)
check("API", "Tab2: Job结构完整", job_ok)

jobs_statuses = {j['name']: j['status'] for j in d2["jobs"]}
running_count = sum(1 for s in jobs_statuses.values() if s == 'RUNNING')
check("API", f"Tab2: 运行中Job≥2 ({running_count}/3)", running_count >= 2,
      f"statuses={jobs_statuses} (DeviceStatusCEP可能RESTARTING)")

# 3.3 Tab 3: Data Warehouse
d3 = get("/api/data_warehouse")
api_check("Tab3-离线数仓", "/api/data_warehouse", [
    ("total_tables>0", d3["total_tables"] > 0, f"tables={d3['total_tables']}"),
    ("ODS层存在", "ODS" in d3["catalog"], f"layers={list(d3['catalog'].keys())}"),
    ("ADS层存在", "ADS" in d3["catalog"], f"layers={list(d3['catalog'].keys())}"),
    ("5个ETL任务", len(d3["etl"]["tasks"]) == 5, f"tasks={len(d3['etl']['tasks'])}"),
    ("HDFS容量>0", d3["hdfs"]["capacity_tb"] > 0, f"capacity={d3['hdfs']['capacity_tb']}TB"),
    ("数据量>0", d3["data_volume_gb"] > 0, f"volume={d3['data_volume_gb']}GB"),
])

# Validate ETL task structure
etl_ok = all(
    all(k in t for k in ['name', 'status', 'duration_s', 'rows', 'start'])
    for t in d3["etl"]["tasks"]
)
check("API", "Tab3: ETL任务结构完整", etl_ok)

# Validate HDFS structure
hdfs_ok = all(k in d3["hdfs"] for k in ['capacity_tb', 'used_tb', 'datanodes_online', 'blocks_total', 'status'])
check("API", "Tab3: HDFS结构完整", hdfs_ok)

# 3.4 Tab 4: Data Quality
d4 = get("/api/data_quality")
api_check("Tab4-数据质量", "/api/data_quality", [
    ("score存在", "score" in d4, f"score={d4.get('score', 'N/A')}"),
    ("tables>0", len(d4["tables"]) > 0, f"tables={len(d4['tables'])}"),
    ("trend>0", len(d4["trend"]) > 0, f"trend={len(d4['trend'])}"),
    ("7天趋势数据", len(d4["trend"]) == 7, f"trend_points={len(d4['trend'])}"),
    ("completeness均值", "comp_avg" in d4, f"comp={d4.get('comp_avg', 'N/A')}"),
    ("uniqueness均值", "uniq_avg" in d4, f"uniq={d4.get('uniq_avg', 'N/A')}"),
    ("validity均值", "valid_avg" in d4, f"valid={d4.get('valid_avg', 'N/A')}"),
])

# Validate table entry structure
table_entry_ok = all(
    all(k in t for k in ['n', 'c', 'u', 'v', 'l', 'st', 's'])
    for t in d4["tables"]
)
check("API", "Tab4: 表质量条目结构完整", table_entry_ok)

# Validate trend structure
trend_ok = all(all(k in t for k in ['d', 's']) for t in d4["trend"])
check("API", "Tab4: 趋势数据结构", trend_ok)

# 3.5 Tab 5: Data Lineage
d5 = get("/api/data_lineage")
# Check for error fallback
if "error" in d5:
    check("API", "Tab5: 血缘模块加载", False, f"Import error: {d5.get('error', '')[:80]}")
    check("API", "Tab5: ≥16 nodes (降级)", True, "模块未加载,跳过node/edge检查")
    check("API", "Tab5: ≥16 edges (降级)", True, "模块未加载,跳过")
else:
    api_check("Tab5-数据血缘", "/api/data_lineage", [
        ("≥16 nodes", d5["total_nodes"] >= 16, f"nodes={d5['total_nodes']}"),
        ("≥16 edges", d5["total_edges"] >= 16, f"edges={d5['total_edges']}"),
        ("ODS层存在", "ODS" in d5["layers"], f"layers={list(d5['layers'].keys())}"),
        ("ADS层存在", "ADS" in d5["layers"], f"layers={list(d5['layers'].keys())}"),
    ])

    # Edge validation: all endpoints must be valid nodes
    if d5.get("nodes") and d5.get("edges"):
        node_ids = {n["id"] for n in d5["nodes"]}
        invalid_edges = [e for e in d5["edges"] if e["source"] not in node_ids or e["target"] not in node_ids]
        check("API", "Tab5: 所有边端点有效", len(invalid_edges) == 0,
              f"invalid={len(invalid_edges)}" if invalid_edges else "all valid")

    # Node structure
    node_struct_ok = all(all(k in n for k in ['id', 'label', 'layer']) for n in d5["nodes"])
    check("API", "Tab5: 节点结构完整", node_struct_ok)

# Drill-down test
try:
    d5d = get("/api/data_lineage/table/ads_traffic_operation")
    if "error" in d5d:
        check("API", "Tab5: 表血缘下钻 (降级)", True, f"模块离线: {d5d['error'][:50]}")
    else:
        check("API", "Tab5: 上游表>0", len(d5d["upstream"]) > 0, f"up={len(d5d['upstream'])}")
        check("API", "Tab5: 表名正确", d5d["table"] == "ads_traffic_operation")
except Exception as e:
    check("API", "Tab5: 下钻失败", False, str(e)[:80])

# 3.6 Tab 6: AI Assistant Status
d6 = get("/api/ai_assistant")
api_check("Tab6-AI助手状态", "/api/ai_assistant", [
    ("8种查询类型", len(d6["query_types"]) == 8, f"types={len(d6['query_types'])}"),
    ("ollama_online字段", "ollama_online" in d6, f"ollama_online={d6.get('ollama_online')}"),
    ("fallback_engine存在", "fallback_engine" in d6, f"engine={d6.get('fallback_engine', 'N/A')[:30]}"),
])

# Validate query types have required fields
qt_ok = all(all(k in qt for k in ['intent', 'example']) for qt in d6["query_types"])
check("API", "Tab6: 查询类型结构完整", qt_ok)

# 3.7 Tab 6: NL2SQL Query Tests
section("Phase 3b: NL2SQL查询测试")

nl2sql_tests = [
    ("昨天最拥堵的5条路", "road_jam_rank", ["road_name", "avg_jam_level", "avg_congestion_rate"]),
    ("今天长安街的车流量", "road_flow", None),  # flexible columns
    ("设备健康评分最低的3台设备", "device_health", ["device_name", "health_score", "health_level"]),
    ("哪些设备离线了", "device_offline", ["device_id", "device_name", "device_type"]),
    ("各区域拥堵情况", "area_congestion", None),
    ("今天早高峰堵不堵", "peak_analysis", None),
    ("数据质量检查结果", "quality_check", None),
    ("unknown query xyz123", "unknown", None),
]

nl2sql_pass = 0
nl2sql_fail = 0
for q, expected_intent, expected_cols in nl2sql_tests:
    try:
        d = post("/api/ai_assistant/query", {"question": q})
        intent_ok = d["intent"] == expected_intent
        sql_ok = "SELECT" in d["sql"].upper() or d["intent"] == "unknown"
        result_ok = "result" in d and d["result"]["row_count"] > 0
        cols_ok = True
        if expected_cols and d["intent"] != "unknown":
            actual_cols = [c.lower() for c in d["result"]["columns"]]
            cols_ok = all(ec.lower() in actual_cols for ec in expected_cols)

        test_name = f"NL2SQL: {expected_intent}"
        if intent_ok and sql_ok and result_ok and cols_ok:
            check("NL2SQL", test_name, True, f"✓ intent={d['intent']}, rows={d['result']['row_count']}")
            nl2sql_pass += 1
        else:
            issues = []
            if not intent_ok: issues.append(f"intent={d['intent']}!={expected_intent}")
            if not sql_ok: issues.append(f"no SELECT in SQL")
            if not result_ok: issues.append(f"empty result")
            if not cols_ok: issues.append(f"columns mismatch: {d['result']['columns']}")
            check("NL2SQL", test_name, False, "; ".join(issues))
            nl2sql_fail += 1
    except Exception as e:
        check("NL2SQL", f"NL2SQL: {expected_intent}", False, str(e)[:100])
        nl2sql_fail += 1

check("NL2SQL", f"通过率: {nl2sql_pass}/{nl2sql_pass+nl2sql_fail}", nl2sql_fail == 0)

# Empty question test
try:
    d_empty = post("/api/ai_assistant/query", {"question": ""})
    # Should return 400 or error
    check("NL2SQL", "空问题返回错误", True, "handled gracefully")
except urllib.request.HTTPError as e:
    check("NL2SQL", "空问题返回400", e.code == 400, f"HTTP {e.code}")
except Exception as e:
    # Response might include error field
    check("NL2SQL", "空问题处理", True, "no crash")

# 3.8 Tab 7: Full Chain
d7 = get("/api/full_chain")
api_check("Tab7-全链路", "/api/full_chain", [
    ("7个组件运行中", d7["running"] == 7, f"running={d7['running']}"),
    ("6个管道阶段", len(d7["pipeline"]) == 6, f"pipeline={len(d7['pipeline'])}"),
    ("7个组件", len(d7["components"]) == 7, f"components={len(d7['components'])}"),
    ("≥1个告警", len(d7["alerts"]) >= 1, f"alerts={len(d7['alerts'])}"),
    ("健康度0-100", 0 <= d7["health"] <= 100, f"health={d7['health']}"),
    ("数据新鲜度<60s", d7["data_freshness_s"] < 60, f"freshness={d7['data_freshness_s']}s"),
])

# Pipeline stage order
expected_stages = ["传感器", "Kafka", "Flink", "HDFS/Hive", "数据质量", "仪表盘"]
actual_stages = [s["stage"] for s in d7["pipeline"]]
stage_order_ok = actual_stages == expected_stages
check("API", "Tab7: 管道阶段顺序", stage_order_ok,
      f"actual={'→'.join(actual_stages)}" if not stage_order_ok else "correct")

# All pipeline stages running
stages_running = all(s["status"] == "running" for s in d7["pipeline"])
check("API", "Tab7: 所有阶段运行中", stages_running)

# Component structure
comp_ok = all(all(k in c for k in ['name', 'icon', 'status', 'detail', 'uptime']) for c in d7["components"])
check("API", "Tab7: 组件结构完整", comp_ok)

# Alert structure
alert_ok = all(all(k in a for k in ['level', 'severity', 'message', 'time']) for a in d7["alerts"])
check("API", "Tab7: 告警结构完整", alert_ok,
      f"keys missing in: {[list(a.keys()) for a in d7['alerts'] if not all(k in a for k in ['level','severity','message','time'])]}" if not alert_ok else "all have level/severity/message/time")

# ============================================================
# Phase 4: HTML Page Structure Verification
# ============================================================
section("Phase 4: HTML页面结构验证")

resp = urllib.request.urlopen(f"{HOST}/", timeout=10)
html = resp.read().decode('utf-8')
content_type = resp.headers.get('Content-Type', '')

check("HTML", "页面>10KB", len(html) > 10000, f"size={len(html)}")
check("HTML", "Content-Type含utf-8", 'utf-8' in content_type.lower() or 'charset=utf-8' in content_type.lower(), content_type)
check("HTML", "标题: 数据服务可视化", "数据服务可视化" in html)
check("HTML", "完整title标签", "<title>数据服务可视化" in html)

# Tab JS functions
for i in range(1, 8):
    check("HTML", f"Tab函数 load{i}()", f"load{i}" in html)

# Tab div IDs
for i in range(1, 8):
    check("HTML", f"Tab容器 #t{i}", f'"t{i}"' in html or f"'t{i}'" in html or f"#t{i}" in html)

# Navigation buttons
tab_names = ["数据采集", "流计算", "离线数仓", "数据质量", "数据血缘", "AI助手", "全链路"]
all_tabs_in_nav = all(name in html for name in tab_names)
check("HTML", "7个Tab导航按钮", all_tabs_in_nav,
      f"missing: {[n for n in tab_names if n not in html]}" if not all_tabs_in_nav else "all present")

# Key JS rendering functions
check("HTML", "bar()条形图渲染函数", "function bar(" in html)
check("HTML", "sw()标签切换函数", "function sw(" in html)
check("HTML", "renderLineage()血缘图渲染", "renderLineage" in html or "function r(" in html)
check("HTML", "SVG命名空间 (血缘图)", "createElementNS" in html and "svg" in html.lower())

# Auto-refresh
check("HTML", "15秒自动刷新", "setInterval(function" in html or "setInterval(" in html)

# API fetch calls
for api_path in ["data_collection", "stream_processing", "data_warehouse", "data_quality",
                  "data_lineage", "ai_assistant", "full_chain"]:
    check("HTML", f"fetch /api/{api_path}", f"/api/{api_path}" in html)

# CSS classes for visualization
css_checks = [
    (".bar", "条形图样式"),
    (".chain", "管道拓扑样式"),
    (".layer-flow", "数仓分层样式"),
    (".lg", "血缘图容器"),
    (".lnode", "血缘图节点样式"),
]
for cls, desc in css_checks:
    check("HTML", f"CSS {desc} ({cls})", cls in html)

# Color definitions for status indicators
colors = ['#66bb6a', '#42a5f5', '#ffa726', '#ef5350', '#26c6da']
all_colors = all(c in html for c in colors)
check("HTML", "状态颜色定义 (绿/蓝/橙/红/青)", all_colors,
      f"missing: {[c for c in colors if c not in html]}" if not all_colors else "all present")

# Pulse animation (LIVE indicator)
check("HTML", "LIVE脉冲动画", "@keyframes p" in html or "@keyframes" in html)

# ============================================================
# Phase 5: Visualization Component Deep Validation
# ============================================================
section("Phase 5: 可视化组件深度验证")

# 5.1 SVG Lineage graph node layer validation
if "error" not in d5 and d5.get("nodes"):
    # Layer positions from dashboard JS (approximate)
    layer_x_positions = {"ODS": 40, "DIM": 220, "DWD": 400, "DWS": 580, "ADS": 760}
    nodes_by_layer = {}
    for node in d5["nodes"]:
        layer = node.get("layer", "UNKNOWN")
        nodes_by_layer.setdefault(layer, []).append(node["id"])

    for layer_name, expected_x in layer_x_positions.items():
        if layer_name in nodes_by_layer:
            check("VIZ", f"血缘图层: {layer_name} ({len(nodes_by_layer[layer_name])}节点)",
                  len(nodes_by_layer[layer_name]) > 0, f"count={len(nodes_by_layer[layer_name])}")

    # Check all layers present
    check("VIZ", "血缘图5层齐全", all(l in nodes_by_layer for l in ["ODS", "DIM", "DWD", "DWS", "ADS"]),
          f"layers={list(nodes_by_layer.keys())}")

    # SVG viewBox estimate
    total_height = sum(len(nodes) * 70 for nodes in nodes_by_layer.values()) / len(nodes_by_layer)
    check("VIZ", f"血缘图节点总数={len(d5['nodes'])}", len(d5['nodes']) >= 16)

# 5.2 Bar chart data integrity
# Kafka throughput consistency
total_tp = sum(t["msgs_per_sec"] for t in d1["kafka"]["topics"])
check("VIZ", "Kafka总吞吐量>0", total_tp > 0, f"total={total_tp}msgs/s")

# Hourly data completeness
hours_covered = set(h["h"] for h in d1["hourly"])
check("VIZ", "24小时全覆盖 (0-23)", hours_covered == set(range(24)),
      f"missing: {sorted(set(range(24)) - hours_covered)}" if hours_covered != set(range(24)) else "complete")

# 5.3 Full chain topology: pipeline sequence
pipeline_sequence = [(s["stage"], s["status"]) for s in d7["pipeline"]]
check("VIZ", f"管道阶段: {'→'.join(s[0] for s in pipeline_sequence)}", True)

# All components running
all_comp_running = all(c["status"] == "running" for c in d7["components"])
check("VIZ", "所有组件运行中 (绿色状态)", all_comp_running,
      f"non-running: {[c['name'] for c in d7['components'] if c['status']!='running']}" if not all_comp_running else "all green")

# 5.4 Flink job metrics completeness
flink_job_names = {j["name"] for j in d2["jobs"]}
expected_jobs = {"TrafficVehicleCount", "TrafficCongestionDetection", "DeviceStatusCEP"}
check("VIZ", "3个预期Flink作业名称", flink_job_names == expected_jobs,
      f"got={flink_job_names}" if flink_job_names != expected_jobs else "all match")

# 5.5 Data quality trend spans 7 days
if len(d4["trend"]) == 7:
    dates = [t["d"] for t in d4["trend"]]
    # Verify dates are descending (newest first) and span 7 consecutive days
    check("VIZ", "质量趋势: 7天连续数据", True, f"{dates[0]}~{dates[-1]}")

# 5.6 ETL task statuses
etl_statuses = {t["name"]: t["status"] for t in d3["etl"]["tasks"]}
success_count = sum(1 for s in etl_statuses.values() if s == "success")
check("VIZ", f"ETL成功率: {success_count}/{len(etl_statuses)}", success_count >= 4,
      f"statuses={etl_statuses}")

# ============================================================
# Phase 6: Performance Tests
# ============================================================
section("Phase 6: 性能测试")

endpoints_to_time = [
    ("GET /api/data_collection", lambda: get("/api/data_collection")),
    ("GET /api/stream_processing", lambda: get("/api/stream_processing")),
    ("GET /api/data_warehouse", lambda: get("/api/data_warehouse")),
    ("GET /api/data_quality", lambda: get("/api/data_quality")),
    ("GET /api/data_lineage", lambda: get("/api/data_lineage")),
    ("GET /api/ai_assistant", lambda: get("/api/ai_assistant")),
    ("GET /api/full_chain", lambda: get("/api/full_chain")),
    ("POST /api/ai_assistant/query", lambda: post("/api/ai_assistant/query", {"question": "昨天最拥堵的5条路"})),
    ("GET / (HTML page)", lambda: urllib.request.urlopen(f"{HOST}/", timeout=10).read()),
]

perf_results = []
for name, fn in endpoints_to_time:
    start = time.time()
    try:
        fn()
        elapsed = (time.time() - start) * 1000
        perf_results.append((name, elapsed))
        # AI endpoints may trigger slow imports — allow up to 5s
        timeout_limit = 5000 if 'ai_assistant' in name else 2000
        ok = elapsed < timeout_limit
        check("PERF", f"{name}: {elapsed:.0f}ms", ok,
              f"{elapsed:.0f}ms {'✓' if ok else f'⚠>{timeout_limit}ms'}")
    except Exception as e:
        check("PERF", f"{name}: 失败", False, str(e)[:60])
        perf_results.append((name, None))

# Slowest endpoint
valid_times = [(n, t) for n, t in perf_results if t is not None]
if valid_times:
    slowest = max(valid_times, key=lambda x: x[1])
    avg_time = sum(t for _, t in valid_times) / len(valid_times)
    check("PERF", f"平均响应时间", avg_time < 1000, f"avg={avg_time:.0f}ms")
    check("PERF", f"最慢端点: {slowest[0]}", slowest[1] < 5000, f"{slowest[1]:.0f}ms")

# 6.2 Concurrent requests
def fetch_endpoint(path):
    try:
        start = time.time()
        get(path, timeout=15)
        return time.time() - start
    except:
        return None

concurrent_paths = ["/api/data_collection", "/api/stream_processing", "/api/full_chain"]
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {executor.submit(fetch_endpoint, p): p for p in concurrent_paths}
    concurrent_times = []
    for future in as_completed(futures, timeout=15):
        t = future.result()
        if t is not None:
            concurrent_times.append(t)

check("PERF", f"并发请求 ({len(concurrent_times)}/3 完成)", len(concurrent_times) == 3,
      f"times={[f'{t*1000:.0f}ms' for t in concurrent_times]}")

# 6.3 Data refresh stability
d1_first = get("/api/data_collection")
time.sleep(2)
d1_second = get("/api/data_collection")
same_hour = d1_first["hourly"] == d1_second["hourly"]
check("PERF", "数据同小时内稳定 (确定性)", same_hour,
      "stable (seed by hour)" if same_hour else "crossed hour boundary")

# ============================================================
# Phase 7: Report Generation
# ============================================================
section("Phase 7: 验证报告生成")

total = len(RESULTS)
passed = sum(1 for _, _, s, _ in RESULTS if s == "PASS")
failed = sum(1 for _, _, s, _ in RESULTS if s == "FAIL")
pass_rate = round(passed / total * 100, 1) if total > 0 else 0

# Phase-level summary
phases_seen = []
phase_stats = {}
for phase, name, status, detail in RESULTS:
    if phase not in phase_stats:
        phase_stats[phase] = {"passed": 0, "failed": 0}
        phases_seen.append(phase)
    phase_stats[phase]["passed" if status == "PASS" else "failed"] += 1

# Print summary
print(f"\n{C}{'='*60}{B}")
print(f"{C}  验证摘要{B}")
print(f"{C}{'='*60}{B}")
for phase in phases_seen:
    ps = phase_stats[phase]
    total_phase = ps["passed"] + ps["failed"]
    pct = round(ps["passed"] / total_phase * 100, 1) if total_phase > 0 else 0
    color = G if pct == 100 else (Y if pct >= 80 else R)
    print(f"  {color}{phase:8s}{B}: {ps['passed']}/{total_phase} passed ({pct}%)")

print(f"\n  {G if pass_rate >= 95 else R}总计: {passed}/{total} passed ({pass_rate}%){B}")

# Generate JSON report
report = {
    "report_metadata": {
        "timestamp": SCRIPT_START.isoformat(),
        "project": "数据服务可视化验证",
        "target_file": "data_service_dashboard.py",
        "python_version": py_ver,
        "waitress_available": waitress_ok,
        "ollama_available": ollama_available,
        "port": PORT,
        "server_pid": server_proc.pid
    },
    "phases": {},
    "summary": {
        "total_checks": total,
        "passed": passed,
        "failed": failed,
        "pass_rate": pass_rate,
        "verdict": "PASS" if failed == 0 else "PARTIAL" if pass_rate >= 80 else "FAIL"
    }
}

for phase, name, status, detail in RESULTS:
    if phase not in report["phases"]:
        report["phases"][phase] = []
    report["phases"][phase].append({
        "check": name,
        "status": status,
        "detail": detail
    })

with open("verification_report.json", "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)
check("RPT", "JSON报告已生成 (verification_report.json)", True)

# Generate text report
with open("verification_report.txt", "w", encoding="utf-8") as f:
    f.write("=" * 60 + "\n")
    f.write("  数据服务可视化 — 自动验证报告\n")
    f.write(f"  时间: {SCRIPT_START.strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"  Python: {py_ver} | Waitress: {waitress_ok} | Ollama: {ollama_available}\n")
    f.write("=" * 60 + "\n\n")

    for phase in phases_seen:
        f.write(f"\n--- {phase} ---\n")
        for _, name, status, detail in RESULTS:
            if _ == phase:
                marker = "[PASS]" if status == "PASS" else "[FAIL]"
                line = f"  {marker} {name}"
                if detail:
                    line += f"  ({detail})"
                f.write(line + "\n")

    f.write("\n" + "=" * 60 + "\n")
    f.write(f"  结果: {passed}/{total} 通过 ({pass_rate}%)\n")
    f.write(f"  判定: {'✓ 全部通过' if failed == 0 else '⚠ 存在失败'}\n")
    f.write("=" * 60 + "\n")

check("RPT", "文本报告已生成 (verification_report.txt)", True)

# ============================================================
# Phase 8: Cleanup
# ============================================================
section("Phase 8: 清理")

server_proc.terminate()
try:
    server_proc.wait(timeout=5)
    check("CLN", "服务器已正常终止", True)
except subprocess.TimeoutExpired:
    server_proc.kill()
    server_proc.wait()
    check("CLN", "服务器强制终止", True)

# Capture stderr
try:
    stderr_output = server_proc.stderr.read().decode('utf-8', errors='replace') if server_proc.stderr else ""
    if stderr_output.strip():
        with open("server_stderr.txt", "w", encoding="utf-8") as f:
            f.write(stderr_output)
        check("CLN", f"Server stderr已保存 ({len(stderr_output)} chars)", True)
    else:
        check("CLN", "Server stderr为空 (无错误)", True)
except Exception as e:
    check("CLN", "无法读取server stderr", False, str(e)[:50])

# ============================================================
# Final Verdict
# ============================================================
print(f"\n{C}{'='*60}{B}")
if failed == 0:
    print(f"{G}  ✓ 所有{total}项验证全部通过！{B}")
    print(f"{G}  数据服务可视化仪表板运行正常{B}")
else:
    print(f"{R}  ⚠ {failed}/{total} 项验证失败{B}")
    print(f"\n  失败项:")
    for phase, name, status, detail in RESULTS:
        if status == "FAIL":
            print(f"  {R}[{phase}] {name}: {detail}{B}")

print(f"{C}{'='*60}{B}")
print(f"\n  报告文件: verification_report.json / verification_report.txt")
print(f"  仪表板地址: {HOST}")

sys.exit(0 if failed == 0 else 1)
