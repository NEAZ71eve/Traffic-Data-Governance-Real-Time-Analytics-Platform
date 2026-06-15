# -*- coding: utf-8 -*-
"""Comprehensive automated test for data_service_dashboard.py"""
import subprocess, time, json, urllib.request, sys, os

os.chdir(r"D:\s\新项目")

proc = subprocess.Popen([sys.executable, "data_service_dashboard.py"],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(4)

results = []
def check(name, ok, detail=""):
    if ok:
        results.append(("PASS", name))
    else:
        results.append(("FAIL", f"{name}: {detail}"))

def get(path):
    resp = urllib.request.urlopen(f"http://127.0.0.1:8089{path}", timeout=10)
    return json.loads(resp.read())

def post(path, body):
    data = json.dumps(body).encode('utf-8')
    req = urllib.request.Request(f"http://127.0.0.1:8089{path}", data=data,
        headers={'Content-Type': 'application/json; charset=utf-8'})
    return json.loads(urllib.request.urlopen(req, timeout=10).read())

# Tab 1: Data Collection
try:
    d = get("/api/data_collection")
    check("Tab1: Data Collection", d["total_sources"] == 4, f"sources={d['total_sources']}")
    check("Tab1: 4 Kafka topics", len(d["kafka"]["topics"]) == 4, f"topics={len(d['kafka']['topics'])}")
    check("Tab1: 24 hours", len(d["hourly"]) == 24)
    check("Tab1: 4 sources detail", len(d["sources"]) == 4)
except Exception as e:
    check("Tab1: Data Collection", False, str(e))

# Tab 2: Stream Processing
try:
    d = get("/api/stream_processing")
    check("Tab2: 3 Flink jobs", len(d["jobs"]) == 3)
    check("Tab2: checkpoint rate > 90", d["checkpoint_success_rate"] > 90)
except Exception as e:
    check("Tab2: Stream Processing", False, str(e))

# Tab 3: Data Warehouse
try:
    d = get("/api/data_warehouse")
    check("Tab3: 24 tables", d["total_tables"] == 24, f"got {d['total_tables']}")
    check("Tab3: ODS layer", "ODS" in d["catalog"])
    check("Tab3: ADS layer", "ADS" in d["catalog"])
    check("Tab3: 5 ETL tasks", len(d["etl"]["tasks"]) == 5)
except Exception as e:
    check("Tab3: Data Warehouse", False, str(e))

# Tab 4: Data Quality
try:
    d = get("/api/data_quality")
    check("Tab4: has score", "score" in d)
    check("Tab4: has tables", len(d["tables"]) > 0)
    check("Tab4: has trend", len(d["trend"]) > 0)
except Exception as e:
    check("Tab4: Data Quality", False, str(e))

# Tab 5: Data Lineage
try:
    d = get("/api/data_lineage")
    check("Tab5: >=16 nodes", d["total_nodes"] >= 16, f"got {d['total_nodes']}")
    check("Tab5: >=16 edges", d["total_edges"] >= 16, f"got {d['total_edges']}")
    check("Tab5: has layers", "ODS" in d["layers"] and "ADS" in d["layers"])
except Exception as e:
    check("Tab5: Data Lineage", False, str(e))

try:
    d = get("/api/data_lineage/table/ads_traffic_operation")
    check("Tab5: drill upstream", len(d["upstream"]) > 0, f"got {len(d['upstream'])}")
    check("Tab5: drill table name", d["table"] == "ads_traffic_operation")
except Exception as e:
    check("Tab5: Lineage drill", False, str(e))

# Tab 6: AI Assistant
try:
    d = get("/api/ai_assistant")
    check("Tab6: 8 query types", len(d["query_types"]) == 8, f"got {len(d['query_types'])}")
except Exception as e:
    check("Tab6: AI Assistant", False, str(e))

# NL2SQL queries
nl2sql_tests = [
    ("昨天最拥堵的5条路", "road_jam_rank"),
    ("今天长安街的车流量", "road_flow"),
    ("设备健康评分最低的3台设备", "device_health"),
    ("哪些设备离线了", "device_offline"),
    ("各区域拥堵情况", "area_congestion"),
]
for q, expected_intent in nl2sql_tests:
    try:
        d = post("/api/ai_assistant/query", {"question": q})
        intent_ok = d["intent"] == expected_intent
        sql_ok = "SELECT" in d["sql"].upper()
        check(f"NL2SQL intent: {expected_intent}", intent_ok, f"got {d['intent']}")
        check(f"NL2SQL SQL: {expected_intent}", sql_ok, f"sql={d['sql'][:80]}")
    except Exception as e:
        check(f"NL2SQL: {expected_intent}", False, str(e))

# Tab 7: Full Chain
try:
    d = get("/api/full_chain")
    check("Tab7: 7 running", d["running"] == 7, f"got {d['running']}")
    check("Tab7: 6 pipeline stages", len(d["pipeline"]) == 6)
    check("Tab7: has alerts", len(d["alerts"]) > 0)
    check("Tab7: 7 components", len(d["components"]) == 7)
except Exception as e:
    check("Tab7: Full Chain", False, str(e))

# HTML page
try:
    resp = urllib.request.urlopen("http://127.0.0.1:8089/", timeout=10)
    html = resp.read().decode('utf-8')
    check("HTML: >10KB", len(html) > 10000, f"got {len(html)}")
    check("HTML: title present", "数据服务可视化" in html)
    check("HTML: all 7 tabs", all(f"load{i}" in html for i in range(1, 8)))
except Exception as e:
    check("HTML page", False, str(e))

# Print results
print("\n" + "=" * 60)
passed = sum(1 for s, _ in results if s == "PASS")
failed = sum(1 for s, _ in results if s == "FAIL")
print(f"RESULTS: {passed} passed, {failed} failed, {len(results)} total")
print("=" * 60)
for status, name in results:
    marker = "PASS" if status == "PASS" else "FAIL"
    print(f"  [{marker}] {name}")
print("=" * 60)

proc.terminate()
proc.wait()

if failed > 0:
    print(f"\n{failed} TEST(S) FAILED!")
    sys.exit(1)
else:
    print("\nALL TESTS PASSED!")
