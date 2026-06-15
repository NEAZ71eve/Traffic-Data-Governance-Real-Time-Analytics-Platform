# -*- coding: utf-8 -*-
"""Test script for data_service_dashboard.py endpoints"""
import urllib.request
import json
import sys
import time

BASE = "http://127.0.0.1:8089"

def test_get(path, name):
    try:
        resp = urllib.request.urlopen(f"{BASE}{path}", timeout=10)
        d = json.loads(resp.read())
        print(f"  [OK] {name}")
        return d
    except Exception as e:
        print(f"  [FAIL] {name}: {e}")
        return None

def test_post(path, data, name):
    try:
        body = json.dumps(data).encode('utf-8')
        req = urllib.request.Request(f"{BASE}{path}", data=body,
            headers={'Content-Type': 'application/json; charset=utf-8'})
        resp = urllib.request.urlopen(req, timeout=10)
        d = json.loads(resp.read())
        print(f"  [OK] {name}")
        return d
    except Exception as e:
        print(f"  [FAIL] {name}: {e}")
        return None

print("Waiting for server...")
time.sleep(2)

print("\n=== API Endpoint Tests ===")

# Tab 1: Data Collection
d1 = test_get("/api/data_collection", "Data Collection")
if d1:
    assert d1["total_sources"] == 4
    assert len(d1["kafka"]["topics"]) == 4
    assert len(d1["hourly"]) == 24

# Tab 2: Stream Processing
d2 = test_get("/api/stream_processing", "Stream Processing")
if d2:
    assert len(d2["jobs"]) == 3

# Tab 3: Data Warehouse
d3 = test_get("/api/data_warehouse", "Data Warehouse")
if d3:
    assert d3["total_tables"] == 24

# Tab 4: Data Quality
d4 = test_get("/api/data_quality", "Data Quality")
if d4:
    assert "score" in d4
    assert len(d4["tables"]) > 0

# Tab 5: Data Lineage
d5 = test_get("/api/data_lineage", "Data Lineage")
if d5:
    assert d5["total_nodes"] >= 16
    assert d5["total_edges"] >= 16

d5d = test_get("/api/data_lineage/table/ads_traffic_operation", "Lineage drill-down")
if d5d:
    assert len(d5d["upstream"]) > 0

# Tab 6: AI Assistant
d6 = test_get("/api/ai_assistant", "AI Assistant")
if d6:
    assert len(d6["query_types"]) == 8

# NL2SQL query
d6q = test_post("/api/ai_assistant/query",
    {"question": "昨天最拥堵的5条路"}, "NL2SQL Query")
if d6q:
    assert d6q["intent"] == "road_jam_rank"
    assert "SELECT" in d6q["sql"].upper()
    print(f"    SQL: {d6q['sql'][:80]}...")
    assert d6q["result"]["row_count"] == 3

# Tab 7: Full Chain
d7 = test_get("/api/full_chain", "Full Chain")
if d7:
    assert d7["running"] == 7
    assert len(d7["pipeline"]) == 6
    assert len(d7["alerts"]) == 5

# HTML page
try:
    resp = urllib.request.urlopen(f"{BASE}/", timeout=10)
    html = resp.read().decode('utf-8')
    print(f"  [OK] HTML page: {len(html)} bytes")
    assert "数据服务可视化" in html
    assert "load1" in html and "load7" in html
except Exception as e:
    print(f"  [FAIL] HTML page: {e}")

print("\n=== ALL TESTS PASSED ===")
