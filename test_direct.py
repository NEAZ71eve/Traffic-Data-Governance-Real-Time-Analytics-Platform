# -*- coding: utf-8 -*-
"""Direct test of Flask endpoint without bash encoding issues"""
import subprocess, time, json, urllib.request, sys, os

os.chdir(r"D:\s\新项目")

# Start server
proc = subprocess.Popen([sys.executable, "data_service_dashboard.py"],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(4)

try:
    # Test NL2SQL query
    data = json.dumps({"question": "昨天最拥堵的5条路"}).encode('utf-8')
    req = urllib.request.Request('http://127.0.0.1:8089/api/ai_assistant/query',
        data=data, headers={'Content-Type': 'application/json; charset=utf-8'})
    resp = urllib.request.urlopen(req)
    d = json.loads(resp.read())

    print(f"Intent: {d['intent']}")
    print(f"SQL length: {len(d['sql'])}")
    print(f"SQL has SELECT: {'SELECT' in d['sql'].upper()}")
    print(f"SQL first 200 chars:")
    print(d['sql'][:200])

    # Also test other endpoints
    for path in ['/api/data_collection', '/api/stream_processing',
                 '/api/data_warehouse', '/api/data_quality',
                 '/api/data_lineage', '/api/ai_assistant', '/api/full_chain']:
        resp = urllib.request.urlopen(f'http://127.0.0.1:8089{path}')
        d = json.loads(resp.read())
        print(f"[OK] {path}")

    # Test lineage drill-down
    resp = urllib.request.urlopen('http://127.0.0.1:8089/api/data_lineage/table/ads_traffic_operation')
    d = json.loads(resp.read())
    print(f"[OK] Lineage drill: upstream={len(d['upstream'])}, downstream={len(d['downstream'])}")

    # HTML page
    resp = urllib.request.urlopen('http://127.0.0.1:8089/')
    html = resp.read().decode('utf-8')
    print(f"[OK] HTML page: {len(html)} bytes, has tabs: {'load1' in html and 'load7' in html}")

except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
finally:
    proc.terminate()
    proc.wait()
    # Print any stderr from server
    stderr = proc.stderr.read().decode('utf-8', errors='replace')
    if stderr:
        print(f"\n--- Server stderr ---\n{stderr[:500]}")
