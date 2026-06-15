# -*- coding: utf-8 -*-
"""Minimal Flask test for NL2SQL endpoint"""
import subprocess, time, json, urllib.request, sys, os

os.chdir(r"D:\s\新项目")

proc = subprocess.Popen([sys.executable, "data_service_dashboard.py"],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(4)

try:
    # Minimal test
    data = json.dumps({"question": "test"}).encode('utf-8')
    req = urllib.request.Request('http://127.0.0.1:8089/api/ai_assistant/query',
        data=data, headers={'Content-Type': 'application/json; charset=utf-8'})
    resp = urllib.request.urlopen(req)
    d = json.loads(resp.read())

    with open("minimal_test.txt", "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    print("Done, check minimal_test.txt")

    # Check stderr
    time.sleep(1)
except Exception as e:
    print(f"ERROR: {e}")
finally:
    proc.terminate()
    time.sleep(0.5)
    stderr = proc.stderr.read()
    if stderr:
        with open("server_stderr.txt", "wb") as f:
            f.write(stderr)
        print(f"Server stderr written ({len(stderr)} bytes)")
    proc.wait()
