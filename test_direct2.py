# -*- coding: utf-8 -*-
"""Direct test that writes response to file to avoid encoding issues"""
import subprocess, time, json, urllib.request, sys, os

os.chdir(r"D:\s\新项目")

proc = subprocess.Popen([sys.executable, "data_service_dashboard.py"],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(4)

try:
    data = json.dumps({"question": "昨天最拥堵的5条路"}).encode('utf-8')
    req = urllib.request.Request('http://127.0.0.1:8089/api/ai_assistant/query',
        data=data, headers={'Content-Type': 'application/json; charset=utf-8'})
    resp = urllib.request.urlopen(req)
    raw = resp.read()

    # Write to file
    with open("test_result.json", "wb") as f:
        f.write(raw)

    d = json.loads(raw)
    with open("test_result.txt", "w", encoding="utf-8") as f:
        f.write(f"Intent: {d['intent']}\n")
        f.write(f"SQL length: {len(d['sql'])}\n")
        f.write(f"SQL hex (first 100): {d['sql'][:100].encode('utf-8').hex()}\n")
        f.write(f"SQL:\n{d['sql']}\n")
        f.write(f"Has SELECT: {'SELECT' in d['sql'].upper()}\n")

    print("Test complete. Check test_result.txt")

    # Also test from ai_assistant directly
    sys.path.insert(0, "python")
    from nl2sql_enhanced import NL2SQLConverter
    c = NL2SQLConverter()
    sql = c.to_sql("昨天最拥堵的5条路")
    with open("test_direct_nl2sql.txt", "w", encoding="utf-8") as f:
        f.write(f"Direct NL2SQL:\n{sql}\n")
        f.write(f"Has SELECT: {'SELECT' in sql}\n")

    print("Direct test complete. Check test_direct_nl2sql.txt")

except Exception as e:
    with open("test_error.txt", "w", encoding="utf-8") as f:
        f.write(f"ERROR: {e}\n")
        import traceback
        traceback.print_exc(file=f)
    print(f"ERROR: {e}")
finally:
    proc.terminate()
    proc.wait()
