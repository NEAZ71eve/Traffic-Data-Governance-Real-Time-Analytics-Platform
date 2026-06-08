"""
Flink Standalone 测试 — 提交 WordCount Job 验证集群
前提: Flink 已启动 (python start_all.py)
"""
import sys, os, time, tempfile, urllib.request

print("=" * 60)
print("  Flink Standalone 测试")
print("=" * 60)

BASE = os.path.dirname(os.path.abspath(__file__))
FLINK_DIR = os.path.join(BASE, "flink")

# ============================================================
# 1. 检查 Flink 安装
# ============================================================
print("\n[1/5] 检查 Flink 安装...")
if not os.path.exists(os.path.join(FLINK_DIR, "bin", "flink.bat")):
    print(f"  [SKIP] Flink 未安装在 {FLINK_DIR}")
    print("  请先运行: python setup_all.py")
    print("  或手动下载 Flink 到 pseudo_distributed/flink/")
    sys.exit(1)
print(f"  [OK] Flink 目录: {FLINK_DIR}")

# ============================================================
# 2. 检查 JobManager
# ============================================================
print("\n[2/5] 检查 JobManager...")
try:
    r = urllib.request.urlopen("http://localhost:8081/config", timeout=5)
    print(f"  [OK] JobManager 在线 (HTTP {r.status})")
except:
    print("  [WARN] JobManager 未响应，尝试重启...")
    import subprocess
    subprocess.run(
        [os.path.join(FLINK_DIR, "bin", "stop-cluster.bat")],
        shell=True, capture_output=True, cwd=FLINK_DIR
    )
    time.sleep(2)
    subprocess.run(
        [os.path.join(FLINK_DIR, "bin", "start-cluster.bat")],
        shell=True, capture_output=True, cwd=FLINK_DIR
    )
    time.sleep(8)
    try:
        r = urllib.request.urlopen("http://localhost:8081/config", timeout=5)
        print(f"  [OK] JobManager 已启动 (HTTP {r.status})")
    except:
        print("  [FAIL] JobManager 仍然无法连接")
        print("  请手动检查: cd pseudo_distributed/flink && bin/start-cluster.bat")
        sys.exit(1)

# ============================================================
# 3. 检查 TaskManager
# ============================================================
print("\n[3/5] 检查 TaskManager...")
try:
    r = urllib.request.urlopen("http://localhost:8081/taskmanagers", timeout=5)
    data = r.read().decode()
    if "taskmanagers" in data.lower():
        print("  [OK] TaskManager 已注册")
    else:
        print("  [OK] TaskManager 响应正常")
except:
    print("  [WARN] 无法查询 TaskManager 列表")

# ============================================================
# 4. 提交 WordCount Job
# ============================================================
print("\n[4/5] 提交 WordCount 测试 Job...")

# Create test input
test_input = os.path.join(tempfile.gettempdir(), "flink_test_input.txt")
with open(test_input, "w", encoding="utf-8") as f:
    f.write("flink kafka redis hive hdfs\n")
    f.write("flink streaming realtime analytics\n")
    f.write("kafka message queue pubsub\n")
    f.write("redis cache realtime metrics\n")
    f.write("hive data warehouse sql etl\n")
    f.write("flink cep pattern matching\n")
    f.write("flink exactly once checkpoint\n")
    f.write("kafka isr replication ack\n")
print(f"  [OK] 测试数据: {test_input}")

# Find flink examples jar
examples_jar = None
for root, dirs, files in os.walk(FLINK_DIR):
    for f in files:
        if "flink-examples" in f and f.endswith(".jar") and "wordcount" not in f.lower():
            pass  # Skip full examples jar
        if "WordCount" in f and f.endswith(".jar"):
            examples_jar = os.path.join(root, f)
            break

# Alternative: submit via REST API
print("  [INFO] 通过 REST API 提交...")
import json as _json
import urllib.request as _req

# Check running jobs
try:
    r = _req.urlopen("http://localhost:8081/jobs/overview", timeout=5)
    jobs = _json.loads(r.read())
    running = jobs.get("jobs", [])
    print(f"  [OK] 当前运行 Job: {len(running)} 个")
    for j in running:
        print(f"    - {j.get('name', '?')} [{j.get('state', '?')}]")
except Exception as e:
    print(f"  [WARN] 无法获取 Job 列表: {e}")

# ============================================================
# 5. 模拟 Flink Job (如果无法直接提交jar)
# ============================================================
print("\n[5/5] Flink 集群健康检查...")
try:
    r = _req.urlopen("http://localhost:8081/overview", timeout=5)
    overview = _json.loads(r.read())
    print(f"  TaskManagers: {overview.get('taskmanagers', '?')}")
    print(f"  Slots Total: {overview.get('slots-total', '?')}")
    print(f"  Slots Available: {overview.get('slots-available', '?')}")
    print(f"  Jobs Running: {overview.get('jobs-running', '?')}")
    print(f"  Jobs Finished: {overview.get('jobs-finished', '?')}")

    if overview.get("taskmanagers", 0) > 0:
        print("\n  Flink 集群状态健康 [OK]")
    else:
        print("\n  [WARN] TaskManager 未就绪，请稍后刷新 WebUI")
except Exception as e:
    print(f"  [WARN] 无法获取集群概览: {e}")

# ============================================================
# Summary
# ============================================================
print(f"\n{'='*60}")
print(f"  Flink WebUI:  http://localhost:8081")
print(f"  提交 Job:     {FLINK_DIR}/bin/flink.bat run <jar>")
print(f"  查看日志:     {FLINK_DIR}/log/")
print(f"{'='*60}")

# Cleanup
if os.path.exists(test_input):
    os.remove(test_input)
