"""
一键启动伪分布式所有服务
"""
import subprocess, time, sys, os

BASE = os.path.dirname(os.path.abspath(__file__))

def run_wsl_bg(cmd, label):
    """在 WSL 后台启动服务"""
    full = f"nohup {cmd} > /tmp/{label}.log 2>&1 & echo PID:$!"
    r = subprocess.run(
        ["wsl", "-d", "Ubuntu", "--", "bash", "-c", full],
        capture_output=True, text=True
    )
    pid = r.stdout.strip().split("PID:")[-1].strip() if "PID:" in r.stdout else "?"
    print(f"  [{label}] 已启动 (PID: {pid})")
    return pid

def run_win_bg(cmd, label):
    """在 Windows 后台启动进程"""
    subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"  [{label}] 已启动")

print(f"""
{'='*60}
  启动伪分布式集群
{'='*60}
""")

# 1. Redis
print("[1/4] 启动 Redis (WSL)...")
subprocess.run(["wsl", "-d", "Ubuntu", "--", "bash", "-c",
    "sudo service redis-server start 2>/dev/null; redis-cli ping 2>/dev/null || redis-server --daemonize yes 2>/dev/null; redis-cli ping"],
    capture_output=True)

# 2. Kafka
print("[2/4] 启动 Kafka (WSL KRaft)...")
kafka_dir = "/opt/kafka"
# Kill any existing Kafka
subprocess.run(["wsl", "-d", "Ubuntu", "--", "bash", "-c",
    f"pkill -f 'kafka.Kafka' 2>/dev/null; sleep 1; {kafka_dir}/bin/kafka-server-start.sh -daemon {kafka_dir}/config/kraft/server.properties"],
    capture_output=True)
time.sleep(3)

# Verify Kafka
r = subprocess.run(["wsl", "-d", "Ubuntu", "--", "bash", "-c",
    f"{kafka_dir}/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1"],
    capture_output=True, text=True)
if r.returncode == 0:
    print(f"  [Kafka] 启动成功 [OK] (topics: {r.stdout.strip()})")
else:
    print(f"  [Kafka] 可能仍在启动中... ({r.stderr.strip()[:100]})")

# 3. Flink
print("[3/4] 启动 Flink Standalone (Windows)...")
flink_dir = os.path.join(BASE, "flink")
if os.path.exists(os.path.join(flink_dir, "bin", "start-cluster.bat")):
    subprocess.run([os.path.join(flink_dir, "bin", "start-cluster.bat")], shell=True,
                   cwd=flink_dir, capture_output=True)
    time.sleep(5)
    # Verify
    try:
        import urllib.request
        urllib.request.urlopen("http://localhost:8081/config", timeout=5)
        print("  [Flink] JobManager 启动成功 [OK] (http://localhost:8081)")
    except:
        print("  [Flink] 启动中... 请稍后访问 http://localhost:8081")
else:
    print("  [Flink] 未安装，请先运行 setup_all.py")

# 4. Dashboard
print("[4/4] 仪表盘已运行在 http://127.0.0.1:8088")
print("  (如果未启动: python ../dashboard_app.py)")

print(f"""
{'='*60}
  所有服务已启动
{'='*60}

  验证端口:
    Kafka:  localhost:9092 (WSL)
    Redis:  localhost:6379 (WSL)
    Flink:  localhost:8081 (Windows)
    仪表盘: localhost:8088

  下一步:
    python test_pipeline.py    # 端到端全链路测试
    python test_kafka.py       # 单独测 Kafka
    python test_redis.py       # 单独测 Redis
    python test_flink.py       # 单独测 Flink
""")
