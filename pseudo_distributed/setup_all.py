"""
伪分布式一键安装脚本
自动下载和配置 Kafka (WSL) + Flink (Windows) + Redis (WSL)
"""
import os, subprocess, sys, time, urllib.request, tarfile, zipfile, shutil

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "data")
os.makedirs(DATA_DIR, exist_ok=True)

def run(cmd, cwd=None, shell=True):
    print(f"  RUN: {cmd}")
    r = subprocess.run(cmd, cwd=cwd, shell=shell, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  STDERR: {r.stderr[:300]}")
    return r

def run_wsl(cmd):
    return run(f'wsl -d Ubuntu -- bash -c "{cmd}"')

def download(url, dest):
    print(f"  Downloading {url} -> {dest} ...")
    try:
        urllib.request.urlretrieve(url, dest)
        print(f"  OK: {os.path.getsize(dest)} bytes")
        return True
    except Exception as e:
        print(f"  FAIL: {e}")
        return False

def step(msg):
    print(f"\n{'='*60}\n  {msg}\n{'='*60}")

# ============================================================
# Step 1: Kafka in WSL
# ============================================================
step("Step 1/4: 安装 Kafka 到 WSL Ubuntu")
kafka_dir = "/opt/kafka"
rc = run_wsl(f"test -d {kafka_dir} && echo 'EXISTS' || echo 'NOT_FOUND'")
if "EXISTS" in rc.stdout:
    print("  Kafka 已安装，跳过")
else:
    print("  下载 Kafka 3.7.0 (约130MB)...")
    run_wsl("cd /tmp && rm -f kafka.tgz")
    ok = run_wsl(
        "curl -sL --connect-timeout 30 --max-time 180 "
        "-o /tmp/kafka.tgz "
        "'https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz' "
        "&& ls -lh /tmp/kafka.tgz"
    )
    if ok.returncode != 0 or os.path.getsize("/tmp/kafka.tgz") < 1000000:
        print("  archive.apache.org 失败，尝试清华镜像...")
        run_wsl(
            "cd /tmp && wget -q --timeout=180 "
            "https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/3.7.0/kafka_2.13-3.7.0.tgz "
            "-O kafka.tgz"
        )

    print("  解压 Kafka...")
    run_wsl(f"sudo mkdir -p {kafka_dir} && cd /tmp && sudo tar xzf kafka.tgz -C /opt/ && sudo mv /opt/kafka_2.13-3.7.0/* {kafka_dir}/")

    # KRaft mode config
    print("  配置 KRaft 模式 (无 Zookeeper)...")
    kafka_uuid = run_wsl(f"{kafka_dir}/bin/kafka-storage.sh random-uuid").stdout.strip()
    print(f"  Cluster UUID: {kafka_uuid}")
    run_wsl(f"{kafka_dir}/bin/kafka-storage.sh format -t {kafka_uuid} -c {kafka_dir}/config/kraft/server.properties 2>&1")

    # Configure advertised listeners for cross-OS access
    run_wsl(f"sed -i 's/^#listeners=.*/listeners=PLAINTEXT:\\/\\/0.0.0.0:9092/' {kafka_dir}/config/kraft/server.properties")
    run_wsl(f"sed -i 's/^#advertised.listeners=.*/advertised.listeners=PLAINTEXT:\\/\\/localhost:9092/' {kafka_dir}/config/kraft/server.properties")

    print("  Kafka 安装完成 [OK]")

# ============================================================
# Step 2: Redis in WSL
# ============================================================
step("Step 2/4: 配置 Redis (WSL)")
rc = run_wsl("which redis-server 2>/dev/null && echo 'FOUND' || echo 'NOT_FOUND'")
if "NOT_FOUND" in rc.stdout:
    print("  安装 Redis...")
    run_wsl("sudo apt-get update -qq && sudo apt-get install -y -qq redis-server")
else:
    print("  Redis 已安装 [OK]")

# Allow remote connections
run_wsl("sudo sed -i 's/^bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/redis.conf 2>/dev/null; sudo sed -i 's/^protected-mode yes/protected-mode no/' /etc/redis/redis.conf 2>/dev/null")

# ============================================================
# Step 3: Flink on Windows
# ============================================================
step("Step 3/4: 安装 Flink 到 Windows")
flink_dir = os.path.join(BASE, "flink")
if os.path.exists(os.path.join(flink_dir, "bin", "start-cluster.bat")):
    print("  Flink 已安装，跳过")
else:
    flink_url = "https://archive.apache.org/dist/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz"
    flink_tgz = os.path.join(BASE, "flink.tgz")
    if download(flink_url, flink_tgz):
        print("  解压 Flink...")
        import tarfile
        with tarfile.open(flink_tgz) as tf:
            tf.extractall(BASE)
        # Rename
        extracted = os.path.join(BASE, "flink-1.18.1")
        if os.path.exists(extracted) and not os.path.exists(flink_dir):
            shutil.move(extracted, flink_dir)
        os.remove(flink_tgz)
        print("  Flink 安装完成 [OK]")
    else:
        print("  !! Flink 下载失败，请手动下载:")
        print(f"     将 flink-1.18.1 解压到: {flink_dir}")
        print("     下载地址: https://flink.apache.org/downloads/")

# Configure Flink
conf = os.path.join(flink_dir, "conf", "flink-conf.yaml")
if os.path.exists(conf):
    with open(conf, "r") as f:
        content = f.read()
    # Ensure single-node config
    if "taskmanager.numberOfTaskSlots: 4" not in content:
        with open(conf, "a") as f:
            f.write("\ntaskmanager.numberOfTaskSlots: 4\n")
            f.write("parallelism.default: 1\n")
            f.write("jobmanager.rpc.address: localhost\n")
            f.write("rest.port: 8081\n")
        print("  Flink 配置已更新 (1 TM, 4 slots) [OK]")

# ============================================================
# Step 4: Python dependencies
# ============================================================
step("Step 4/4: 安装 Python 依赖")
run(f"{sys.executable} -m pip install kafka-python redis apscheduler pyspark -q")

# ============================================================
# Summary
# ============================================================
print(f"""
{'='*60}
  安装完成！
{'='*60}

  目录结构:
    {BASE}/
      ├── data/         ← 数据文件 (HDFS 替代)
      ├── flink/        ← Flink 1.18.1
      ├── start_all.py  ← 一键启动
      ├── stop_all.py   ← 一键停止
      ├── test_kafka.py ← Kafka 测试
      ├── test_flink.py ← Flink 测试
      ├── test_redis.py ← Redis 测试
      ├── test_hive_sql.py ← Hive/SQL 测试
      ├── test_hdfs.py  ← HDFS 模拟测试
      └── test_pipeline.py ← 端到端全链路

  下一步:
    cd {BASE}
    python start_all.py      # 启动所有服务
    python test_pipeline.py  # 端到端测试
""")
