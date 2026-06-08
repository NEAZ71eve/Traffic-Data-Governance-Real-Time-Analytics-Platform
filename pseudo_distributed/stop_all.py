"""
一键停止所有服务
"""
import subprocess, os

print("停止所有服务...")

# Redis
print("[1/3] 停止 Redis (WSL)...")
subprocess.run(["wsl", "-d", "Ubuntu", "--", "bash", "-c",
    "sudo service redis-server stop 2>/dev/null; redis-cli shutdown 2>/dev/null; echo done"],
    capture_output=True)

# Kafka
print("[2/3] 停止 Kafka (WSL)...")
subprocess.run(["wsl", "-d", "Ubuntu", "--", "bash", "-c",
    "pkill -f 'kafka.Kafka' 2>/dev/null; echo done"],
    capture_output=True)

# Flink
print("[3/3] 停止 Flink (Windows)...")
BASE = os.path.dirname(os.path.abspath(__file__))
flink_stop = os.path.join(BASE, "flink", "bin", "stop-cluster.bat")
if os.path.exists(flink_stop):
    subprocess.run([flink_stop], shell=True, capture_output=True)
else:
    # Kill Java processes named Flink
    subprocess.run("taskkill /F /IM java.exe 2>nul", shell=True, capture_output=True)

print("\n所有服务已停止 [OK]")
