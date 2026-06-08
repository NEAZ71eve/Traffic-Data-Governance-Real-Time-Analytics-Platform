#!/usr/bin/env python3
"""
Docker 环境检查 + 镜像预拉取
用法: python docker/setup_docker.py
"""
import subprocess, sys, os

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

def run(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return r.returncode, r.stdout + r.stderr

def check(title, ok_msg, cmd):
    code, out = run(cmd)
    if code == 0:
        print(f"  {GREEN}[OK]{RESET} {title}: {ok_msg}")
        return True
    else:
        print(f"  {RED}[FAIL]{RESET} {title}: {out.strip()[-100:]}")
        return False

def section(msg):
    print(f"\n{'='*60}\n  {msg}\n{'='*60}")

# ============================================================
# 1. Docker 环境检查
# ============================================================
section("1. Docker 环境检查")

check("Docker CLI", "", "docker --version")
check("Docker Compose", "", "docker compose version")
code, out = run("docker info 2>&1")
if code == 0:
    for line in out.split("\n"):
        if any(k in line for k in ["Server Version", "CPUs", "Total Memory", "Operating System"]):
            print(f"       {line.strip()}")
else:
    print(f"  {RED}[FAIL]{RESET} Docker daemon 未运行，请启动 Docker Desktop")

# ============================================================
# 2. 网络连通性
# ============================================================
section("2. Docker Hub 连通性")

can_pull = check("Docker Hub 拉取测试", "",
    "timeout 10 docker pull hello-world:latest 2>&1 || echo PULL_FAILED")

if not can_pull:
    print(f"\n  {YELLOW}Docker Hub 不可达！{RESET}")
    print(f"  解决方法:")
    print(f"    1. 配置镜像加速器: Docker Desktop → Settings → Docker Engine")
    print(f"       添加: \"registry-mirrors\": [\"https://docker.m.daocloud.io\"]")
    print(f"    2. 或使用 VPN/代理")
    print(f"    3. 或手动下载镜像后 docker load")

# ============================================================
# 3. 已有镜像
# ============================================================
section("3. 本地已有镜像")

_, out = run("docker images --format 'table {{.Repository}}:{{.Tag}}\t{{.Size}}'")
print(out[:2000])

# ============================================================
# 4. 拉取所需镜像
# ============================================================
section("4. 拉取项目依赖镜像 (约1.5GB)")

IMAGES = {
    "python": "python:3.12-slim",
    "kafka": "bitnami/kafka:3.7",
    "flink": "flink:1.18-scala_2.12",
    "redis": "redis:7-alpine",
}

# Check if redis:6-alpine exists (can be used as fallback)
code, _ = run("docker images -q redis:6-alpine")
has_redis6 = code == 0

for name, image in IMAGES.items():
    if name == "redis" and has_redis6:
        print(f"  {YELLOW}[SKIP]{RESET} {image} — 已有 redis:6-alpine 可替代")
        continue
    print(f"  拉取 {image}...")
    code, out = run(f"timeout 60 docker pull {image} 2>&1")
    if code == 0:
        print(f"  {GREEN}[OK]{RESET}")
    else:
        print(f"  {RED}[FAIL]{RESET} — 请手动拉取或用替代镜像")

# ============================================================
# 5. 构建应用镜像
# ============================================================
section("5. 构建应用镜像")

_, out = run("docker compose -p traffic build app 2>&1")
if "ERROR" in out.upper():
    print(f"  {RED}[FAIL]{RESET} 构建失败")
    print(out[-1000:])
else:
    print(f"  {GREEN}[OK]{RESET} 应用镜像构建成功")

# ============================================================
print(f"\n{'='*60}")
print(f"  执行 'make up' 启动全部服务")
print(f"{'='*60}")
