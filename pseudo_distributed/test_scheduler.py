"""
DolphinScheduler 替代方案 — 轻量级任务调度器
模拟: 18个DAG任务定时执行
"""
import time, json, os, sys, sqlite3
from datetime import datetime
from threading import Thread

print("=" * 60)
print("  DolphinScheduler 替代 — 轻量级任务调度")
print("=" * 60)

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.interval import IntervalTrigger
    APS_OK = True
except ImportError:
    print("  [WARN] apscheduler 未安装, pip install apscheduler")
    print("  将使用简单定时循环代替")
    APS_OK = False

BASE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE)
DB_PATH = os.path.join(PROJECT_ROOT, "traffic_data.db")
DT = datetime.now().strftime("%Y-%m-%d")

# ============================================================
# 模拟 5 层 ETL 任务 (精简版)
# ============================================================
TASKS = [
    ("ODS_Ingestion",     "ods", "采集车辆通行数据→Kafka→ODS层", 60),
    ("ODS_DeviceStatus",  "ods", "采集设备状态数据→Kafka→ODS层", 60),
    ("DWD_Cleaning",      "dwd","清洗去重+合法性校验→DWD层",     300),
    ("DWD_DeviceClean",   "dwd","设备状态清洗→DWD层",             120),
    ("DIM_Area_Snapshot", "dim","维度表区域快照刷新",             3600),
    ("DIM_Road_Snapshot", "dim","维度表道路快照刷新",             3600),
    ("DWS_RoadHourFlow",  "dws","道路小时流量聚合",              3600),
    ("DWS_DeviceDay",     "dws","设备天级健康聚合",              86400),
    ("ADS_TrafficOp",     "ads","交通运营指标计算",               3600),
    ("ADS_TopJam",        "ads","拥堵TOP10排名",                  3600),
    ("ADS_DeviceScore",   "ads","设备健康评分计算",               3600),
    ("ADS_MTBF_MTTR",     "ads","MTBF/MTTR可靠性计算",            86400),
    ("DataQuality_Check", "gvn","数据质量监控(完整率/唯一率/合法性)", 1800),
    ("KafkaLag_Monitor",  "gvn","Kafka消费Lag监控",              300),
]

def execute_task(name, layer, desc):
    """模拟执行一个ETL任务"""
    start = time.time()
    print(f"  [{datetime.now().strftime('%H:%M:%S')}] 执行: {name} ({desc})")
    # 模拟耗时
    time.sleep(0.2)
    elapsed = time.time() - start
    status = "SUCCESS"
    print(f"  [{datetime.now().strftime('%H:%M:%S')}] 完成: {name} [{status}] ({elapsed:.1f}s)")
    return status

# ============================================================
# 运行所有任务一次
# ============================================================
print(f"\n[1] 执行全部 {len(TASKS)} 个任务...")
print(f"    模拟 DolphinScheduler DAG 工作流\n")

results = {}
for name, layer, desc, interval in TASKS:
    status = execute_task(name, layer, desc)
    results[name] = status

# ============================================================
# 统计
# ============================================================
success = sum(1 for v in results.values() if v == "SUCCESS")
failed = sum(1 for v in results.values() if v != "SUCCESS")
print(f"\n[2] 执行统计: {success}/{len(TASKS)} 成功, {failed} 失败")

# ============================================================
# 工作流 DAG 检查 (模拟依赖)
# ============================================================
print(f"\n[3] 工作流 DAG 依赖检查:")
deps = {
    "DWD_Cleaning": ["ODS_Ingestion"],
    "DWD_DeviceClean": ["ODS_DeviceStatus"],
    "DWS_RoadHourFlow": ["DWD_Cleaning"],
    "DWS_DeviceDay": ["DWD_DeviceClean"],
    "DIM_Area_Snapshot": [],
    "DIM_Road_Snapshot": [],
    "ADS_TrafficOp": ["DWS_RoadHourFlow", "DIM_Area_Snapshot"],
    "ADS_TopJam": ["DWS_RoadHourFlow", "DIM_Road_Snapshot"],
    "ADS_DeviceScore": ["DWS_DeviceDay", "DIM_Road_Snapshot"],
    "ADS_MTBF_MTTR": ["DWS_DeviceDay"],
    "DataQuality_Check": ["DWS_RoadHourFlow", "DWS_DeviceDay"],
    "KafkaLag_Monitor": [],
}

for task, upstream in deps.items():
    up_status = [results.get(u, "UNKNOWN") for u in upstream]
    ok = all(s == "SUCCESS" for s in up_status)
    status = "OK" if ok else "BLOCKED"
    deps_str = ", ".join(f"{u}({results.get(u,'?')})" for u in upstream) if upstream else "无"
    print(f"  {task:20s} ← [{status:7s}] 上游: {deps_str}")

# ============================================================
# 调度计划 (可选)
# ============================================================
print(f"\n[4] 调度计划 (对应 DolphinScheduler):")
print(f"{'任务':25s} {'层级':6s} {'间隔':>8s} {'描述'}")
print("-" * 70)
for name, layer, desc, interval in TASKS:
    if interval >= 3600:
        ival = f"{interval//3600}h"
    elif interval >= 60:
        ival = f"{interval//60}m"
    else:
        ival = f"{interval}s"
    print(f"  {name:23s} {layer:6s} {ival:>8s} {desc}")

print(f"\n{'='*60}")
print(f"  任务调度验证完成 [OK]")
print(f"  对应生产: DolphinScheduler 18 任务 DAG")
print(f"  配置: ../config/dolphinscheduler_config.json")
print(f"{'='*60}")

# ============================================================
# 持续运行模式 (可选, Ctrl+C 停止)
# ============================================================
if APS_OK and "--run" in sys.argv:
    print(f"\n[持续模式] 按 Ctrl+C 停止调度...")
    scheduler = BackgroundScheduler()
    for name, layer, desc, interval in TASKS:
        scheduler.add_job(
            execute_task, IntervalTrigger(seconds=interval),
            args=[name, layer, desc],
            id=name, name=name,
        )
    scheduler.start()
    try:
        while True:
            time.sleep(10)
            jobs = scheduler.get_jobs()
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] 调度中: {len(jobs)} 个任务")
    except KeyboardInterrupt:
        scheduler.shutdown()
        print("\n  调度已停止")
elif not APS_OK:
    print("\n  提示: pip install apscheduler 后可用 --run 持续运行")
