#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DolphinScheduler / Airflow 风格 DAG 调度器
18 任务完整 DAG 图 — 拓扑排序 + 并行执行模拟
纯 Python 标准库，零外部依赖
"""

import time
import random
import queue
import threading
from collections import deque
from typing import Dict, List, Set, Tuple


# ============================================================================
# 1. DAG 定义
# ============================================================================

DAG_TASKS: Dict[str, dict] = {
    "start":                 {"deps": [],                    "layer": 0},
    "datax_sync_road":       {"deps": ["start"],             "layer": 1},
    "datax_sync_device":     {"deps": ["start"],             "layer": 1},
    "datax_sync_area":       {"deps": ["start"],             "layer": 1},
    "flume_collect_logs":    {"deps": ["datax_sync_road",
                                       "datax_sync_device",
                                       "datax_sync_area"],   "layer": 2},
    "kafka_produce":         {"deps": ["flume_collect_logs"],"layer": 3},
    "flink_vehicle_count":   {"deps": ["kafka_produce"],     "layer": 4},
    "flink_congestion_detect":{"deps": ["kafka_produce"],    "layer": 4},
    "flink_device_cep":      {"deps": ["kafka_produce"],     "layer": 4},
    "ods_vehicle_pass":      {"deps": ["flink_vehicle_count"],"layer": 5},
    "ods_traffic_status":    {"deps": ["flink_congestion_detect"],"layer": 5},
    "ods_device_status":     {"deps": ["flink_device_cep"],  "layer": 5},
    "ods_alarm_log":         {"deps": ["flink_device_cep"],  "layer": 5},
    "dwd_vehicle_pass":      {"deps": ["ods_vehicle_pass"],  "layer": 6},
    "dwd_traffic_status":    {"deps": ["ods_traffic_status"],"layer": 6},
    "dwd_device_status":     {"deps": ["ods_device_status"], "layer": 6},
    "dwd_alarm_log":         {"deps": ["ods_alarm_log"],     "layer": 6},
    "dws_road_hour_flow":    {"deps": ["dwd_vehicle_pass"],  "layer": 7},
    "dws_area_jam_hour":     {"deps": ["dwd_traffic_status"],"layer": 7},
    "dws_device_health_day": {"deps": ["dwd_device_status"], "layer": 7},
    "dws_alarm_day":         {"deps": ["dwd_alarm_log"],     "layer": 7},
    "ads_traffic_operation": {"deps": ["dws_road_hour_flow",
                                       "dws_area_jam_hour"], "layer": 8},
    "ads_top_jam_roads":     {"deps": ["dws_area_jam_hour"], "layer": 8},
    "ads_device_health_score":{"deps": ["dws_device_health_day"],"layer": 8},
    "ads_device_mtbf_mttr":  {"deps": ["dws_device_health_day"],"layer": 8},
    "ads_device_fault_top":  {"deps": ["dws_alarm_day"],     "layer": 8},
    "data_quality_check":    {"deps": ["ads_traffic_operation",
                                       "ads_top_jam_roads",
                                       "ads_device_health_score",
                                       "ads_device_mtbf_mttr",
                                       "ads_device_fault_top"],"layer": 9},
    "end":                   {"deps": ["data_quality_check"],"layer": 10},
}


# ============================================================================
# 2. 拓扑排序
# ============================================================================

def topological_sort(tasks: Dict[str, dict]) -> List[List[str]]:
    """返回按层级分组的拓扑排序结果：每层为一组可并行执行的任务"""
    in_degree = {name: len(info["deps"]) for name, info in tasks.items()}
    adj = {name: [] for name in tasks}
    for name, info in tasks.items():
        for dep in info["deps"]:
            adj[dep].append(name)

    # BFS 分层
    layer_groups: List[List[str]] = []
    current_layer = [name for name, deg in in_degree.items() if deg == 0]

    while current_layer:
        layer_groups.append(sorted(current_layer, key=lambda n: tasks[n]["layer"]))
        next_layer = []
        for name in current_layer:
            for child in adj[name]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    next_layer.append(child)
        current_layer = next_layer

    return layer_groups


# ============================================================================
# 3. 任务执行器
# ============================================================================

def execute_task(name: str, duration: float) -> Tuple[str, str, float]:
    """执行单个任务，返回 (name, status, duration)"""
    time.sleep(duration)
    # 模拟执行（全部成功，0.5% 概率失败以展示容错）
    status = "FAILURE" if random.random() < 0.005 else "SUCCESS"
    return name, status, duration


def execute_layer_parallel(layer: List[str]) -> List[Tuple[str, str, float]]:
    """并行执行一层中的所有任务"""
    results: List[Tuple[str, str, float]] = []
    threads: List[threading.Thread] = []
    lock = threading.Lock()

    def worker(name: str, dur: float):
        result = execute_task(name, dur)
        with lock:
            results.append(result)

    for name in layer:
        duration = random.uniform(1.0, 3.0)
        t = threading.Thread(target=worker, args=(name, duration), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    return results


# ============================================================================
# 4. 关键路径计算
# ============================================================================

def compute_critical_path() -> List[str]:
    """计算 DAG 的关键路径（按层级的最长链）"""
    # 每层取一个代表性任务形成路径
    path = [
        "start",
        "datax_sync_road",
        "flume_collect_logs",
        "kafka_produce",
        "flink_vehicle_count",
        "ods_vehicle_pass",
        "dwd_vehicle_pass",
        "dws_road_hour_flow",
        "ads_traffic_operation",
        "data_quality_check",
        "end",
    ]
    return path


# ============================================================================
# 5. 打印 DAG 执行图
# ============================================================================

BOX_WIDTH = 28

def print_dag_diagram(layer_groups: List[List[str]], results_map: Dict[str, Tuple[str, float]]):
    """打印精美的 DAG 执行图"""
    print()
    print("=" * 100)
    print("  🚦  DAG 执行图 — 城市交通大数据平台 ETL 流水线")
    print("=" * 100)
    print()

    colors = {
        0: "\033[37m",   # start
        1: "\033[34m",   # datax
        2: "\033[36m",   # flume
        3: "\033[35m",   # kafka
        4: "\033[33m",   # flink
        5: "\033[94m",   # ods
        6: "\033[92m",   # dwd
        7: "\033[93m",   # dws
        8: "\033[91m",   # ads
        9: "\033[95m",   # quality
        10: "\033[37m",  # end
    }
    RESET = "\033[0m"

    for idx, layer in enumerate(layer_groups):
        layer_num = DAG_TASKS[layer[0]]["layer"]
        color = colors.get(layer_num, "\033[37m")

        # 层标题
        layer_names = {
            0: "START",
            1: "DataX 数据同步",
            2: "Flume 日志采集",
            3: "Kafka 消息队列",
            4: "Flink 流计算",
            5: "ODS 贴源层",
            6: "DWD 明细层",
            7: "DWS 汇总层",
            8: "ADS 应用层",
            9: "数据质量检查",
            10: "END",
        }
        layer_label = layer_names.get(layer_num, f"Layer {layer_num}")
        print(f"  {color}▌ Layer {layer_num}: {layer_label}{RESET}")

        # 并行任务盒子
        boxes = []
        for name in layer:
            if name in results_map:
                status, dur = results_map[name]
                icon = "[OK]" if status == "SUCCESS" else "[ERR]"
                box = f"{icon} {name:<{BOX_WIDTH-4}} {dur:.1f}s"
            else:
                box = f"[TIMER] {name:<{BOX_WIDTH-4}} ---"
            boxes.append(box)

        # 行式并排显示
        max_per_row = 3
        for i in range(0, len(boxes), max_per_row):
            row_boxes = boxes[i:i + max_per_row]
            line = "     " + "  │  ".join(row_boxes)
            print(f"  {color}{line}{RESET}")

        # 层间箭头
        if idx < len(layer_groups) - 1:
            next_layer = layer_groups[idx + 1]
            arrow_count = max(len(layer), len(next_layer))
            arrows = "     " + "  │  ".join(["↓" * 3 for _ in range(min(arrow_count, max_per_row))])
            print(f"  {color}{arrows}{RESET}")
        print()

    print("-" * 100)


# ============================================================================
# 6. 统计摘要
# ============================================================================

def print_summary(results_map: Dict[str, Tuple[str, float]], total_time: float, is_backfill: bool = False):
    """打印最终统计摘要"""
    success_count = sum(1 for s, _ in results_map.values() if s == "SUCCESS")
    fail_count = sum(1 for s, _ in results_map.values() if s == "FAILURE")
    total = len(results_map)

    critical_path = compute_critical_path()
    cp_names = ["datax", "kafka", "flink", "ods", "dwd", "dws", "ads", "quality"]
    cp_str = "→".join(cp_names)

    mode_label = "[回刷模式] " if is_backfill else ""

    print("=" * 100)
    print(f"  [CHART] DAG 执行完成{'(回刷 7 天)' if is_backfill else ''}: {success_count}/{total} 成功, "
          f"关键路径: {cp_str} ({total_time:.1f}秒)")
    if fail_count > 0:
        failed = [n for n, (s, _) in results_map.items() if s == "FAILURE"]
        print(f"  [WARN]  失败任务: {', '.join(failed)}")
    print("=" * 100)
    print()


# ============================================================================
# 7. DAG 运行主函数
# ============================================================================

def run_dag(is_backfill: bool = False, backfill_days: int = 7):
    """
    执行完整 DAG。
    is_backfill=True 时模拟回刷 backfill_days 天的数据
    """
    mode = "回刷" if is_backfill else "正常运行"
    print(f"\n[TOOL] 模式: {mode}  |  任务数: {len(DAG_TASKS)}  |  时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    layer_groups = topological_sort(DAG_TASKS)
    results_map: Dict[str, Tuple[str, float]] = {}
    dag_start = time.time()

    if is_backfill:
        print(f"  ⏮  开始回刷 {backfill_days} 天历史数据...\n")
        for day_offset in range(backfill_days - 1, -1, -1):
            day_label = f"T-{day_offset + 1}"
            print(f"  [DAY] 回刷日期: {day_label}")
            for layer_idx, layer in enumerate(layer_groups):
                # 回刷时不详细打印每一层
                layer_results = execute_layer_parallel(layer)
                for name, status, dur in layer_results:
                    results_map[name] = (status, dur)
            print(f"  [OK] {day_label} 完成")
        print()

    else:
        # 逐层打印执行
        for layer_idx, layer in enumerate(layer_groups):
            layer_label = f"Layer {DAG_TASKS[layer[0]]['layer']}"
            print(f"  ⚡ 执行 {layer_label}: {', '.join(layer)}")

            layer_results = execute_layer_parallel(layer)
            for name, status, dur in layer_results:
                results_map[name] = (status, dur)
                icon = "[OK]" if status == "SUCCESS" else "[ERR]"
                print(f"     {icon} {name} 完成 ({dur:.1f}s)")

            if layer_idx < len(layer_groups) - 1:
                print()

        # 打印 DAG 图
        print_dag_diagram(layer_groups, results_map)

    dag_end = time.time()
    total_time = dag_end - dag_start
    print_summary(results_map, total_time, is_backfill)

    return results_map, total_time


# ============================================================================
# 8. 回刷模式
# ============================================================================

def run_backfill(days: int = 7):
    """回刷指定天数数据"""
    print("\n" + "█" * 100)
    print("█  ⏮  回刷模式 — 批量重跑历史数据")
    print("█" * 100)
    run_dag(is_backfill=True, backfill_days=days)


# ============================================================================
# 9. main
# ============================================================================

def main():
    print()
    print("╔" + "═" * 98 + "╗")
    print("║" + "  [DAG] DolphinScheduler / [AIRFLOW]  Airflow 风格 DAG 调度器 — 城市交通大数据平台".center(90) + "║")
    print("╚" + "═" * 98 + "╝")

    # 正常模式运行
    results, total_time = run_dag(is_backfill=False)

    # 回刷模式
    run_backfill(days=7)


if __name__ == "__main__":
    main()
