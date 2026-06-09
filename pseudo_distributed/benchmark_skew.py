#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据倾斜检测与两阶段聚合缓解演示
- 生成倾斜数据集: 80%数据属于5个"热点"路段, 20%属于45个"冷门"路段
- 计算Gini系数检测倾斜度
- 对比传统单次聚合 vs 两阶段聚合的Reducer负载差异
零外部依赖，纯Python。
"""

import math
import random
import statistics
import sys


# ==============================================================================
# 工具函数
# ==============================================================================

def gini_coefficient(values):
    """
    计算 Gini 系数 (0=完全均匀, 1=完全集中)。
    公式: G = 1 - 2 * sum(i=1..n)((n - i + 0.5) / n * (sorted_values[i] / total))
    简化: 使用均值差法 G = sum_i sum_j |x_i - x_j| / (2 * n^2 * mean)
    """
    n = len(values)
    if n == 0 or sum(values) == 0:
        return 0.0
    sorted_vals = sorted(values)
    total = sum(sorted_vals)
    # Lorenz curve 法
    cumulative = 0.0
    lorenz_area = 0.0
    for i, v in enumerate(sorted_vals):
        cumulative += v
        # 梯形面积近似
        prev_cum = cumulative - v
        lorenz_area += (prev_cum + cumulative) / (2 * total)
    lorenz_area /= n
    gini = 1 - 2 * lorenz_area
    # Gini 系数的理论范围是 [0, 1-1/n]，这里归一化
    gini_normalized = gini * n / (n - 1) if n > 1 else 0.0
    return round(gini_normalized, 4)


def concentration_top_n(counters, top_n=5):
    """计算前N个key的数据集中度百分比"""
    sorted_items = sorted(counters.items(), key=lambda x: x[1], reverse=True)
    total = sum(v for _, v in sorted_items)
    top_total = sum(v for _, v in sorted_items[:top_n])
    return sorted_items[:top_n], round(top_total / total * 100, 2) if total else 0


def fmt_percent(pct):
    """格式化百分比"""
    return f"{pct:.1f}%"


# ==============================================================================
# 数据生成
# ==============================================================================

def generate_skewed_dataset(num_roads=50, total_records=100000,
                             hot_roads=5, hot_ratio=0.80):
    """
    生成倾斜数据集。
    - hot_roads个热点路段占 hot_ratio 比例的数据
    - 其余 (num_roads - hot_roads) 个冷门路段占剩余数据
    """
    road_ids = [f"RD-{i:04d}" for i in range(1, num_roads + 1)]
    hot_road_ids = road_ids[:hot_roads]
    cold_road_ids = road_ids[hot_roads:]

    hot_total = int(total_records * hot_ratio)
    cold_total = total_records - hot_total

    records = []

    # 生成热点数据 -> 每个热点路段按指数递减分配
    hot_weights = [1.0 / (i + 1) for i in range(hot_roads)]
    hot_sum = sum(hot_weights)
    hot_dist = [int(hot_total * w / hot_sum) for w in hot_weights]
    # 调整使总和精确
    diff = hot_total - sum(hot_dist)
    for i in range(abs(diff)):
        hot_dist[i % hot_roads] += 1 if diff > 0 else -1

    for road, count in zip(hot_road_ids, hot_dist):
        for _ in range(count):
            records.append({
                "road_id": road,
                "is_hot": True,
                "pass_count": random.randint(50, 200),
            })

    # 生成冷门数据 -> 均匀分布
    cold_per_road = cold_total // len(cold_road_ids)
    cold_remainder = cold_total - cold_per_road * len(cold_road_ids)
    for idx, road in enumerate(cold_road_ids):
        count = cold_per_road + (1 if idx < cold_remainder else 0)
        for _ in range(count):
            records.append({
                "road_id": road,
                "is_hot": False,
                "pass_count": random.randint(5, 20),
            })

    random.shuffle(records)
    return records, road_ids, hot_road_ids


# ==============================================================================
# 单次聚合 (传统方式)
# ==============================================================================

def traditional_aggregation(records):
    """
    传统单次聚合: 直接按 road_id 做 GROUP BY + SUM(pass_count)。
    返回每个 road_id 的 total_pass_count 及各 reducer 的负载分布。
    """
    reducer_load = {}  # road_id -> total
    for rec in records:
        rid = rec["road_id"]
        reducer_load[rid] = reducer_load.get(rid, 0) + rec["pass_count"]
    return reducer_load


# ==============================================================================
# 两阶段聚合
# ==============================================================================

def two_phase_aggregation(records, num_local_slots=10):
    """
    两阶段聚合:
    Phase 1 (Map端预聚合): 将数据分片，在每个分片内按 road_id 局部聚合
    Phase 2 (Reduce端全局聚合): 对 Phase 1 的结果再做全局聚合

    num_local_slots 模拟 Combine/Map端预聚合的分片数。
    """
    # 随机分片模拟行分发
    slots = [[] for _ in range(num_local_slots)]
    for rec in records:
        slot = random.randint(0, num_local_slots - 1)
        slots[slot].append(rec)

    # Phase 1: Map端局部聚合
    local_agg = []
    for slot_data in slots:
        slot_result = {}
        for rec in slot_data:
            rid = rec["road_id"]
            slot_result[rid] = slot_result.get(rid, 0) + rec["pass_count"]
        for rid, partial_sum in slot_result.items():
            local_agg.append({"road_id": rid, "partial_sum": partial_sum})

    # Phase 2: Reduce端全局聚合
    global_agg = {}
    for item in local_agg:
        rid = item["road_id"]
        global_agg[rid] = global_agg.get(rid, 0) + item["partial_sum"]

    # 同时输出 Phase 2 的每条中间数据的key分布（用于计算reducer端的实际负载）
    # 实际上两阶段后Reducer收到的key数量大大减少
    return global_agg, local_agg


# ==============================================================================
# 负载均衡分析
# ==============================================================================

def analyze_load_balance(result_map, label="Reducer"):
    """分析Reducer负载均衡情况，返回指标"""
    loads = list(result_map.values())
    if not loads:
        return {}
    max_load = max(loads)
    min_load = min(loads)
    avg_load = statistics.mean(loads)
    stdev_load = statistics.stdev(loads) if len(loads) > 1 else 0

    # 负载偏差系数 (CV = std/mean)
    cv = stdev_load / avg_load if avg_load else 0

    # 最大负载 / 平均负载 (反映skew严重程度)
    skew_ratio = max_load / avg_load if avg_load else 0

    return {
        "max": max_load,
        "min": min_load,
        "avg": avg_load,
        "stdev": stdev_load,
        "cv": round(cv, 4),
        "skew_ratio": round(skew_ratio, 2),
        "total_keys": len(loads),
        "total_sum": sum(loads),
    }


# ==============================================================================
# 主逻辑
# ==============================================================================

def main():
    print("=" * 72)
    print("        数据倾斜检测与两阶段聚合缓解演示")
    print("=" * 72)

    random.seed(42)

    NUM_ROADS = 50
    HOT_ROADS = 5
    TOTAL_RECORDS = 100000
    HOT_RATIO = 0.80

    # ------------------------------------------------------------------
    # Step 1: 生成倾斜数据集
    # ------------------------------------------------------------------
    print(f"\n[Step 1] 生成倾斜数据集...")
    print(f"  总路段数: {NUM_ROADS}, 热点路段: {HOT_ROADS}, 热数据占比: {HOT_RATIO*100:.0f}%")

    records, road_ids, hot_road_ids = generate_skewed_dataset(
        num_roads=NUM_ROADS,
        total_records=TOTAL_RECORDS,
        hot_roads=HOT_ROADS,
        hot_ratio=HOT_RATIO
    )
    print(f"  总记录数: {len(records):,}")

    # ------------------------------------------------------------------
    # Step 2: 检测倾斜
    # ------------------------------------------------------------------
    print(f"\n[Step 2] 数据倾斜检测...")

    # 统计每个 road_id 的数据量
    road_counts = {}
    for rec in records:
        rid = rec["road_id"]
        road_counts[rid] = road_counts.get(rid, 0) + 1

    # Gini 系数
    gini = gini_coefficient(list(road_counts.values()))
    print(f"  Gini 系数: {gini}  (0=完全均匀, 1=完全集中)")

    # Top-N 集中度
    top_items, top_pct = concentration_top_n(road_counts, top_n=HOT_ROADS)
    print(f"\n  Top-{HOT_ROADS} 数据集中度: {top_pct}%")
    print(f"  {'排名':<6} {'路段ID':<12} {'记录数':>10} {'占比':>10}")
    print(f"  {'-'*6} {'-'*12} {'-'*10} {'-'*10}")
    total_rec = sum(road_counts.values())
    for i, (rid, cnt) in enumerate(top_items):
        pct = cnt / total_rec * 100
        bar = "#" * int(pct / 2)
        print(f"  {i+1:<6} {rid:<12} {cnt:>10,} {pct:>9.1f}% {bar}")

    # 热点 vs 冷门对比
    hot_total = sum(road_counts.get(r, 0) for r in hot_road_ids)
    cold_total = total_rec - hot_total
    cold_road_count = NUM_ROADS - HOT_ROADS
    print(f"\n  热点 {HOT_ROADS} 路段: {hot_total:,} 条 ({hot_total/total_rec*100:.1f}%)")
    print(f"  冷门 {cold_road_count} 路段: {cold_total:,} 条 ({cold_total/total_rec*100:.1f}%)")

    # ------------------------------------------------------------------
    # Step 3: 传统单次聚合
    # ------------------------------------------------------------------
    print(f"\n[Step 3] 传统单次聚合 (直接 GROUP BY)...")
    trad_result = traditional_aggregation(records)
    trad_stats = analyze_load_balance(trad_result, "传统Reducer")
    print(f"  Key数量: {trad_stats['total_keys']}")
    print(f"  总聚合值: {trad_stats['total_sum']:,}")
    print(f"  最大负载: {trad_stats['max']:,}")
    print(f"  最小负载: {trad_stats['min']:,}")
    print(f"  平均负载: {trad_stats['avg']:,.1f}")
    print(f"  CV (变异系数): {trad_stats['cv']}")
    print(f"  Max/Avg 倾斜比: {trad_stats['skew_ratio']}x")

    # ------------------------------------------------------------------
    # Step 4: 两阶段聚合
    # ------------------------------------------------------------------
    print(f"\n[Step 4] 两阶段聚合 (Map端预聚合 + Reduce全局聚合)...")
    two_result, local_agg = two_phase_aggregation(records, num_local_slots=10)

    # Phase 1 负载
    phase1_loads = {}
    for item in local_agg:
        rid = item["road_id"]
        phase1_loads[rid] = phase1_loads.get(rid, 0) + 1
    phase1_stats = analyze_load_balance(phase1_loads, "Phase1")
    print(f"  Phase 1 (Map端预聚合):")
    print(f"    中间Key数量: {len(local_agg):,}")
    print(f"    去重Key数: {phase1_stats['total_keys']}")

    # Phase 2 负载 (Reducer真正收到的是去重后的数据)
    two_stats = analyze_load_balance(two_result, "Phase2-Reducer")
    print(f"  Phase 2 (Reduce全局聚合):")
    print(f"    Key数量: {two_stats['total_keys']}")
    print(f"    Max/Avg 倾斜比: {two_stats['skew_ratio']}x")

    # ------------------------------------------------------------------
    # Step 5: 对比
    # ------------------------------------------------------------------
    print(f"\n[Step 5] 对比分析...")

    # 传统方式: 每条记录都作为Reducer的输入，Reducer收到的记录数 = 原始记录数
    traditional_reducer_input = len(records)
    # 两阶段: Reducer收到的中间记录数 = len(local_agg)
    two_phase_reducer_input = len(local_agg)

    reduction_ratio = (1 - two_phase_reducer_input / traditional_reducer_input) * 100

    # Reducer 负载的CV对比
    # 传统方式下，每个Reducer处理对应road_id的所有记录
    # 两阶段下，Reducer收到的是预聚合后的数据
    print(f"\n  {'指标':<35} {'传统聚合':>15} {'两阶段聚合':>15} {'改善':>15}")
    print(f"  {'-'*35} {'-'*15} {'-'*15} {'-'*15}")
    print(f"  {'Reducer输入记录数':<35} {traditional_reducer_input:>15,} {two_phase_reducer_input:>15,} {reduction_ratio:>14.1f}%")
    print(f"  {'Reducer端Key数':<35} {trad_stats['total_keys']:>15} {two_stats['total_keys']:>15} {'-':>15}")
    print(f"  {'Max/Avg 倾斜比':<35} {trad_stats['skew_ratio']:>14.2f}x {two_stats['skew_ratio']:>14.2f}x {'-':>15}")

    # 虽然两阶段不能改变Key本身的分布，但减少了Reducer需要处理的数据量
    # 传统方式下，Reducer处理的是原始记录
    # 两阶段下，Reducer处理的是预聚合后的轻量中间结果

    print(f"\n")
    print("=" * 72)
    print(f"  结论:")
    print(f"  - 数据倾斜检测: Gini系数 = {gini}")
    print(f"  - Top {HOT_ROADS} 路段集中度: {top_pct}%")
    print(f"  - 传统聚合: 所有 {traditional_reducer_input:,} 条原始记录发送到Reducer")
    print(f"  - 两阶段聚合: 仅 {two_phase_reducer_input:,} 条中间结果发送到Reducer")
    print(f"  - Shuffle数据量减少: {reduction_ratio:.1f}%")
    print(f"  - 两阶段聚合通过Map端预聚合减少了Shuffle和Reducer端的计算压力")
    print("=" * 72)

    # 构造要求的输出格式
    print(f"\n  >>> Skew detected: Gini={gini}, "
          f"Top{HOT_ROADS} roads={top_pct}% of data, "
          f"Two-phase aggregation reduced reducer input "
          f"from {traditional_reducer_input:,} to {two_phase_reducer_input:,} "
          f"({reduction_ratio:.1f}% reduction)")
    print()


if __name__ == "__main__":
    main()
