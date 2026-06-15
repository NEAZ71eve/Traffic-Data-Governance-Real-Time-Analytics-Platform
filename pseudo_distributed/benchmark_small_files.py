﻿#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HDFS 小文件治理演示
模拟 1000 个小文件 → 3 种合并策略 → 对比治理前后效果
纯 Python 标准库，零外部依赖
"""

import time
import math
import random
from dataclasses import dataclass, field
from typing import List, Dict, Tuple


# ============================================================================
# 1. 模拟 HDFS 小文件
# ============================================================================

HDFS_BLOCK_SIZE = 128 * 1024 * 1024         # 128 MB (HDFS 默认块大小)
METADATA_PER_FILE = 10 * 1024               # 每个文件约 10 KB NameNode 元数据
SMALL_FILE_SIZE = 1 * 1024                  # 每个小文件 1 KB
TOTAL_FILES = 1000                          # 1000 个小文件
TARGET_COMPACTION_SIZE = HDFS_BLOCK_SIZE    # 目标合并块大小 128 MB


@dataclass
class SmallFile:
    """模拟 HDFS 小文件"""
    path: str
    size: int          # bytes
    partition_time: str  # 分区时间 "2026-06-09 14:00"


@dataclass
class CompactedFile:
    """合并后的大文件"""
    path: str
    size: int
    source_files: int  # 包含多少个原始小文件


# ============================================================================
# 2. 生成模拟数据
# ============================================================================

def generate_small_files(total: int = TOTAL_FILES, file_size: int = SMALL_FILE_SIZE) -> List[SmallFile]:
    """生成模拟 HDFS 小文件列表，按小时分区"""
    files = []
    base_date = "2026-06-09"
    for i in range(total):
        hour = i % 24
        partition_time = f"{base_date} {hour:02d}:00"
        path = f"/user/hive/warehouse/traffic.db/vehicle_pass/dt={base_date}/hour={hour:02d}/part-{i:05d}.parquet"
        files.append(SmallFile(path=path, size=file_size, partition_time=partition_time))
    return files


# ============================================================================
# 3. 测量治理前指标
# ============================================================================

def measure_before(files: List[SmallFile]) -> Dict[str, any]:
    """测量小文件治理前的各项指标"""
    total_size = sum(f.size for f in files)
    metadata_size = len(files) * METADATA_PER_FILE
    dir_listing_time = len(files) * 0.001  # 模拟每个文件 1ms 的列表开销 (秒)

    return {
        "file_count": len(files),
        "total_size_mb": total_size / (1024 * 1024),
        "metadata_kb": metadata_size / 1024,
        "dir_listing_sec": dir_listing_time,
    }


# ============================================================================
# 4. 策略 A: CONCATENATE — 合并到目标 128MB 块
# ============================================================================

def strategy_concatenate(files: List[SmallFile]) -> List[CompactedFile]:
    """
    策略 A: CONCATENATE
    将小文件按顺序合并，每达到 128 MB 生成一个新文件
    """
    compacted: List[CompactedFile] = []
    current_size = 0
    current_sources = 0
    block_idx = 0

    for f in files:
        if current_size + f.size > TARGET_COMPACTION_SIZE and current_sources > 0:
            path = f"/user/hive/warehouse/traffic.db/vehicle_pass/compacted/block-{block_idx:04d}.parquet"
            compacted.append(CompactedFile(path=path, size=current_size, source_files=current_sources))
            block_idx += 1
            current_size = 0
            current_sources = 0
        current_size += f.size
        current_sources += 1

    # 处理末尾剩余
    if current_sources > 0:
        path = f"/user/hive/warehouse/traffic.db/vehicle_pass/compacted/block-{block_idx:04d}.parquet"
        compacted.append(CompactedFile(path=path, size=current_size, source_files=current_sources))

    return compacted


# ============================================================================
# 5. 策略 B: HAR (Hadoop Archive) — 按天归档
# ============================================================================

def strategy_har(files: List[SmallFile]) -> List[CompactedFile]:
    """
    策略 B: HAR (Hadoop Archive)
    把同一天的小文件打包成 .har 归档文件
    """
    # 按天分组
    groups: Dict[str, List[SmallFile]] = {}
    for f in files:
        day = f.partition_time.split(" ")[0]  # "2026-06-09"
        if day not in groups:
            groups[day] = []
        groups[day].append(f)

    compacted: List[CompactedFile] = []
    for day, day_files in groups.items():
        total_size = sum(f.size for f in day_files)
        path = f"/user/hive/warehouse/traffic.db/vehicle_pass/archive/dt={day}.har"
        compacted.append(CompactedFile(path=path, size=total_size, source_files=len(day_files)))

    return compacted


# ============================================================================
# 6. 策略 C: Delta Compaction — 按小时合并
# ============================================================================

def strategy_delta_compaction(files: List[SmallFile]) -> List[CompactedFile]:
    """
    策略 C: Delta Compaction
    按 3 小时窗口合并，将相邻小时分区压缩为一个文件（减少文件数至 ~8/day）
    """
    WINDOW_HOURS = 3  # 3 小时合并窗口
    groups: Dict[str, List[SmallFile]] = {}
    for f in files:
        # 提取小时数，归入 3 小时窗口
        hour = int(f.partition_time.split(" ")[1].split(":")[0])
        window_start = (hour // WINDOW_HOURS) * WINDOW_HOURS
        date_part = f.partition_time.split(" ")[0]
        key = f"{date_part} H{window_start:02d}-{(window_start + WINDOW_HOURS - 1):02d}"
        if key not in groups:
            groups[key] = []
        groups[key].append(f)

    compacted: List[CompactedFile] = []
    for key, pt_files in groups.items():
        total_size = sum(f.size for f in pt_files)
        path = f"/user/hive/warehouse/traffic.db/vehicle_pass/compacted/{key.replace(' ', '_')}.parquet"
        compacted.append(CompactedFile(path=path, size=total_size, source_files=len(pt_files)))

    return compacted


# ============================================================================
# 7. 测量治理后指标
# ============================================================================

def measure_after(compacted: List[CompactedFile]) -> Dict[str, any]:
    """测量治理后的各项指标"""
    total_size = sum(c.size for c in compacted)
    metadata_size = len(compacted) * METADATA_PER_FILE
    dir_listing_time = len(compacted) * 0.005  # 大文件列表稍慢但数量少得多

    return {
        "file_count": len(compacted),
        "total_size_mb": total_size / (1024 * 1024),
        "metadata_kb": metadata_size / 1024,
        "dir_listing_sec": dir_listing_time,
    }


# ============================================================================
# 8. 打印对比报告
# ============================================================================

def print_strategy_report(
    name: str,
    before: Dict[str, any],
    after: Dict[str, any],
):
    """打印单个策略的 Before/After 对比报告"""
    reduction_pct = (1 - after["file_count"] / before["file_count"]) * 100

    print()
    print("  " + "─" * 80)
    print(f"  [STRATEGY] 策略: {name}")
    print("  " + "─" * 80)
    print(f"  {'指标':<24} {'治理前':>18} {'治理后':>18} {'改善':>18}")
    print("  " + "-" * 80)

    print(f"  {'文件数':<24} {before['file_count']:>18,} {after['file_count']:>18,} "
          f"{-reduction_pct:>17.1f}%")

    print(f"  {'总大小 (MB)':<24} {before['total_size_mb']:>18.2f} {after['total_size_mb']:>18.2f} "
          f"{'—':>18}")

    meta_before = before["metadata_kb"]
    meta_after = after["metadata_kb"]
    meta_reduction = (1 - meta_after / meta_before) * 100 if meta_before > 0 else 0
    print(f"  {'元数据 (KB)':<24} {meta_before:>18.1f} {meta_after:>18.1f} "
          f"{-meta_reduction:>17.1f}%")

    listing_before = before["dir_listing_sec"]
    listing_after = after["dir_listing_sec"]
    listing_improve = (listing_before - listing_after) / listing_before * 100 if listing_before > 0 else 0
    print(f"  {'目录列表时间 (s)':<24} {listing_before:>18.2f} {listing_after:>18.2f} "
          f"{-listing_improve:>17.1f}%")


# ============================================================================
# 9. 主函数
# ============================================================================

def main():
    print()
    print("╔" + "═" * 98 + "╗")
    print("║" + "  [FILE] HDFS 小文件治理 — 基准测试与策略对比".center(90) + "║")
    print("╚" + "═" * 98 + "╝")

    # 1. 生成模拟文件
    print("\n  [GEN] 正在生成模拟 HDFS 小文件...")
    files = generate_small_files(total=TOTAL_FILES, file_size=SMALL_FILE_SIZE)
    print(f"  [OK] 已生成 {len(files)} 个模拟文件 (每个 {SMALL_FILE_SIZE / 1024:.0f} KB)")

    # 2. 测量前
    print("\n  [MEASURE] 测量治理前指标...")
    before = measure_before(files)

    # 3. 运行 3 种策略
    strategies = [
        ("A. CONCATENATE (合并到 128MB 块)", strategy_concatenate),
        ("B. HAR (Hadoop Archive 按天归档)", strategy_har),
        ("C. Delta Compaction (3小时窗口合并)", strategy_delta_compaction),
    ]

    print("\n" + "=" * 100)
    print("  [CHART] 策略对比报告")
    print("=" * 100)

    for name, strategy_fn in strategies:
        after = measure_after(strategy_fn(files))
        print_strategy_report(name, before, after)

    # 4. 最优策略摘要 (Delta Compaction 作为推荐)
    best = measure_after(strategy_delta_compaction(files))
    reduction_pct = (1 - best["file_count"] / before["file_count"]) * 100
    meta_reduction_pct = (1 - best["metadata_kb"] / before["metadata_kb"]) * 100

    print()
    print("=" * 100)
    print("  [WINNER] 推荐策略: Delta Compaction (3小时窗口合并)")
    print("=" * 100)
    print(f"  Before: {before['file_count']} files "
          f"({before['total_size_mb']:.2f} MB total, "
          f"{before['metadata_kb']:.1f} KB metadata), "
          f"After: {best['file_count']} files "
          f"({best['total_size_mb']:.2f} MB total, "
          f"{best['metadata_kb']:.1f} KB metadata), "
          f"Reduction: {reduction_pct:.1f}% file count")
    print("=" * 100)
    print()


if __name__ == "__main__":
    main()
