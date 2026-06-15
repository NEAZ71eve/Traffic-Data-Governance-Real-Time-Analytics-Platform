#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hive/Spark存储格式与Join策略性能基准测试（纯Python模拟）
- TEXTFILE vs ORC 序列化模拟
- Snappy压缩 vs 不压缩对比
- MapJoin vs ReduceJoin 对比
零外部依赖，纯Python。
"""

import random
import struct
import time
import sys


# ==============================================================================
# 工具函数
# ==============================================================================

def time_it(func, *args, **kwargs):
    """计时包装器，返回 (duration_sec, result)"""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return elapsed, result


def fmt_duration(sec):
    """格式化耗时"""
    if sec < 0.001:
        return f"{sec*1000000:.1f} us"
    elif sec < 1:
        return f"{sec*1000:.1f} ms"
    else:
        return f"{sec:.3f} s"


def fmt_bytes(n):
    """格式化字节数"""
    if n < 1024:
        return f"{n} B"
    elif n < 1024 * 1024:
        return f"{n/1024:.1f} KB"
    else:
        return f"{n/(1024*1024):.1f} MB"


# ==============================================================================
# 模拟行数据
# ==============================================================================

COLUMNS_SCHEMA = [
    ("road_id",     "string"),
    ("plate",       "string"),
    ("pass_time",   "string"),
    ("speed_kmh",   "int"),
    ("vehicle_type","string"),
    ("lane_no",     "int"),
    ("avg_speed",   "float"),
    ("traffic_flow","int"),
    ("jam_level",   "int"),
    ("device_id",   "string"),
    ("cpu_usage",   "float"),
    ("mem_usage",   "float"),
    ("temp",        "float"),
]

ROAD_NAMES = [
    "中山路", "人民路", "建设路", "解放路", "和平路",
    "长安街", "南京路", "北京路", "天府大道", "滨海大道",
]

VEHICLE_TYPES = ["car", "bus", "truck", "motorcycle"]

PLATE_PREFIXES = [
    "京", "津", "沪", "渝", "冀", "豫", "云", "辽", "黑", "湘",
    "皖", "鲁", "新", "苏", "浙", "赣", "鄂", "桂", "甘", "晋",
]


def generate_row():
    """生成一条模拟行数据 (dict)"""
    return {
        "road_id":      f"RD-{random.randint(10000,99999):05d}",
        "plate":        f"{random.choice(PLATE_PREFIXES)}{random.choice('ABCDEFGH')}{random.randint(10000,99999)}",
        "pass_time":    f"2025-06-0{random.randint(1,7)} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
        "speed_kmh":    random.randint(20, 120),
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "lane_no":      random.randint(1, 4),
        "avg_speed":    round(random.uniform(5, 100), 1),
        "traffic_flow": random.randint(50, 1500),
        "jam_level":    random.randint(1, 5),
        "device_id":    f"DEV-{random.randint(1000,9999):04d}",
        "cpu_usage":    round(random.uniform(5, 95), 1),
        "mem_usage":    round(random.uniform(10, 95), 1),
        "temp":         round(random.uniform(25, 85), 1),
    }


# ==============================================================================
# 基准测试1: TEXTFILE vs ORC 序列化模拟
# ==============================================================================

def serialize_textfile(row):
    """TEXTFILE序列化：每条记录转为 | 分隔的文本行 + 换行"""
    values = [
        row["road_id"],
        row["plate"],
        row["pass_time"],
        str(row["speed_kmh"]),
        row["vehicle_type"],
        str(row["lane_no"]),
        str(row["avg_speed"]),
        str(row["traffic_flow"]),
        str(row["jam_level"]),
        row["device_id"],
        str(row["cpu_usage"]),
        str(row["mem_usage"]),
        str(row["temp"]),
    ]
    return ("|".join(values) + "\n").encode("utf-8")


def serialize_orc_simulated(row):
    """
    ORC列式存储模拟：
    - 字符串用长度前缀 + UTF-8
    - 整数用小端序4字节
    - 浮点数用小端序4字节(f32近似)
    意义：列式格式按列组织，这里我们模拟每条记录的紧凑二进制布局。
    """
    buf = bytearray()
    # road_id (string)
    s = row["road_id"].encode("utf-8")
    buf.extend(struct.pack("<I", len(s)))
    buf.extend(s)
    # plate (string)
    s = row["plate"].encode("utf-8")
    buf.extend(struct.pack("<I", len(s)))
    buf.extend(s)
    # pass_time (string)
    s = row["pass_time"].encode("utf-8")
    buf.extend(struct.pack("<I", len(s)))
    buf.extend(s)
    # speed_kmh (int)
    buf.extend(struct.pack("<i", row["speed_kmh"]))
    # vehicle_type (string)
    s = row["vehicle_type"].encode("utf-8")
    buf.extend(struct.pack("<I", len(s)))
    buf.extend(s)
    # lane_no (int)
    buf.extend(struct.pack("<i", row["lane_no"]))
    # avg_speed (float -> f32)
    buf.extend(struct.pack("<f", float(row["avg_speed"])))
    # traffic_flow (int)
    buf.extend(struct.pack("<i", row["traffic_flow"]))
    # jam_level (int)
    buf.extend(struct.pack("<i", row["jam_level"]))
    # device_id (string)
    s = row["device_id"].encode("utf-8")
    buf.extend(struct.pack("<I", len(s)))
    buf.extend(s)
    # cpu_usage (float)
    buf.extend(struct.pack("<f", float(row["cpu_usage"])))
    # mem_usage (float)
    buf.extend(struct.pack("<f", float(row["mem_usage"])))
    # temp (float)
    buf.extend(struct.pack("<f", float(row["temp"])))
    return bytes(buf)


def benchmark_serialization(rows, iterations=3):
    """TEXTFILE vs ORC 序列化基准测试"""
    print("\n" + "=" * 72)
    print("  基准测试 1: TEXTFILE vs ORC 序列化性能对比")
    print("=" * 72)

    text_total_time = 0.0
    text_total_bytes = 0
    orc_total_time = 0.0
    orc_total_bytes = 0

    for it in range(iterations):
        # TEXTFILE
        t, text_data = time_it(lambda: [serialize_textfile(r) for r in rows])
        text_total_time += t
        text_total_bytes += sum(len(d) for d in text_data)

        # ORC simulated
        t, orc_data = time_it(lambda: [serialize_orc_simulated(r) for r in rows])
        orc_total_time += t
        orc_total_bytes += sum(len(d) for d in orc_data)

    text_avg_time = text_total_time / iterations
    orc_avg_time = orc_total_time / iterations
    text_avg_bytes = text_total_bytes / iterations
    orc_avg_bytes = orc_total_bytes / iterations
    size_ratio = text_avg_bytes / orc_avg_bytes if orc_avg_bytes else 0
    speed_ratio = text_avg_time / orc_avg_time if orc_avg_time else 0

    print(f"  数据量: {len(rows):,} 行")
    print(f"  TEXTFILE 序列化: 平均 {fmt_duration(text_avg_time)}, 大小 {fmt_bytes(text_avg_bytes)}")
    print(f"  ORC模拟  序列化: 平均 {fmt_duration(orc_avg_time)}, 大小 {fmt_bytes(orc_avg_bytes)}")
    print(f"  ORC 存储节省: {size_ratio:.2f}x 更小")
    print(f"  ORC 速度对比: {speed_ratio:.2f}x {'更快' if speed_ratio >= 1 else '更慢'}")

    return {
        "name": "TEXTFILE vs ORC",
        "text_time": text_avg_time,
        "orc_time": orc_avg_time,
        "text_size": text_avg_bytes,
        "orc_size": orc_avg_bytes,
        "size_ratio": size_ratio,
    }


# ==============================================================================
# 基准测试2: Snappy 压缩 vs 不压缩 模拟
# ==============================================================================

def simple_rle_compress(data):
    """
    简单RLE压缩模拟（类似轻量压缩算法思路）。
    Snappy 基于 LZ77，这里用 RLE 做近似模拟，展示压缩比和速度差异。
    """
    compressed = bytearray()
    n = len(data)
    i = 0
    while i < n:
        run_len = 1
        while i + run_len < n and data[i + run_len] == data[i] and run_len < 255:
            run_len += 1
        if run_len > 3:
            # 转义标记 0xFF, 重复字节, 计数
            compressed.append(0xFF)
            compressed.append(data[i])
            compressed.append(run_len)
            i += run_len
        else:
            if data[i] == 0xFF:
                compressed.append(0xFF)
                compressed.append(0x00)
                compressed.append(1)
            else:
                compressed.append(data[i])
            i += 1
    return bytes(compressed)


def simple_rle_decompress(compressed):
    """RLE解压缩"""
    decompressed = bytearray()
    n = len(compressed)
    i = 0
    while i < n:
        b = compressed[i]
        if b == 0xFF:
            if compressed[i + 1] == 0x00 and compressed[i + 2] == 1:
                decompressed.append(0xFF)
                i += 3
            else:
                val = compressed[i + 1]
                count = compressed[i + 2]
                decompressed.extend([val] * count)
                i += 3
        else:
            decompressed.append(b)
            i += 1
    return bytes(decompressed)


def generate_chunk_data(size_kb=256):
    """生成模拟存储块数据。交通监控数据有重复模式（如相同road_id），体现压缩优势。"""
    data = bytearray()
    road_ids = [f"RD-{i:05d}" for i in range(10000, 10100)]
    for _ in range(size_kb * 1024 // 64):  # 每条约64字节
        road = random.choice(road_ids)
        data.extend(road.encode("utf-8"))
        data.extend(b"|")
        data.extend(f"{random.randint(20,120)}|{random.randint(1,4)}|".encode("utf-8"))
        data.extend(f"{random.uniform(5,100):.1f}|".encode("utf-8"))
    return bytes(data[:size_kb * 1024])


def benchmark_compression(num_chunks=10, chunk_size_kb=256):
    """Snappy-like压缩 vs 不压缩基准测试"""
    print("\n" + "=" * 72)
    print("  基准测试 2: 压缩 vs 不压缩 存储对比")
    print("=" * 72)

    # 生成测试数据
    print(f"  生成 {num_chunks} 个 {chunk_size_kb} KB 测试块...")
    chunks = [generate_chunk_data(chunk_size_kb) for _ in range(num_chunks)]
    total_raw = sum(len(c) for c in chunks)

    # 不压缩（基准）
    t, _ = time_it(lambda: [c for c in chunks])
    noc_time = t

    # 压缩
    compress_time = 0.0
    decompress_time = 0.0
    total_compressed = 0
    for chunk in chunks:
        t1, comp = time_it(simple_rle_compress, chunk)
        compress_time += t1
        total_compressed += len(comp)
        t2, dec = time_it(simple_rle_decompress, comp)
        decompress_time += t2
        # 验证完整性
        assert dec == chunk, "压缩解压验证失败!"

    ratio = total_raw / total_compressed if total_compressed else 0
    compression_speed = total_raw / compress_time / (1024 * 1024) if compress_time else 0
    decompression_speed = total_raw / decompress_time / (1024 * 1024) if decompress_time else 0

    print(f"  原始数据总量: {fmt_bytes(total_raw)}")
    print(f"  压缩后总量:   {fmt_bytes(total_compressed)}")
    print(f"  压缩比:       {ratio:.2f}x")
    print(f"  压缩速度:     {compression_speed:.1f} MB/s")
    print(f"  解压速度:     {decompression_speed:.1f} MB/s")
    print(f"  压缩耗时:     {fmt_duration(compress_time)}")
    print(f"  解压耗时:     {fmt_duration(decompress_time)}")

    return {
        "name": "Snappy 压缩 vs 不压缩",
        "raw_size": total_raw,
        "compressed_size": total_compressed,
        "ratio": ratio,
        "compress_time": compress_time,
        "decompress_time": decompress_time,
        "comp_speed_mbps": compression_speed,
        "decomp_speed_mbps": decompression_speed,
    }


# ==============================================================================
# 基准测试3: MapJoin vs ReduceJoin 模拟
# ==============================================================================

def benchmark_join():
    """
    MapJoin vs ReduceJoin 模拟：
    - 小表: 100条维度数据 (road_id -> road_name)
    - 大表: 100,000条事实数据
    - MapJoin: 小表放入内存hash map，逐条查找 O(1)
    - ReduceJoin: 对大表和小表分别排序，然后归并 (sort-merge join)
    """
    print("\n" + "=" * 72)
    print("  基准测试 3: MapJoin vs ReduceJoin 模拟")
    print("=" * 72)

    # 小维度表
    dim_table = {f"RD-{i:05d}": ROAD_NAMES[i % len(ROAD_NAMES)] for i in range(10000, 10100)}

    # 大事实表
    n_large = 100000
    print(f"  生成大表 ({n_large:,} 行) 和小表 ({len(dim_table)} 行)...")
    fact_table = []
    for i in range(n_large):
        road_id = f"RD-{random.randint(10000, 10099):05d}"
        fact_table.append({
            "road_id": road_id,
            "plate": f"京A{random.randint(10000, 99999)}",
            "speed": random.randint(20, 120),
        })

    # --- MapJoin (Broadcast Hash Join) ---
    def mapjoin(fact, dim):
        results = []
        for row in fact:
            road_name = dim.get(row["road_id"], "未知路段")
            results.append({**row, "road_name": road_name})
        return results

    t_map, result_map = time_it(mapjoin, fact_table, dim_table)
    print(f"\n  MapJoin (Hash Join):")
    print(f"    策略: 小表广播到内存，Hash查找 O(1)")
    print(f"    耗时: {fmt_duration(t_map)}")
    print(f"    结果: {len(result_map):,} 行")

    # --- ReduceJoin (Sort-Merge Join) ---
    def reduce_join(fact, dim_dict):
        # 排序阶段
        dim_sorted = sorted(dim_dict.items(), key=lambda x: x[0])
        fact_sorted = sorted(fact, key=lambda x: x["road_id"])

        # 归并阶段
        results = []
        di = 0
        dn = len(dim_sorted)
        for row in fact_sorted:
            rid = row["road_id"]
            # 移动维度表指针
            while di < dn and dim_sorted[di][0] < rid:
                di += 1
            if di < dn and dim_sorted[di][0] == rid:
                road_name = dim_sorted[di][1]
            else:
                road_name = "未知路段"
            results.append({**row, "road_name": road_name})
        return results

    t_reduce, result_reduce = time_it(reduce_join, fact_table, dim_table)
    print(f"\n  ReduceJoin (Sort-Merge Join):")
    print(f"    策略: 两表排序后归并，O(N log N)")
    print(f"    耗时: {fmt_duration(t_reduce)}")
    print(f"    结果: {len(result_reduce):,} 行")

    speedup = t_reduce / t_map if t_map else 0
    print(f"\n  MapJoin 加速比: {speedup:.2f}x 快于 ReduceJoin")

    # 验证结果一致性
    map_result_dict = {(r["road_id"], r["plate"], r["speed"]): r["road_name"] for r in result_map}
    reduce_result_dict = {(r["road_id"], r["plate"], r["speed"]): r["road_name"] for r in result_reduce}
    consistent = map_result_dict == reduce_result_dict
    print(f"  结果一致性: {'[OK] 一致' if consistent else '[FAIL] 不一致!'}")

    return {
        "name": "MapJoin vs ReduceJoin",
        "map_time": t_map,
        "reduce_time": t_reduce,
        "speedup": speedup,
        "consistent": consistent,
    }


# ==============================================================================
# 汇总输出
# ==============================================================================

def print_summary_table(results):
    """打印汇总表格"""
    print("\n")
    print("=" * 72)
    print("                     基准测试结果汇总")
    print("=" * 72)
    print(f"  {'测试项':<30} {'关键指标':<42}")
    print(f"  {'-'*30} {'-'*42}")

    if "ser" in results:
        r = results["ser"]
        msg = "ORC节省空间 {:.2f}x".format(r["size_ratio"])
        print(f"  {'TEXTFILE vs ORC 序列化':<30} {msg:<42}")

    if "comp" in results:
        r = results["comp"]
        msg = "压缩比 {:.2f}x, 压缩速度 {:.1f} MB/s".format(r["ratio"], r["comp_speed_mbps"])
        print(f"  {'Snappy压缩 vs 不压缩':<30} {msg:<42}")

    if "join" in results:
        r = results["join"]
        consistent_str = "[OK]" if r["consistent"] else "[FAIL]"
        msg = "MapJoin 快 {:.2f}x, 结果{}".format(r["speedup"], consistent_str)
        print(f"  {'MapJoin vs ReduceJoin':<30} {msg:<42}")

    print("=" * 72)


# ==============================================================================
# 主入口
# ==============================================================================

def main():
    print("=" * 72)
    print("  Hive/Spark 存储格式 & Join 策略性能基准测试 (纯Python模拟)")
    print("=" * 72)

    random.seed(42)
    results = {}

    # 1. 序列化对比
    num_rows = 100000
    print(f"\n  预生成 {num_rows:,} 行测试数据...")
    test_rows = [generate_row() for _ in range(num_rows)]
    results["ser"] = benchmark_serialization(test_rows)

    # 2. 压缩对比
    results["comp"] = benchmark_compression(num_chunks=20, chunk_size_kb=128)

    # 3. Join 对比
    results["join"] = benchmark_join()

    # 汇总
    print_summary_table(results)
    print("\n  所有基准测试完成。\n")


if __name__ == "__main__":
    main()
