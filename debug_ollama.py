#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""调试 Ollama 返回的原始内容"""
import sys, json, urllib.request
sys.stdout.reconfigure(encoding='utf-8')

question = "昨天最拥堵的5条路"

schemas = """- dws_road_hour_flow: road_id STRING, hour INT, traffic_count BIGINT, avg_speed DECIMAL
- ads_top_jam_roads: rank_num INT, road_id STRING, road_name STRING, avg_jam_level DECIMAL"""

prompt = f"""你是一个 Hive SQL 专家。根据用户问题生成 Hive SQL。

数据库 traffic_db 包含以下表：
{schemas}

要求：
1. 只返回 Hive SQL，不要有任何解释、注释或 markdown 标记
2. 分区条件统一使用 dt = '2026-06-10'
3. LIMIT 不要超过 20
4. 表名不要加 traffic_db. 前缀
5. SQL 必须语法正确、字段名必须来自上面的表结构

用户问题：{question}
SQL："""

print("[1] 发送请求到 Ollama...")
payload = json.dumps({
    "model": "qwen3:8b",
    "prompt": prompt,
    "stream": False,
    "temperature": 0.1,
}).encode("utf-8")

try:
    req = urllib.request.Request(
        "http://localhost:11434/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        data = json.loads(r.read())
        raw = data.get("response", "").strip()

    print(f"\n[2] Ollama 原始返回 ({len(raw)} 字符):")
    print("=" * 50)
    print(repr(raw[:500]))
    print("=" * 50)

    print(f"\n[3] 分段显示（按行）:")
    for i, line in enumerate(raw.split("\n")):
        print(f"  [{i}] {line}")

except Exception as e:
    print(f"\n[❌] 错误: {e}")

input("\n按回车退出...")
