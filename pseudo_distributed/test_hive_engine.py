#!/usr/bin/env python3
"""测试 Hive 模拟引擎 - 加载项目实际 SQL 文件"""
import sys
sys.path.insert(0, 'pseudo_distributed')
from hive_simulator import get_hive

hive = get_hive()

# 测试 24 个 SQL 文件
sql_files = [
    ("ODS", [
        "sql/ods/ods_vehicle_pass_di.sql",
        "sql/ods/ods_traffic_status_di.sql",
        "sql/ods/ods_device_status_di.sql",
        "sql/ods/ods_alarm_log_di.sql",
        "sql/ods/ods_road_info.sql",
        "sql/ods/ods_device_info.sql",
        "sql/ods/ods_area_info.sql",
    ]),
    ("DIM", [
        "sql/dim/dim_road_zip.sql",
        "sql/dim/dim_device_zip.sql",
        "sql/dim/dim_time.sql",
        "sql/dim/dim_area.sql",
    ]),
    ("DWD", [
        "sql/dwd/dwd_vehicle_pass_di.sql",
        "sql/dwd/dwd_traffic_status_di.sql",
        "sql/dwd/dwd_device_status_di.sql",
        "sql/dwd/dwd_alarm_log_di.sql",
    ]),
    ("DWS", [
        "sql/dws/dws_road_hour_flow.sql",
        "sql/dws/dws_area_jam_hour.sql",
        "sql/dws/dws_device_health_day.sql",
        "sql/dws/dws_alarm_day.sql",
    ]),
    ("ADS", [
        "sql/ads/ads_traffic_operation.sql",
        "sql/ads/ads_top_jam_roads.sql",
        "sql/ads/ads_device_health_score.sql",
        "sql/ads/ads_device_mtbf_mttr.sql",
        "sql/ads/ads_device_fault_top.sql",
    ]),
]

print("=" * 60)
print("  Hive SQL 加载测试 - 24 个 SQL 文件")
print("=" * 60)

total_ok = 0
total_err = 0
storage_summary = {"TEXTFILE": 0, "ORC": 0}

for layer_name, files in sql_files:
    print(f"\n--- {layer_name} 层 ({len(files)} 张表) ---")
    for fp in files:
        try:
            result = hive.load_sql_file(fp)
            lines = result.split("\n") if result else []
            create_lines = [l for l in lines if "CREATE TABLE" in l]
            create_line = create_lines[0] if create_lines else result[:80] if result else "N/A"
            print(f"  [OK] {fp.split('/')[-1]} : {create_line[:90]}")
            total_ok += 1

            # 统计存储格式
            tbl_name = fp.split("/")[-1].replace(".sql", "")
            info = hive.get_table_info(tbl_name)
            if info.get("storage"):
                storage_summary[info["storage"]] = storage_summary.get(info["storage"], 0) + 1
        except Exception as e:
            print(f"  [ERR] {fp.split('/')[-1]} : {str(e)[:80]}")
            total_err += 1

print(f"\n{'=' * 60}")
print(f"  结果: {total_ok}/{total_ok+total_err} 文件加载成功")
print(f"  存储格式: TEXTFILE={storage_summary.get('TEXTFILE', 0)}, ORC={storage_summary.get('ORC', 0)}")
print(f"  建表总数: {len(hive.tables)} 张表")
print(f"{'=' * 60}")

# 测试跨层关联查询
print("\n--- 跨层关联查询测试 ---")
queries = [
    "SELECT road_id, SUM(traffic_count) as total_flow FROM dws_road_hour_flow WHERE dt='2026-06-08' GROUP BY road_id",
    "SELECT device_id, health_score FROM ads_device_health_score ORDER BY health_score DESC LIMIT 5",
    "SELECT jam_level, COUNT(*) as cnt FROM ads_top_jam_roads GROUP BY jam_level",
]
for q in queries:
    rows = hive.execute_dml(q)
    print(f"  [OK] {q[:60]}... -> {len(rows)} rows")
