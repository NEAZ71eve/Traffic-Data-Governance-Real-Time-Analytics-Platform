"""
SCD2 (Slowly Changing Dimension Type 2) 拉链表 ETL 验证
使用 SQLite 模拟完整 SCD2 拉链表逻辑，零第三方依赖。

目标:
  - dim_road_zip:  道路维度拉链表 (5条道路, 模拟2天变更)
  - dim_device_zip: 设备维度拉链表 (5台设备, 模拟2天变更)

SCD2 核心逻辑:
  - NEW:      插入 start_time=当前日期, end_time='9999-12-31', is_current='Y'
  - UPDATE:   闭合旧记录 (end_time=昨天, is_current='N') + 插入新版本
  - UNCHANGED: 保持原样
"""
import sqlite3
import os
import sys
from datetime import date, timedelta

print("=" * 65)
print("  SCD2 拉链表 ETL 验证 — SQLite 实现")
print("=" * 65)

# ── 初始化内存数据库 ──────────────────────────────────────
conn = sqlite3.connect(":memory:")
conn.row_factory = sqlite3.Row

# ── 建表 (结构与项目 SQL 文件完全一致) ────────────────────
conn.executescript("""
    CREATE TABLE dim_road_zip (
        road_id      TEXT    NOT NULL,
        road_name    TEXT    NOT NULL,
        road_type    TEXT    NOT NULL,
        road_length  REAL    NOT NULL,
        lane_count   INTEGER NOT NULL,
        speed_limit  INTEGER NOT NULL,
        area_id      TEXT    NOT NULL,
        direction    TEXT    NOT NULL,
        start_time   TEXT    NOT NULL,
        end_time     TEXT    NOT NULL,
        is_current   TEXT    NOT NULL CHECK(is_current IN ('Y','N')),
        dt           TEXT    NOT NULL
    );

    CREATE TABLE dim_device_zip (
        device_id    TEXT    NOT NULL,
        device_name  TEXT    NOT NULL,
        device_type  TEXT    NOT NULL,
        device_model TEXT    NOT NULL,
        road_id      TEXT    NOT NULL,
        area_id      TEXT    NOT NULL,
        install_date TEXT    NOT NULL,
        manufacturer TEXT    NOT NULL,
        firmware_ver TEXT    NOT NULL,
        status       TEXT    NOT NULL,
        start_time   TEXT    NOT NULL,
        end_time     TEXT    NOT NULL,
        is_current   TEXT    NOT NULL CHECK(is_current IN ('Y','N')),
        dt           TEXT    NOT NULL
    );

    CREATE INDEX idx_rd_cur ON dim_road_zip(road_id, is_current, dt);
    CREATE INDEX idx_dev_cur ON dim_device_zip(device_id, is_current, dt);
""")

# ── 源数据定义 ────────────────────────────────────────────
# ---- 道路源数据 ----
ROADS_SOURCE = [
    # road_id, road_name,        road_type, road_length, lane_count, speed_limit, area_id, direction
    ["R001", "京藏高速",   "高速",   25.60, 4, 120, "A01", "BIDIRECTIONAL"],
    ["R002", "北四环路",   "主干道", 8.20,  6, 80,  "A01", "BIDIRECTIONAL"],
    ["R003", "长安街",     "主干道", 6.80,  8, 60,  "A01", "E"],
    ["R004", "二环路",     "快速路", 32.70, 6, 80,  "A01", "BIDIRECTIONAL"],
    ["R005", "京通快速",   "快速路", 14.00, 4, 100, "A02", "BIDIRECTIONAL"],
]

# ---- 设备源数据 ----
DEVICES_SOURCE = [
    # device_id, device_name,           device_type, device_model, road_id, area_id, install_date, manufacturer, firmware_ver, status
    ["D001",     "北四环摄像头01",      "CAMERA",    "HK-DS2CD",  "R002",  "A01",   "2024-03-15", "海康威视", "v3.2.1", "RUNNING"],
    ["D002",     "京藏高速雷达01",      "RADAR",     "DW-R350",   "R001",  "A01",   "2024-06-01", "德威",     "v2.0.4", "RUNNING"],
    ["D003",     "长安街信号灯01",      "TRAFFIC_LIGHT","TL-X500","R003",  "A01",   "2023-11-20", "西门子",   "v5.1.0", "RUNNING"],
    ["D004",     "二环路传感器01",      "SENSOR",    "SEN-GEO5",  "R004",  "A01",   "2024-01-10", "Geophone","v1.8.3", "RUNNING"],
    ["D005",     "京通快速闸机01",      "GATE",      "GT-B200",   "R005",  "A02",   "2024-04-22", "大华",     "v4.0.1", "MAINTENANCE"],
]

# ── 变更定义 (模拟两天的源数据变化) ──────────────────────
# Day 2 变更:
#   R001 京藏高速: speed_limit 120→100 (限速下调)
#   R003 长安街:   lane_count 8→6     (车道缩减)
#   D001:         firmware_ver v3.2.1→v3.3.0 (固件升级)
#   D005:         status MAINTENANCE→RUNNING  (恢复运行)
#   NEW road:     R006 "三环路" (新增道路)
# Day 3 变更:
#   R002 北四环路: lane_count 6→8    (车道扩建)
#   D002:          status RUNNING→MAINTENANCE (检修)

ROADS_DAY2 = [list(r) for r in ROADS_SOURCE]
for r in ROADS_DAY2:
    if r[0] == "R001":
        r[5] = 100           # 限速 120→100
    if r[0] == "R003":
        r[4] = 6             # 车道 8→6
ROADS_DAY2.append(["R006", "三环路", "快速路", 48.30, 6, 80, "A01", "BIDIRECTIONAL"])

ROADS_DAY3 = [list(r) for r in ROADS_DAY2]
for r in ROADS_DAY3:
    if r[0] == "R002":
        r[4] = 8             # 车道 6→8

DEVICES_DAY2 = [list(d) for d in DEVICES_SOURCE]
for d in DEVICES_DAY2:
    if d[0] == "D001":
        d[8] = "v3.3.0"      # 固件升级
    if d[0] == "D005":
        d[9] = "RUNNING"     # 恢复运行

DEVICES_DAY3 = [list(d) for d in DEVICES_DAY2]
for d in DEVICES_DAY3:
    if d[0] == "D002":
        d[9] = "MAINTENANCE" # 检修

# ── SCD2 Merge 核心函数 ──────────────────────────────────
ROAD_COLS = ["road_id","road_name","road_type","road_length","lane_count",
             "speed_limit","area_id","direction"]
ROAD_COMPARE = ["road_name","road_type","road_length","lane_count","speed_limit",
                "area_id","direction"]  # 变更检测列

DEVICE_COLS = ["device_id","device_name","device_type","device_model",
               "road_id","area_id","install_date","manufacturer","firmware_ver","status"]
DEVICE_COMPARE = ["device_name","device_type","device_model","road_id","area_id",
                  "install_date","manufacturer","firmware_ver","status"]

def scd2_merge(conn, table, pk_col, src_rows, columns, compare_cols, prev_dt, cur_dt):
    """一次 SCD2 合并周期。prev_dt 之前的记录必须是有效且 is_current='Y' 的。"""
    ET = "9999-12-31"

    # 1) 加载前一天的当前记录
    if prev_dt is None:
        prev_rows = {}  # 首日无历史
    else:
        cur = conn.execute(
            f"SELECT * FROM [{table}] WHERE dt=? AND is_current='Y'", (prev_dt,)
        )
        prev_rows = {r[pk_col]: dict(r) for r in cur.fetchall()}

    src_map = {r[0]: r for r in src_rows}          # pk → 源行
    prev_ids = set(prev_rows.keys())
    src_ids   = set(src_map.keys())

    new_count    = 0
    update_count = 0
    unchanged    = 0

    param_placeholders = ",".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO [{table}] ({','.join(columns)},start_time,end_time,is_current,dt) VALUES ({param_placeholders},?,?,?,?)"

    for pk in src_ids:
        src = src_map[pk]

        if pk not in prev_ids:
            # ── NEW ──
            conn.execute(insert_sql, [*src, cur_dt, ET, "Y", cur_dt])
            new_count += 1
            continue

        old = prev_rows[pk]
        # 检测是否变更
        changed = False
        for c in compare_cols:
            if str(src[columns.index(c)]) != str(old[c]):
                changed = True
                break

        if changed:
            # ── UPDATE: 闭合旧记录 ──
            yesterday = str(date.fromisoformat(cur_dt) - timedelta(days=1))
            conn.execute(
                f"UPDATE [{table}] SET end_time=?, is_current='N' WHERE {pk_col}=? AND dt=? AND is_current='Y'",
                (yesterday, pk, prev_dt)
            )
            # 插入新版本
            conn.execute(insert_sql, [*src, cur_dt, ET, "Y", cur_dt])
            update_count += 1
        else:
            # ── UNCHANGED: 直接复制 ──
            conn.execute(insert_sql, [*src, old["start_time"], old["end_time"], "Y", cur_dt])
            unchanged += 1

    conn.commit()
    return new_count, update_count, unchanged

# ── 执行 3 个周期 (Day1 初始化 + Day2 + Day3) ─────────────
DATES = ["2026-06-07", "2026-06-08", "2026-06-09"]
road_sources = [ROADS_SOURCE, ROADS_DAY2, ROADS_DAY3]
dev_sources  = [DEVICES_SOURCE, DEVICES_DAY2, DEVICES_DAY3]

road_stats = []
dev_stats  = []

for i, dt in enumerate(DATES):
    prev = DATES[i-1] if i > 0 else None
    src_road = road_sources[i]
    src_dev  = dev_sources[i]

    nr, ur, un = scd2_merge(conn, "dim_road_zip", "road_id",
                            src_road, ROAD_COLS, ROAD_COMPARE, prev, dt)
    nd, ud, un2 = scd2_merge(conn, "dim_device_zip", "device_id",
                             src_dev, DEVICE_COLS, DEVICE_COMPARE, prev, dt)

    road_stats.append((dt, nr, ur, un))
    dev_stats.append((dt, nd, ud, un2))

    label = "初始化" if i == 0 else f"Day{i+1} 增量"
    print(f"\n[{dt}] {label}")
    print(f"  dim_road_zip   新增={nr}  变更={ur}  未变={un}")
    print(f"  dim_device_zip  新增={nd}  变更={ud}  未变={un2}")

# ── 打印版本详情 ──────────────────────────────────────────
print("\n" + "=" * 65)
print("  拉链版本明细")
print("=" * 65)

print("\n── dim_road_zip ──")
rows = conn.execute(
    "SELECT road_id,road_name,lane_count,speed_limit,start_time,end_time,is_current,dt "
    "FROM dim_road_zip ORDER BY road_id, dt, start_time"
).fetchall()
road_total = len(rows)
print(f"  {'道路ID':<6s} {'名称':<10s} {'车道':<4s} {'限速':<5s} {'开始日期':<12s} {'结束日期':<12s} {'当前':<5s} {'分区':<12s}")
print(f"  {'-'*70}")
for r in rows:
    icon = "●" if r["is_current"] == "Y" else "○"
    print(f"  {r['road_id']:<6s} {r['road_name']:<10s} {r['lane_count']:<4d} {r['speed_limit']:<5d} {r['start_time']:<12s} {r['end_time']:<12s} {icon:<5s} {r['dt']:<12s}")

print(f"\n── dim_device_zip ──")
rows = conn.execute(
    "SELECT device_id,device_name,device_type,firmware_ver,status,start_time,end_time,is_current,dt "
    "FROM dim_device_zip ORDER BY device_id, dt, start_time"
).fetchall()
dev_total = len(rows)
print(f"  {'设备ID':<6s} {'名称':<16s} {'类型':<14s} {'固件':<8s} {'状态':<14s} {'开始日期':<12s} {'结束日期':<12s} {'当前':<5s} {'分区':<12s}")
print(f"  {'-'*85}")
for r in rows:
    icon = "●" if r["is_current"] == "Y" else "○"
    print(f"  {r['device_id']:<6s} {r['device_name']:<16s} {r['device_type']:<14s} {r['firmware_ver']:<8s} {r['status']:<14s} {r['start_time']:<12s} {r['end_time']:<12s} {icon:<5s} {r['dt']:<12s}")

# ── 断言验证 ──────────────────────────────────────────────
PASS = 0
FAIL = 0

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name} {detail}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")
    return condition

print(f"\n{'='*65}")
print("  数据完整性验证")
print("=" * 65)

# 1. 过期版本 end_time != '9999-12-31'
expired_road = conn.execute(
    "SELECT COUNT(*) as cnt FROM dim_road_zip WHERE is_current='N' AND end_time='9999-12-31'"
).fetchone()
check("过期道路版本 end_time 非 9999-12-31",
      expired_road["cnt"] == 0,
      f"异常={expired_road['cnt']}")

expired_dev = conn.execute(
    "SELECT COUNT(*) as cnt FROM dim_device_zip WHERE is_current='N' AND end_time='9999-12-31'"
).fetchone()
check("过期设备版本 end_time 非 9999-12-31",
      expired_dev["cnt"] == 0,
      f"异常={expired_dev['cnt']}")

# 2. 当前版本 end_time 必须 = '9999-12-31'
invalid_current_road = conn.execute(
    "SELECT COUNT(*) as cnt FROM dim_road_zip WHERE is_current='Y' AND end_time!='9999-12-31'"
).fetchone()
check("当前道路版本 end_time='9999-12-31'",
      invalid_current_road["cnt"] == 0,
      f"异常={invalid_current_road['cnt']}")

invalid_current_dev = conn.execute(
    "SELECT COUNT(*) as cnt FROM dim_device_zip WHERE is_current='Y' AND end_time!='9999-12-31'"
).fetchone()
check("当前设备版本 end_time='9999-12-31'",
      invalid_current_dev["cnt"] == 0,
      f"异常={invalid_current_dev['cnt']}")

# 3. is_current 与 end_time 一致性
inconsistent_road = conn.execute(
    "SELECT COUNT(*) as cnt FROM dim_road_zip WHERE "
    "(is_current='Y' AND end_time!='9999-12-31') OR (is_current='N' AND end_time='9999-12-31')"
).fetchone()
check("is_current 与 end_time 一致性(road)",
      inconsistent_road["cnt"] == 0,
      f"不一致={inconsistent_road['cnt']}")

inconsistent_dev = conn.execute(
    "SELECT COUNT(*) as cnt FROM dim_device_zip WHERE "
    "(is_current='Y' AND end_time!='9999-12-31') OR (is_current='N' AND end_time='9999-12-31')"
).fetchone()
check("is_current 与 end_time 一致性(device)",
      inconsistent_dev["cnt"] == 0,
      f"不一致={inconsistent_dev['cnt']}")

# 4. 最终日期的当前记录与源数据一致
for r in ROADS_DAY3:
    row = conn.execute(
        "SELECT * FROM dim_road_zip WHERE road_id=? AND dt='2026-06-09' AND is_current='Y'",
        (r[0],)
    ).fetchone()
    check(f"道路 {r[0]} 最终版本与源一致",
          row is not None and row["road_name"] == r[1],
          f"{row['road_name']!r} vs {r[1]!r}" if row else "缺失")

for d in DEVICES_DAY3:
    row = conn.execute(
        "SELECT * FROM dim_device_zip WHERE device_id=? AND dt='2026-06-09' AND is_current='Y'",
        (d[0],)
    ).fetchone()
    check(f"设备 {d[0]} 最终版本与源一致",
          row is not None and row["status"] == d[9],
          f"{row['status']!r} vs {d[9]!r}" if row else "缺失")

# 5. 无孤立版本 (每个 pk 在每个 dt 分区有且仅有 1 条 is_current='Y')
for dt in DATES:
    dup_road = conn.execute(
        "SELECT road_id, COUNT(*) as cnt FROM dim_road_zip WHERE dt=? AND is_current='Y' GROUP BY road_id HAVING cnt>1",
        (dt,)
    ).fetchall()
    check(f"道路分区 {dt} 无重复当前版本",
          len(dup_road) == 0,
          f"重复={len(dup_road)}" if dup_road else "")
    dup_dev = conn.execute(
        "SELECT device_id, COUNT(*) as cnt FROM dim_device_zip WHERE dt=? AND is_current='Y' GROUP BY device_id HAVING cnt>1",
        (dt,)
    ).fetchall()
    check(f"设备分区 {dt} 无重复当前版本",
          len(dup_dev) == 0,
          f"重复={len(dup_dev)}" if dup_dev else "")

# ── 汇总 ──────────────────────────────────────────────────
print(f"\n{'='*65}")
print(f"  汇总报告")
print(f"  {'─'*40}")
print(f"  {"当日":<8s} {"Road新增":>8s} {"变更":>4s} {"未变":>4s}   {"Dev新增":>8s} {"变更":>4s} {"未变":>4s}")
for i, dt in enumerate(DATES):
    _, nr, ur, un = road_stats[i]
    _, nd, ud, un2 = dev_stats[i]
    print(f"  {dt:<8s} {nr:>8d} {ur:>4d} {un:>4d}   {nd:>8d} {ud:>4d} {un2:>4d}")

total_road_versions = conn.execute("SELECT COUNT(*) FROM dim_road_zip").fetchone()[0]
total_dev_versions  = conn.execute("SELECT COUNT(*) FROM dim_device_zip").fetchone()[0]
total_changes = sum(ur for _,_,ur,_ in road_stats) + sum(ud for _,_,ud,_ in dev_stats)

print(f"  {'─'*40}")
print(f"  道路拉链总版本:  {total_road_versions}")
print(f"  设备拉链总版本:  {total_dev_versions}")
print(f"  累计变更次数:    {total_changes}")
print(f"  断言通过:        {PASS}/{PASS+FAIL}")
print(f"{'='*65}")

if FAIL > 0:
    print(f"\n  [FAIL] {FAIL} 项断言失败!")
    sys.exit(1)
else:
    print(f"\n  [OK] SCD2 拉链表验证全部通过!")

conn.close()
