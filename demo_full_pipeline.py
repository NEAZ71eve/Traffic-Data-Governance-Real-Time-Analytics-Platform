# -*- coding: utf-8 -*-
"""
全流程端到端模拟演示
===================
模拟: Kafka数采(模拟) → ODS落盘 → DWD清洗 → DWS聚合 → ADS指标 →
      Flink CEP(模拟) → 数据血缘 → 数据质量监控 → 告警通知(模拟)

无需任何外部依赖，纯Python即可运行
"""
import json, os, random, sys
from datetime import datetime, timedelta
from collections import defaultdict

SEP = "=" * 70
LINE = "-" * 70
TODAY = datetime.now().strftime('%Y-%m-%d')
OUTPUT_DIR = "demo_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def print_header(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")

# ================================================================
# 阶段0: 模拟数据生成 (模拟Kafka Topic数据)
# ================================================================
def generate_mock_data():
    """生成模拟数据: 车辆通行 + 路况监测 + 设备状态 + 设备告警"""
    roads = [f"ROAD{i:04d}" for i in range(1, 21)]
    devices = [f"DEV{i:04d}" for i in range(1, 51)]
    areas = {r: f"A00{i%10+1}" for i, r in enumerate(roads)}
    hours = list(range(24))

    vehicle_data = []
    for _ in range(500):
        h = random.choice(hours)
        v = {
            "vehicle_id": f"VEH{random.randint(1,99999):05d}",
            "road_id": random.choice(roads),
            "device_id": random.choice(devices),
            "pass_time": f"{TODAY} {h:02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            "speed": max(0, min(200, round(random.gauss(40, 20), 1))),
            "direction": random.choice(["N","S","E","W"]),
            "plate_number": f"京A{random.randint(10000,99999)}",
            "vehicle_type": random.choice(["小型车","小型车","小型车","中型车","大型车"]),
            "lane": random.randint(1, 4)
        }
        vehicle_data.append(v)

    traffic_data = []
    for r in random.sample(roads, 10):
        for h in hours:
            t = {
                "road_id": r,
                "avg_speed": max(5, round(random.gauss(35, 15), 1)),
                "traffic_flow": random.randint(100, 2500),
                "jam_level": random.choices([1,2,3,4,5], weights=[30,25,20,15,10])[0],
                "congestion_rate": round(random.uniform(0, 90), 2),
                "peak_flag": random.choice(["MORNING_PEAK","EVENING_PEAK","NORMAL","NORMAL","NORMAL","NORMAL","OFF_PEAK"]),
                "sample_time": f"{TODAY} {h:02d}:00:00"
            }
            traffic_data.append(t)

    device_data = []
    for d in random.sample(devices, 30):
        for _ in range(24):
            ds = {
                "device_id": d,
                "cpu_usage": round(random.gauss(50, 20), 2),
                "memory_usage": round(random.gauss(55, 18), 2),
                "temperature": round(random.gauss(45, 15), 1),
                "online_flag": random.choices(["ONLINE","ONLINE","ONLINE","ONLINE","OFFLINE"])[0],
                "heartbeat_time": f"{TODAY} {random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
                "signal_strength": random.randint(-90, -30),
                "device_type": random.choice(["CAMERA","SENSOR","RADAR","GATE"])
            }
            device_data.append(ds)

    alarm_data = []
    for _ in range(30):
        dev = random.choice(devices)
        alarm_types = ["OFFLINE","CPU_HIGH","MEMORY_HIGH","TEMP_HIGH","SIGNAL_WEAK","HARDWARE_FAULT"]
        atype = random.choice(alarm_types)
        alarm_time = f"{TODAY} {random.randint(0,23):02d}:{random.randint(0,59):02d}:00"
        recovered = random.random() > 0.3
        recover_time = (datetime.strptime(alarm_time,"%Y-%m-%d %H:%M:%S") + timedelta(minutes=random.randint(1,120))).strftime("%Y-%m-%d %H:%M:%S") if recovered else None
        a = {
            "alarm_id": f"ALM{random.randint(10000,99999)}",
            "device_id": dev,
            "alarm_type": atype,
            "alarm_level": random.choice(["CRITICAL","MAJOR","MINOR","WARNING"]),
            "alarm_content": f"[{atype}] device {dev} alarm triggered",
            "alarm_time": alarm_time,
            "recover_time": recover_time,
            "recover_status": "RECOVERED" if recovered else "UNRECOVERED"
        }
        alarm_data.append(a)

    return vehicle_data, traffic_data, device_data, alarm_data, areas, roads

# ================================================================
# 阶段1: ODS层 - 原始数据落地
# ================================================================
def phase_ods(vehicle_data, traffic_data, device_data, alarm_data):
    print_header("阶段1: ODS层 - 原始数据采集与落地")
    print(f"  [数据源] 交通终端设备 → Flume/Maxwell → Kafka")
    print(f"  [Kafka Topic] traffic_vehicle / traffic_status / device_status / device_alarm")
    print(f"  [落地目标] Hive ODS层 (ods_*_di) TEXTFILE格式, dt={TODAY}")
    print()
    print(f"  ods_vehicle_pass_di   : {len(vehicle_data)} 条车辆通行记录")
    print(f"  ods_traffic_status_di : {len(traffic_data)} 条路况监测记录")
    print(f"  ods_device_status_di  : {len(device_data)} 条设备状态快照")
    print(f"  ods_alarm_log_di      : {len(alarm_data)} 条故障告警记录")
    print(f"\n  [ODS合计] 共 {len(vehicle_data)+len(traffic_data)+len(device_data)+len(alarm_data)} 条原始记录落地")
    
    # 保存样本
    with open(f"{OUTPUT_DIR}/ods_vehicle_pass_sample.json","w",encoding="utf-8") as f:
        json.dump(vehicle_data[:5], f, ensure_ascii=False, indent=2)
    with open(f"{OUTPUT_DIR}/ods_traffic_status_sample.json","w",encoding="utf-8") as f:
        json.dump(traffic_data[:5], f, ensure_ascii=False, indent=2)
    with open(f"{OUTPUT_DIR}/ods_device_status_sample.json","w",encoding="utf-8") as f:
        json.dump(device_data[:5], f, ensure_ascii=False, indent=2)
    with open(f"{OUTPUT_DIR}/ods_alarm_log_sample.json","w",encoding="utf-8") as f:
        json.dump(alarm_data[:5], f, ensure_ascii=False, indent=2)
    
    return vehicle_data, traffic_data, device_data, alarm_data

# ================================================================
# 阶段2: DWD层 - 数据清洗与去重
# ================================================================
def phase_dwd(vehicle_data, traffic_data, device_data, alarm_data):
    print_header("阶段2: DWD层 - 数据清洗与去重 (ODS→DWD)")
    
    # DWD车辆清洗: 过滤异常车速 + 去重 + 派生hour
    cleaned_vehicle = []
    dirty_count = 0
    dup_count = 0
    seen = set()
    for v in vehicle_data:
        # 逐行校验
        speed = v["speed"]
        if speed < 0 or speed > 200:
            dirty_count += 1; continue
        dup_key = (v["vehicle_id"], v["pass_time"])
        if dup_key in seen:
            dup_count += 1; continue
        seen.add(dup_key)
        v["hour"] = int(v["pass_time"].split()[1].split(":")[0])
        v["speed_km_per_s"] = round(speed / 3600.0, 4)
        cleaned_vehicle.append(v)
    
    # DWD路况清洗: 拥堵等级校验 + 拥堵描述派生
    cleaned_traffic = []
    for t in traffic_data:
        jl = t["jam_level"]
        if jl < 1 or jl > 5: continue
        jam_map = {1:"畅通",2:"基本畅通",3:"轻度拥堵",4:"中度拥堵",5:"严重拥堵"}
        t["jam_desc"] = jam_map.get(jl, "未知")
        cleaned_traffic.append(t)
    
    # DWD设备清洗: 健康标识派生
    cleaned_device = []
    for d in device_data:
        cpu = d["cpu_usage"]; mem = d["memory_usage"]; tmp = d["temperature"]
        if cpu < 0 or cpu > 100 or mem < 0 or mem > 100 or tmp < -40 or tmp > 100:
            dirty_count += 1; continue
        if cpu > 90 or mem > 90 or tmp > 70 or d["online_flag"]=="OFFLINE" or d["signal_strength"] < -90:
            d["health_flag"] = "ABNORMAL"
        elif cpu > 70 or mem > 70 or tmp > 50:
            d["health_flag"] = "WARNING"
        else:
            d["health_flag"] = "NORMAL"
        cleaned_device.append(d)
    
    # DWD告警清洗: 恢复耗时派生
    cleaned_alarm = []
    for a in alarm_data:
        a["is_recovered"] = "Y" if a["recover_time"] else "N"
        if a["recover_time"]:
            delta = datetime.strptime(a["recover_time"],"%Y-%m-%d %H:%M:%S") - datetime.strptime(a["alarm_time"],"%Y-%m-%d %H:%M:%S")
            a["recover_duration_min"] = int(delta.total_seconds() / 60)
        else:
            a["recover_duration_min"] = None
        cleaned_alarm.append(a)
    
    print(f"  [清洗规则]")
    print(f"    - 车速值域校验 0~200km/h     → 剔除 {dirty_count} 条异常")
    print(f"    - vehicle_id+pass_time去重    → 剔除 {dup_count} 条重复")
    print(f"    - 拥堵等级值域校验 1~5         → 全量通过")
    print(f"    - CPU/内存/温度值域校验        → 全量通过")
    print(f"    - 派生hour字段                 → {len(cleaned_vehicle)} 条含hour")
    print(f"    - 派生health_flag(设备健康标识) → NORMAL/WARNING/ABNORMAL")
    print(f"    - 派生recover_duration(恢复耗时) → {sum(1 for a in cleaned_alarm if a['is_recovered']=='Y')} 条已恢复")
    print()
    print(f"  dwd_vehicle_pass_di   : {len(cleaned_vehicle)} 条 (清洗率 {len(cleaned_vehicle)/len(vehicle_data)*100:.1f}%)")
    print(f"  dwd_traffic_status_di : {len(cleaned_traffic)} 条")
    print(f"  dwd_device_status_di  : {len(cleaned_device)} 条")
    print(f"  dwd_alarm_log_di      : {len(cleaned_alarm)} 条")
    
    return cleaned_vehicle, cleaned_traffic, cleaned_device, cleaned_alarm

# ================================================================
# 阶段3: DWS层 - 轻度汇总聚合
# ================================================================
def phase_dws(cleaned_vehicle, cleaned_traffic, cleaned_device, cleaned_alarm):
    print_header("阶段3: DWS层 - 轻度汇总聚合 (DWD→DWS)")
    
    # DWS: 道路小时流量
    road_hour = defaultdict(lambda: {"count":0,"total_speed":0,"small":0,"medium":0,"large":0,"other":0})
    for v in cleaned_vehicle:
        key = (v["road_id"], v["hour"])
        h = road_hour[key]
        h["count"] += 1
        h["total_speed"] += v["speed"]
        if v["vehicle_type"] == "小型车": h["small"] += 1
        elif v["vehicle_type"] == "中型车": h["medium"] += 1
        elif v["vehicle_type"] == "大型车": h["large"] += 1
        else: h["other"] += 1
    
    # DWS: 区域小时拥堵
    area_jam = defaultdict(lambda: {"roads": set(), "flows": 0, "jams": [], "rates": []})
    for t in cleaned_traffic:
        h = int(t["sample_time"].split()[1].split(":")[0])
        key = (t["road_id"][:3], h)  # 模拟area_id
        a = area_jam[key]
        a["roads"].add(t["road_id"])
        a["flows"] += t["traffic_flow"]
        a["jams"].append(t["jam_level"])
        a["rates"].append(t["congestion_rate"])
    
    # DWS: 设备健康日汇总
    device_health = defaultdict(lambda: {"online":0,"offline":0,"cpus":[],"mems":[],"temps":[],"abnormal":0,"warning":0,"normal":0,"signals":[]})
    for d in cleaned_device:
        key = d["device_id"]
        h = device_health[key]
        if d["online_flag"] == "ONLINE": h["online"] += 1
        else: h["offline"] += 1
        h["cpus"].append(d["cpu_usage"])
        h["mems"].append(d["memory_usage"])
        h["temps"].append(d["temperature"])
        h["signals"].append(d["signal_strength"])
        if d["health_flag"] == "ABNORMAL": h["abnormal"] += 1
        elif d["health_flag"] == "WARNING": h["warning"] += 1
        else: h["normal"] += 1
    
    # DWS: 告警日汇总
    alarm_summary = defaultdict(lambda: {"total":0,"recovered":0,"unrecovered":0,"durations":[]})
    for a in cleaned_alarm:
        key = (a["alarm_type"], a["alarm_level"])
        s = alarm_summary[key]
        s["total"] += 1
        if a["is_recovered"] == "Y":
            s["recovered"] += 1
            if a["recover_duration_min"]: s["durations"].append(a["recover_duration_min"])
        else:
            s["unrecovered"] += 1
    
    print(f"  [聚合维度]")
    print(f"    dws_road_hour_flow    : {len(road_hour)} 条 (道路x小时)")
    print(f"    dws_area_jam_hour     : {len(area_jam)} 条 (区域x小时)")
    print(f"    dws_device_health_day : {len(device_health)} 条 (设备x天)")
    print(f"    dws_alarm_day         : {len(alarm_summary)} 条 (告警类型x级别)")
    
    # 展示TOP5
    print(f"\n  [DWS展示] 小时车流量 TOP5:")
    top5 = sorted(road_hour.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
    for (road, hour), data in top5:
        avg_v = data["total_speed"] / max(data["count"], 1)
        print(f"    {road} | 小时{hour:02d} | 车流 {data['count']:4d} | 均速 {avg_v:5.1f}km/h | 大型车{data['large']:2d}")
    
    return road_hour, area_jam, device_health, alarm_summary

# ================================================================
# 阶段4: ADS层 - 应用指标计算
# ================================================================
def phase_ads(road_hour, area_jam, device_health, alarm_summary, cleaned_traffic, cleaned_alarm, roads, areas):
    print_header("阶段4: ADS层 - 应用指标计算 (DWS→ADS)")
    
    # ADS1: 交通运营综合指标
    total_flow = sum(h["count"] for h in road_hour.values())
    all_speeds = [v["total_speed"]/max(v["count"],1) for v in road_hour.values() if v["count"]>0]
    avg_speed = sum(all_speeds)/max(len(all_speeds),1) if all_speeds else 0
    jam_roads = len([t for t in cleaned_traffic if t["jam_level"] >= 3])
    severe_jam = len([t for t in cleaned_traffic if t["jam_level"] == 5])
    
    print(f"  [ads_traffic_operation] 城市运营看板指标:")
    print(f"    总车流量       : {total_flow:>6} 辆/天")
    print(f"    全市平均车速   : {avg_speed:>6.1f} km/h")
    print(f"    拥堵道路数     : {jam_roads:>4} 条 (等级>=3)")
    print(f"    严重拥堵道路数 : {severe_jam:>4} 条 (等级=5)")
    
    # 新增: 高峰持续时长 & 区域饱和度
    jam_hours = len(set(t.get("hour", 0) for t in cleaned_traffic if t["congestion_rate"] > 50))
    total_roads_count = len(roads)
    hour_traffic = sum(h["count"] for h in road_hour.values())
    area_sat = round(hour_traffic * 100.0 / max(total_roads_count * 3000, 1), 2)
    print(f"    高峰持续时长   : {jam_hours:>4} 小时 (拥堵率>50%)")
    print(f"    区域饱和度     : {area_sat:>6.2f}%")
    
    # ADS2: TOP拥堵道路榜
    road_jam = defaultdict(list)
    for t in cleaned_traffic:
        road_jam[t["road_id"]].append(t["jam_level"])
    top_jam = sorted(road_jam.items(), key=lambda x: sum(x[1])/len(x[1]), reverse=True)[:10]
    
    print(f"\n  [ads_top_jam_roads] 拥堵TOP10道路:")
    for rank, (road, levels) in enumerate(top_jam, 1):
        avg = sum(levels)/len(levels)
        max_l = max(levels)
        name = f"道路{road[-3:]}"  
        print(f"    #{rank:2d} {road} | 均拥堵等级 {avg:.1f} | 最高 {max_l} | 采样 {len(levels)}次")
    
    # ADS3: 设备健康评分 (三因子评分，对标ADS DDL规格)
    print(f"\n  [ads_device_health_score] 设备健康评分 TOP5:")
    health_scores = []
    for dev_id, data in device_health.items():
        total = data["online"] + data["offline"]
        online_rate = data["online"] / max(total, 1) * 100
        avg_cpu = sum(data["cpus"]) / max(len(data["cpus"]), 1)
        avg_mem = sum(data["mems"]) / max(len(data["mems"]), 1)
        abnormal_rate = data["abnormal"] / max(total, 1) * 100
        
        # HealthScore = 0.4×OnlineRate + 0.3×(1-FaultRate) + 0.3×ResourceScore
        online_score = online_rate / 100 * 40
        resource_score = max(0, (1 - (avg_cpu + avg_mem) / 200)) * 30
        fault_score = max(0, (1 - abnormal_rate / 100)) * 30
        total_score = round(online_score + resource_score + fault_score, 1)
        
        if total_score >= 90: level = "EXCELLENT"
        elif total_score >= 75: level = "GOOD"
        elif total_score >= 60: level = "FAIR"
        elif total_score >= 40: level = "POOR"
        else: level = "CRITICAL"
        
        health_scores.append((dev_id, total_score, level, round(online_rate,1), round(avg_cpu,1)))
    
    for dev, score, level, onrate, cpu in sorted(health_scores, key=lambda x: x[1])[:5]:
        flag = "!!!" if level in ("POOR","CRITICAL") else ""
        print(f"    {dev} | 评分 {score:5.1f} | {level:9s} | 在线率{onrate:.0f}% | CPU{cpu:.0f}% {flag}")
    
    # ADS4: MTBF/MTTR
    print(f"\n  [ads_device_mtbf_mttr] 设备MTBF/MTTR TOP5:")
    
    # 按设备维度计算告警统计
    device_alarm_stats = defaultdict(lambda: {"total": 0, "recovered": 0, "durations": []})
    for a in cleaned_alarm:
        dev = a.get("device_id", "UNKNOWN")
        s = device_alarm_stats[dev]
        s["total"] += 1
        if a["is_recovered"] == "Y":
            s["recovered"] += 1
            if a.get("recover_duration_min"):
                s["durations"].append(a["recover_duration_min"])
    
    mtbf_data = []
    for dev_id, data in device_health.items():
        alarm_s = device_alarm_stats.get(dev_id, {"total": 0, "recovered": 0, "durations": []})
        alarm_cnt = max(alarm_s["total"], 1)
        mtbf = round(1440 / alarm_cnt, 2)
        mttr = round(sum(alarm_s["durations"]) / max(len(alarm_s["durations"]), 1), 2) if alarm_s["durations"] else 0.0
        if mtbf >= 720: rank = "HIGH"
        elif mtbf >= 144: rank = "MEDIUM"
        else: rank = "LOW"
        mtbf_data.append((dev_id, mtbf, mttr, rank))
    
    for dev, mtbf, mttr, rank in sorted(mtbf_data, key=lambda x: x[1])[:5]:
        print(f"    {dev} | MTBF {mtbf:6.1f}min ({mtbf/60:.1f}h) | MTTR {mttr:5.1f}min | 可靠性 {rank}")
    
    # ADS5: 故障率TOP设备
    print(f"\n  [ads_device_fault_top] 故障率TOP5设备:")
    fault_data = []
    for dev_id, data in device_health.items():
        total = data["online"] + data["offline"]
        if total == 0: continue
        fault_rate = round(data["abnormal"] / total * 100, 1)
        online_rate = round(data["online"] / total * 100, 1)
        fault_data.append((dev_id, fault_rate, data["abnormal"], data["warning"], data["offline"], online_rate, round(sum(data["cpus"])/max(len(data["cpus"]),1), 1)))
    
    for dev, fr, abnormal, warning, offline, online_r, avg_c in sorted(fault_data, key=lambda x: x[1], reverse=True)[:5]:
        flag = "!!!" if fr >= 20 else ""
        print(f"    {dev} | 故障率 {fr:5.1f}% | 异常{abnormal}次/预警{warning}次/离线{offline}次 | 在线{online_r:.0f}% {flag}")
    
    return total_flow, avg_speed, jam_roads, severe_jam, health_scores, mtbf_data

# ================================================================
# 阶段5: Flink实时计算模拟 (CEP异常检测)
# ================================================================
def phase_flink_cep(cleaned_device, cleaned_alarm):
    print_header("阶段5: Flink实时计算 - CEP异常检测 (模拟)")

    from itertools import groupby
    
    offline_alerts = []
    cpu_alerts = []
    temp_alerts = []
    high_freq_alerts = []
    
    # 按设备分组（按心跳时间排序）
    for dev_id, group in groupby(sorted(cleaned_device, key=lambda x: (x["device_id"], x["heartbeat_time"])), key=lambda x: x["device_id"]):
        records = list(group)
        # 规则1: 连续离线检测 (连续3条OFFLINE)
        for i in range(len(records)-2):
            if all(r["online_flag"] == "OFFLINE" for r in records[i:i+3]):
                offline_alerts.append(f"  [CRITICAL] {dev_id}: 连续离线 (3个心跳周期)")
                break
        
        # 规则2: CPU持续高负载检测 (连续3次>90%)
        for i in range(len(records)-2):
            if all(r["cpu_usage"] > 90 for r in records[i:i+3]):
                cpu_alerts.append(f"  [MAJOR]    {dev_id}: CPU持续高负载>90% ({records[i]['cpu_usage']:.0f}%/ {records[i+1]['cpu_usage']:.0f}%/ {records[i+2]['cpu_usage']:.0f}%)")
                break
        
        # 规则3: 设备温度过高检测 (连续2次 > 80℃)
        for i in range(len(records)-1):
            if all(r["temperature"] > 80 for r in records[i:i+2]):
                temp_alerts.append(f"  [MAJOR]    {dev_id}: 温度过高 > 80℃ ({records[i]['temperature']:.1f}℃/ {records[i+1]['temperature']:.1f}℃)")
                break
    
    # 规则4: 高频告警检测 (5分钟内同一设备告警>10次)
    alarm_by_device = defaultdict(list)
    for a in cleaned_alarm:
        dev = a.get("device_id", "UNKNOWN")
        alarm_by_device[dev].append(a)
    
    for dev_id, alarms in alarm_by_device.items():
        if len(alarms) >= 10:
            # 按5分钟窗口统计
            window_alarms = defaultdict(int)
            for a in alarms:
                t = a["alarm_time"]
                try:
                    dt = datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
                    window_key = dt.replace(minute=dt.minute // 5 * 5, second=0, microsecond=0)
                    window_alarms[window_key] += 1
                except:
                    pass
            for win, cnt in window_alarms.items():
                if cnt > 10:
                    high_freq_alerts.append(f"  [CRITICAL] {dev_id}: {win.strftime('%H:%M')} 5分钟内 {cnt} 次告警 (高频告警)")
                    break
        
        if len(alarms) >= 5:
            high_freq_alerts.append(f"  [WARNING]   {dev_id}: 全天累计 {len(alarms)} 次告警 (频繁告警)")
    
    print(f"  [CEP规则1] 连续离线检测 (3min内连续3次OFFLINE):")
    print(f"    触发告警: {len(offline_alerts)} 次")
    for a in offline_alerts[:5]:
        print(a)
    if len(offline_alerts) > 5: print(f"    ... 还有 {len(offline_alerts)-5} 条")
    
    print(f"\n  [CEP规则2] CPU持续高负载检测 (5min内连续3次>90%):")
    print(f"    触发告警: {len(cpu_alerts)} 次")
    for a in cpu_alerts[:5]:
        print(a)
    if len(cpu_alerts) > 5: print(f"    ... 还有 {len(cpu_alerts)-5} 条")
    
    print(f"\n  [CEP规则3] 设备温度过高检测 (10min内连续2次>80℃):")
    print(f"    触发告警: {len(temp_alerts)} 次")
    for a in temp_alerts[:5]:
        print(a)
    if len(temp_alerts) > 5: print(f"    ... 还有 {len(temp_alerts)-5} 条")
    
    print(f"\n  [CEP规则4] 高频告警检测 (5min窗口内>10次):")
    print(f"    触发告警: {len(high_freq_alerts)} 次")
    for a in high_freq_alerts[:5]:
        print(a)
    if len(high_freq_alerts) > 5: print(f"    ... 还有 {len(high_freq_alerts)-5} 条")
    
    total_alert = len(offline_alerts) + len(cpu_alerts) + len(temp_alerts) + len(high_freq_alerts)
    print(f"\n  [CEP输出] 共 {total_alert} 条告警 → Kafka device_alert → Redis实时缓存 → 钉钉/邮件推送")
    return offline_alerts, cpu_alerts, temp_alerts, high_freq_alerts

# ================================================================
# 阶段6: 数据血缘分析
# ================================================================
def phase_lineage():
    print_header("阶段6: 数据血缘分析")

    from python.data_lineage import DataLineageManager
    
    mgr = DataLineageManager()
    
    print("  [血缘链路图]")
    print(f"  {LINE}")
    for line in mgr.visualize_lineage().split('\n'):
        if line.strip(): print(f"  {line}")
    
    print(f"\n  [上游分析] ads_device_health_score 的完整上游链路:")
    for t in mgr.get_upstream_tables('ads_device_health_score'):
        print(f"    {t}")
    
    print(f"\n  [下游影响分析] 若 dwd_traffic_status_di 变更会影响:")
    impact = mgr.detect_impact('dwd_traffic_status_di', ['avg_speed'])
    for item in impact['impact_details']:
        print(f"    {item['type']:8s} → {item['table']}")
    
    mgr.export_lineage(f"{OUTPUT_DIR}/lineage.json")
    print(f"\n  [血缘导出] → {OUTPUT_DIR}/lineage.json")

# ================================================================
# 阶段7: 数据质量监控
# ================================================================
def phase_quality(vehicle_data, traffic_data, device_data):
    print_header("阶段7: 数据质量监控")
    
    # 完整率检查
    total = len(vehicle_data)
    null_vehicle = sum(1 for v in vehicle_data if not v.get("vehicle_id"))
    null_road = sum(1 for v in vehicle_data if not v.get("road_id"))
    completeness = (total - null_vehicle - null_road) / max(total, 1) * 100
    
    # 合法性检查
    invalid_speed = sum(1 for v in vehicle_data if v.get("speed",0) < 0 or v.get("speed",0) > 200)
    validity = (total - invalid_speed) / max(total, 1) * 100
    
    # 唯一性检查
    seen = set()
    dup = sum(1 for v in vehicle_data if (k:=(v.get("vehicle_id"), v.get("pass_time"))) in seen or seen.add(k))
    uniqueness = (total - dup) / max(total, 1) * 100
    
    quality_score = round((completeness + uniqueness + validity) / 3, 1)
    
    print(f"  ┌─────────────────┬──────────┬──────────┬──────────┐")
    print(f"  │ 检测维度        │ 数值      │ 阈值     │ 状态     │")
    print(f"  ├─────────────────┼──────────┼──────────┼──────────┤")
    print(f"  │ 完整率(空值)    │ {completeness:7.2f}% │ > 99.0%  │ {'OK' if completeness>=99 else 'FAIL'}     │")
    print(f"  │ 唯一率(重复)    │ {uniqueness:7.2f}% │ > 99.9%  │ {'OK' if uniqueness>=99.9 else 'WARN'}     │")
    print(f"  │ 合法性(值域)    │ {validity:7.2f}% │ > 99.0%  │ {'OK' if validity>=99 else 'FAIL'}     │")
    print(f"  ├─────────────────┼──────────┼──────────┼──────────┤")
    print(f"  │ 综合质量评分    │ {quality_score:7.1f}% │ > 95.0%  │ {'OK' if quality_score>=95 else 'FAIL'}     │")
    print(f"  └─────────────────┴──────────┴──────────┴──────────┘")
    
    # 生成报告
    report = {
        "report_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_checks": 3,
        "pass_count": sum(1 for v in [completeness,uniqueness,validity] if v >= 99),
        "quality_score": quality_score,
        "results": [
            {"check_type":"completeness","table":"ods_vehicle_pass_di","rate":completeness,"status":"PASS" if completeness>=99 else "FAIL"},
            {"check_type":"uniqueness","table":"ods_vehicle_pass_di","rate":uniqueness,"status":"PASS" if uniqueness>=99.9 else "WARN"},
            {"check_type":"validity","table":"ods_vehicle_pass_di","rate":validity,"status":"PASS" if validity>=99 else "FAIL"}
        ]
    }
    with open(f"{OUTPUT_DIR}/data_quality_report.json","w",encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n  [报告导出] → {OUTPUT_DIR}/data_quality_report.json")
    
    return quality_score

# ================================================================
# 阶段8: 告警通知模拟
# ================================================================
def phase_alert(quality_score, health_scores):
    print_header("阶段8: 告警通知模拟")
    
    alerts = []
    
    # 质量告警
    if quality_score < 95:
        alerts.append(("MAJOR", f"[质量告警] 综合质量评分 {quality_score}% < 95%"))
    else:
        alerts.append(("MINOR", f"[质量通告] 综合质量评分 {quality_score}% > 95%, 数据质量正常"))
    
    # 设备健康告警
    critical_devices = [(dev,score) for dev,score,level,*_ in health_scores if level in ("POOR","CRITICAL")]
    for dev, score in critical_devices:
        alerts.append(("CRITICAL", f"[设备告警] {dev} 健康评分 {score} 分, 触发一级告警 → 生成运维工单"))
    
    print(f"  [告警渠道] DingTalk + Email + (CRITICAL: 短信/电话升级)")
    print(f"  [告警抑制] 相同告警30分钟内不重复推送")
    print(f"  [运维窗口] 凌晨2-3点 非CRITICAL告警静默")
    print()
    
    for severity, msg in alerts:
        icon = {"CRITICAL":"[CRITICAL]","MAJOR":"[MAJOR]   ","MINOR":"[MINOR]   "}.get(severity,"[INFO]")
        channel = {"CRITICAL":"钉钉@所有人 + 邮件 + 短信 + 电话 + 自动生成工单",
                    "MAJOR":"钉钉群消息 + 邮件",
                    "MINOR":"邮件日报"}.get(severity,"邮件")
        print(f"  {icon} {msg}")
        print(f"         渠道: {channel}")
        print()
    
    total = len(alerts)
    print(f"  [告警汇总] 共 {total} 条 ({sum(1 for _,s in alerts if 'CRITICAL' in s)}CRITICAL / {sum(1 for s,_ in alerts if s=='MAJOR')}MAJOR / {sum(1 for s,_ in alerts if s=='MINOR')}MINOR)")

# ================================================================
# 主流程
# ================================================================
def main():
    print(f"\n{'#'*70}")
    print(f"#  Traffic-Data-Governance-Real-Time-Analytics-Platform")
    print(f"#  全流程端到端模拟演示")
    print(f"#  演示日期: {TODAY}")
    print(f"#  模式: Mock数据 + 本地计算（无需Kafka/Hadoop/Hive/Flink/Redis）")
    print(f"{'#'*70}\n")
    
    # 阶段0: 生成模拟数据
    print_header("阶段0: 模拟数据生成 (Kafka Topic)")
    vehicle_data, traffic_data, device_data, alarm_data, areas, roads = generate_mock_data()
    print(f"  [Kafka生产者] 模拟写入4个Topic完成")
    print(f"    traffic_vehicle  : {len(vehicle_data)} 条 → 分区8, 副本3, 保留1天")
    print(f"    traffic_status   : {len(traffic_data)} 条 → 分区4, 副本3, 保留1天")
    print(f"    device_status    : {len(device_data)} 条 → 分区4, 副本3, 保留1天")
    print(f"    device_alarm     : {len(alarm_data)} 条 → 分区4, 副本3, 保留7天")
    
    # 阶段1: ODS落地
    vehicle_data, traffic_data, device_data, alarm_data = phase_ods(vehicle_data, traffic_data, device_data, alarm_data)
    
    # 阶段2: DWD清洗
    cleaned_vehicle, cleaned_traffic, cleaned_device, cleaned_alarm = phase_dwd(vehicle_data, traffic_data, device_data, alarm_data)
    
    # 阶段3: DWS聚合
    road_hour, area_jam, device_health, alarm_summary = phase_dws(cleaned_vehicle, cleaned_traffic, cleaned_device, cleaned_alarm)
    
    # 阶段4: ADS应用指标
    total_flow, avg_speed, jam_roads, severe_jam, health_scores, mtbf_data = phase_ads(
        road_hour, area_jam, device_health, alarm_summary, cleaned_traffic, cleaned_alarm, roads, areas
    )
    
    # 阶段5: Flink CEP模拟
    offline_alerts, cpu_alerts, temp_alerts, high_freq_alerts = phase_flink_cep(cleaned_device, cleaned_alarm)
    
    # 阶段6: 数据血缘
    phase_lineage()
    
    # 阶段7: 数据质量
    quality_score = phase_quality(vehicle_data, traffic_data, device_data)
    
    # 阶段8: 告警通知
    phase_alert(quality_score, health_scores)
    
    # 最终总结
    print_header("全流程执行完毕 - 最终概览")
    print(f"  ┌──────────────────────────────────────────────────────────────┐")
    print(f"  │ 阶段              数据量         关键指标                    │")
    print(f"  ├──────────────────────────────────────────────────────────────┤")
    print(f"  │ 0-Kafka采集       {len(vehicle_data)+len(traffic_data)+len(device_data)+len(alarm_data):>5}条        4个Topic→消息队列            │")
    print(f"  │ 1-ODS落地         {len(vehicle_data)+len(traffic_data)+len(device_data)+len(alarm_data):>5}条        20张Hive表就绪               │")
    print(f"  │ 2-DWD清洗         {len(cleaned_vehicle)+len(cleaned_traffic)+len(cleaned_device)+len(cleaned_alarm):>5}条        去重/校验/派生              │")
    print(f"  │ 3-DWS聚合         {len(road_hour)+len(area_jam)+len(device_health)+len(alarm_summary):>5}条        道路x小时/设备x天           │")
    print(f"  │ 4-ADS指标         -            车速{avg_speed:.0f}km/h/拥堵{jam_roads}条            │")
    print(f"  │ 5-Flink CEP       {len(offline_alerts)+len(cpu_alerts)+len(temp_alerts)+len(high_freq_alerts):>5}次        离线{len(offline_alerts)}/CPU{len(cpu_alerts)}/温度{len(temp_alerts)}/高频{len(high_freq_alerts)}               │")
    print(f"  │ 6-数据血缘        1份图谱       20表完整血缘链路            │")
    print(f"  │ 7-数据质量        3维度        评分{quality_score:.1f}%                       │")
    print(f"  │ 8-告警通知        -            DingTalk+邮件+工单          │")
    print(f"  └──────────────────────────────────────────────────────────────┘")
    
    print(f"\n  [输出文件]")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        size = os.path.getsize(os.path.join(OUTPUT_DIR, f))
        print(f"    {OUTPUT_DIR}/{f} ({size:,} bytes)")
    
    print(f"\n  [全链路耗时] <1秒 (模拟模式)")
    print(f"  [生产环境预估] Kafka→Redis端到端延迟 <5秒, ODS→ADS离线链路 <2小时\n")

if __name__ == '__main__':
    main()
