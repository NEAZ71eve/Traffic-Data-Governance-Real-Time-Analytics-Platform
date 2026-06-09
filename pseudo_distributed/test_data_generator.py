#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一模拟交通数据生成器
生成7天4类交通数据，支持JSON/CSV/控制台输出，零外部依赖。
用法: python test_data_generator.py --days 7 --output csv --dir data/
"""

import argparse
import csv
import json
import os
import random
import sys
from datetime import datetime, timedelta


# ==============================================================================
# 常量配置
# ==============================================================================

ROAD_NAMES = [
    "中山路", "人民路", "建设路", "解放路", "和平路",
    "长安街", "南京路", "北京路", "天府大道", "滨海大道",
    "环城路", "迎宾路", "学府路", "科技路", "工业大道",
    "东风路", "西环路", "南环路", "北环路", "东环路",
    "西湖大道", "太湖路", "黄山大道", "长江路", "珠江路",
    "火车站路", "机场路", "港口路", "商贸路", "文化路",
]

DEVICE_PREFIXES = ["DEV", "CAM", "SEN", "RSU", "VMS"]

VEHICLE_TYPES = ["car", "bus", "truck", "motorcycle"]

ALARM_TYPES = [
    ("设备离线", "low"), ("CPU过载", "medium"), ("内存不足", "medium"),
    ("温度过高", "high"), ("网络延迟", "low"), ("数据异常", "high"),
    ("存储满", "medium"), ("电源故障", "critical"), ("传感器失效", "high"),
    ("通讯中断", "critical"),
]

PLATE_PREFIXES = [
    "京", "津", "沪", "渝", "冀", "豫", "云", "辽", "黑", "湘",
    "皖", "鲁", "新", "苏", "浙", "赣", "鄂", "桂", "甘", "晋",
    "蒙", "陕", "吉", "闽", "贵", "粤", "青", "藏", "川", "宁", "琼",
]

CHINESE_CHARS = "的一是在不了有和人这中大为上个国我以要他时来用们生到作地于出就分对成会可主发年动同工也能下过子说产种面而方后多定行学法所民得经十三之进着等部度家电力里如水化高自二理起小物现实加量都两体制机当使点从业本去把性好应开它合还因由其些然前外天政四日那社义事平形相全表间样与关各重新线内数正心反你明看原又么利比或但质气第向道命此变条只没结解问意建月公无系军很情者最立代想已通并提直题党程展五果料象员革位入常文总次品式活设及管特件长求老头基资边流路级少图山统接知较将组见计别她手角期根论运农指几九区强放决西被干做必战先回则任取据处队南给色光门即保治北造百规热领七海口东导器压志世金增争济阶油思术极交受联什认六共权收证改清己美再采转更单风切打白教速花带安场身车例真务具万每目至达走积示议声报斗完类八离华名确才科张信马节话米整空元况今集温传土许步群广石记需段研界拉林律叫且究观越织装影算低持音众书布复容儿须际商非验连断深难近矿千周委素技备半办青省列习响约支般史感劳便团往酸历市克何除消构府称太准精值号率族维划选标写存候毛亲快效斯院查江型眼王按格养易置派层片始却专状育厂京识适属圆包火住调满县局照参红细引听该铁价严龙飞"


# ==============================================================================
# 工具函数
# ==============================================================================

def generate_plate():
    """生成中国车牌号"""
    prefix = random.choice(PLATE_PREFIXES)
    letter = random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ")
    digits = "".join(str(random.randint(0, 9)) for _ in range(5))
    return f"{prefix}{letter}{digits}"


def generate_device_id():
    """生成设备ID"""
    prefix = random.choice(DEVICE_PREFIXES)
    suffix = f"{random.randint(1000, 9999):04d}"
    return f"{prefix}-{suffix}"


def generate_road_id():
    """生成路段ID"""
    return f"RD-{random.randint(10000, 99999):05d}"


def generate_description(alarm_type, alarm_level):
    """生成告警描述"""
    templates = {
        "设备离线": "设备检测到通信中断，已超过{}分钟未收到心跳信号",
        "CPU过载": "CPU使用率持续超过{}%，可能影响设备正常运行",
        "内存不足": "内存使用率达到{}%，接近物理内存上限",
        "温度过高": "设备温度达到{}°C，超过正常工作温度范围",
        "网络延迟": "网络延迟达到{}ms，数据传输可能出现丢包",
        "数据异常": "检测到异常数据波动，数值偏离正常范围{}%",
        "存储满": "磁盘使用率超过{}%，存储空间即将耗尽",
        "电源故障": "主电源异常，已切换至备用电源，预计续航{}小时",
        "传感器失效": "传感器{}连续{}次数据采集失败",
        "通讯中断": "与中心服务器通信中断，最后心跳时间{}分钟前",
    }
    template = templates.get(alarm_type, "发生异常告警")
    values = {
        "设备离线": (random.randint(5, 60),),
        "CPU过载": (random.randint(85, 99),),
        "内存不足": (random.randint(85, 99),),
        "温度过高": (random.randint(70, 95),),
        "网络延迟": (random.randint(500, 3000),),
        "数据异常": (random.randint(20, 80),),
        "存储满": (random.randint(85, 99),),
        "电源故障": (random.randint(1, 8),),
        "传感器失效": (random.choice(["A相", "B相", "C相", "主传感器"]), random.randint(3, 10)),
        "通讯中断": (random.randint(5, 120),),
    }
    return template.format(*values.get(alarm_type, ()))


# ==============================================================================
# 数据生成器
# ==============================================================================

class TrafficDataGenerator:
    """交通数据生成器"""

    def __init__(self, days=7, base_date=None):
        self.days = days
        self.base_date = base_date or datetime(2025, 6, 1)
        self.total_records = 100000

        # 数据量分配
        days_factor = max(days, 1)
        self.counts = {
            "vehicle_pass": int(50000 * days_factor / 7),
            "traffic_status": int(20000 * days_factor / 7),
            "device_status": int(20000 * days_factor / 7),
            "alarm_log": int(10000 * days_factor / 7),
        }

        # 预生成静态数据
        self.road_ids = [generate_road_id() for _ in range(50)]
        self.device_ids = [generate_device_id() for _ in range(30)]
        self.plates = [generate_plate() for _ in range(200)]

    def _random_time(self, day_offset):
        """生成指定日期的随机时间，考虑高峰时段权重"""
        hour = random.choices(
            range(24),
            weights=[
                0.5, 0.3, 0.2, 0.2, 0.3, 0.5,   # 0-5
                2.0, 3.0, 3.0, 1.0,               # 6-9 (peak 7-9)
                0.8, 0.8, 0.6, 0.6, 0.8, 0.8,     # 10-15
                1.0, 3.0, 3.0, 1.5,               # 16-19 (peak 17-19)
                1.0, 0.8, 0.5, 0.3,               # 20-23
            ],
            k=1
        )[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        dt = self.base_date + timedelta(days=day_offset, hours=hour,
                                         minutes=minute, seconds=second)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _is_peak_hour(self, dt_str):
        """判断是否为高峰时段 (7-9AM, 5-7PM)"""
        try:
            hour = int(dt_str.split(" ")[1].split(":")[0])
            return (7 <= hour <= 8) or (17 <= hour <= 18)
        except (ValueError, IndexError):
            return False

    def generate_vehicle_pass(self):
        """生成车辆通行记录"""
        records = []
        n = self.counts["vehicle_pass"]
        for _ in range(n):
            day = random.randint(0, self.days - 1)
            pass_time = self._random_time(day)
            is_peak = self._is_peak_hour(pass_time)
            if is_peak:
                speed = random.randint(15, 55)  # 高峰低速
            else:
                speed = random.randint(40, 120)
            records.append({
                "road_id": random.choice(self.road_ids),
                "plate": random.choice(self.plates),
                "pass_time": pass_time,
                "speed_kmh": speed,
                "vehicle_type": random.choice(VEHICLE_TYPES),
                "lane_no": random.randint(1, 4),
            })
        return records

    def generate_traffic_status(self):
        """生成交通状态记录"""
        records = []
        n = self.counts["traffic_status"]
        for _ in range(n):
            day = random.randint(0, self.days - 1)
            record_time = self._random_time(day)
            is_peak = self._is_peak_hour(record_time)
            if is_peak:
                avg_speed = random.randint(10, 40)
                traffic_flow = random.randint(300, 1500)  # 3x
                jam_level = random.randint(3, 5)
            else:
                avg_speed = random.randint(30, 90)
                traffic_flow = random.randint(50, 500)
                jam_level = random.randint(1, 3)
            congestion_index = round(random.uniform(0.0, 10.0), 2)
            if is_peak:
                congestion_index = round(congestion_index * 2.0, 2) if congestion_index < 5 else congestion_index
            records.append({
                "road_id": random.choice(self.road_ids),
                "record_time": record_time,
                "avg_speed": round(avg_speed + random.uniform(-5, 5), 1),
                "traffic_flow": traffic_flow + random.randint(-50, 50),
                "jam_level": jam_level,
                "congestion_index": congestion_index,
            })
        return records

    def generate_device_status(self):
        """生成设备状态记录"""
        records = []
        n = self.counts["device_status"]
        for _ in range(n):
            day = random.randint(0, self.days - 1)
            record_time = self._random_time(day)
            records.append({
                "device_id": random.choice(self.device_ids),
                "record_time": record_time,
                "cpu_usage_percent": round(random.uniform(5.0, 95.0), 1),
                "memory_usage_percent": round(random.uniform(10.0, 95.0), 1),
                "temperature_celsius": round(random.uniform(25.0, 85.0), 1),
                "online_flag": random.choices([True, False], weights=[95, 5])[0],
            })
        return records

    def generate_alarm_log(self):
        """生成告警日志"""
        records = []
        n = self.counts["alarm_log"]
        for _ in range(n):
            day = random.randint(0, self.days - 1)
            alarm_time = self._random_time(day)
            alarm_type, alarm_level = random.choice(ALARM_TYPES)
            records.append({
                "device_id": random.choice(self.device_ids),
                "alarm_time": alarm_time,
                "alarm_type": alarm_type,
                "alarm_level": alarm_level,
                "description": generate_description(alarm_type, alarm_level),
            })
        return records

    def generate_all(self):
        """生成全部数据"""
        print(f"正在生成 {self.days} 天模拟交通数据...")
        data = {}
        for dtype, gen_func in [
            ("vehicle_pass", self.generate_vehicle_pass),
            ("traffic_status", self.generate_traffic_status),
            ("device_status", self.generate_device_status),
            ("alarm_log", self.generate_alarm_log),
        ]:
            print(f"  生成 {dtype} ({self.counts[dtype]} 条)...")
            data[dtype] = gen_func()
        total = sum(len(v) for v in data.values())
        print(f"数据生成完成，共 {total} 条记录。")
        return data


# ==============================================================================
# 输出模块
# ==============================================================================

def output_csv(data, output_dir):
    """输出为CSV文件"""
    os.makedirs(output_dir, exist_ok=True)
    for dtype, records in data.items():
        if not records:
            continue
        fpath = os.path.join(output_dir, f"{dtype}.csv")
        with open(fpath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
        print(f"  CSV已保存: {fpath} ({len(records)} 条)")
    print(f"\n所有CSV文件已输出到: {os.path.abspath(output_dir)}")


def output_json(data, output_dir):
    """输出为JSON文件"""
    os.makedirs(output_dir, exist_ok=True)
    for dtype, records in data.items():
        if not records:
            continue
        fpath = os.path.join(output_dir, f"{dtype}.json")
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        print(f"  JSON已保存: {fpath} ({len(records)} 条)")
    print(f"\n所有JSON文件已输出到: {os.path.abspath(output_dir)}")


def output_console(data):
    """输出到控制台(预览)"""
    for dtype, records in data.items():
        print(f"\n{'='*60}")
        print(f"  类型: {dtype} (共 {len(records)} 条)")
        print(f"{'='*60}")
        if not records:
            print("  (无数据)")
            continue
        # 打印前5条
        for i, rec in enumerate(records[:5]):
            print(f"  [{i+1}] {rec}")
        if len(records) > 5:
            print(f"  ... (还有 {len(records) - 5} 条未显示)")


# ==============================================================================
# 主入口
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="统一模拟交通数据生成器 - 生成车辆通行/交通状态/设备状态/告警日志数据"
    )
    parser.add_argument("--days", type=int, default=7,
                        help="生成天数 (默认: 7)")
    parser.add_argument("--output", type=str, default="csv",
                        choices=["json", "csv", "console"],
                        help="输出模式: json, csv, console (默认: csv)")
    parser.add_argument("--dir", type=str, default="data",
                        help="输出目录 (默认: data/)")
    args = parser.parse_args()

    gen = TrafficDataGenerator(days=args.days)
    data = gen.generate_all()

    total = sum(len(v) for v in data.values())
    print(f"\n总计生成 {total} 条记录 ({args.days} 天)")

    if args.output == "csv":
        output_csv(data, args.dir)
    elif args.output == "json":
        output_json(data, args.dir)
    elif args.output == "console":
        output_console(data)


if __name__ == "__main__":
    main()
