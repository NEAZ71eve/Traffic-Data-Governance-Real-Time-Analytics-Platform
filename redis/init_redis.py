#!/usr/bin/env python3
# ============================================
# Redis 初始化脚本
# 创建实时数据缓存结构并加载初始数据
# 用法：python init_redis.py
# ============================================

import random
import time
from datetime import datetime

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


def init_redis(host='localhost', port=6379):
    """初始化 Redis 数据结构"""
    if not REDIS_AVAILABLE:
        print("[演示模式] redis-py 未安装，输出预期的Redis命令")
        _print_init_commands()
        return
    
    r = redis.Redis(host=host, port=port, decode_responses=True)
    print(f"连接到 Redis: {host}:{port}")
    
    # 1. 城市级概览
    r.hset('traffic:city:overview', mapping={
        'total_online_vehicles': 12500,
        'avg_speed_city': 45,
        'jam_road_count': 8,
        'severe_jam_count': 2,
    })
    r.expire('traffic:city:overview', 300)
    
    # 2. 各道路实时车流
    roads = {
        'R001': {'name': '长安街', 'flow': 856, 'speed': 42},
        'R002': {'name': '东三环路', 'flow': 720, 'speed': 35},
        'R003': {'name': '西三环路', 'flow': 580, 'speed': 48},
        'R004': {'name': '建国路', 'flow': 650, 'speed': 32},
        'R005': {'name': '朝阳北路', 'flow': 320, 'speed': 55},
        'R006': {'name': '中关村大街', 'flow': 690, 'speed': 38},
        'R007': {'name': '学院路', 'flow': 280, 'speed': 52},
        'R008': {'name': '知春路', 'flow': 240, 'speed': 58},
        'R009': {'name': '世纪大道', 'flow': 780, 'speed': 30},
        'R010': {'name': '张江路', 'flow': 310, 'speed': 50},
        'R011': {'name': '天河路', 'flow': 620, 'speed': 40},
        'R012': {'name': '中山大道', 'flow': 550, 'speed': 44},
        'R013': {'name': '深南大道', 'flow': 920, 'speed': 25},
        'R014': {'name': '科技园路', 'flow': 350, 'speed': 56},
        'R015': {'name': '滨海大道', 'flow': 680, 'speed': 36},
    }
    
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for road_id, info in roads.items():
        r.hset(f'traffic:vehicle:{road_id}', mapping={
            'road_name': info['name'],
            'traffic_count': info['flow'] + random.randint(-50, 50),
            'avg_speed': info['speed'] + random.randint(-5, 5),
            'window_start': now,
            'update_time': now,
        })
        r.expire(f'traffic:vehicle:{road_id}', 600)
        
        # 拥堵指数
        jam_level = 1 if info['speed'] > 55 else 2 if info['speed'] > 40 else 3 if info['speed'] > 30 else 4 if info['speed'] > 20 else 5
        r.hset(f'traffic:congestion:{road_id}', mapping={
            'jam_level': jam_level,
            'congestion_rate': random.randint(100 - info['speed'], min(95, 100 - info['speed'] + 15)),
            'avg_speed': info['speed'],
            'traffic_flow': info['flow'],
            'update_time': now,
        })
        r.expire(f'traffic:congestion:{road_id}', 600)
        
        # TOP 拥堵 (ZSET)
        r.zadd('traffic:top_jam_roads', {road_id: 100 - info['speed']})
    
    r.expire('traffic:top_jam_roads', 600)
    
    # 3. 设备状态
    devices = {
        'D001': {'name': '长安街卡口1号', 'status': 'ONLINE', 'cpu': 45, 'mem': 62, 'temp': 38},
        'D002': {'name': '长安街卡口2号', 'status': 'ONLINE', 'cpu': 52, 'mem': 70, 'temp': 42},
        'D003': {'name': '东三环雷达1号', 'status': 'ONLINE', 'cpu': 88, 'mem': 85, 'temp': 55},
        'D004': {'name': '西三环地磁传感器1号', 'status': 'ONLINE', 'cpu': 12, 'mem': 35, 'temp': 28},
        'D005': {'name': '建国路摄像头1号', 'status': 'ONLINE', 'cpu': 38, 'mem': 55, 'temp': 35},
        'D006': {'name': '朝阳北路红绿灯1号', 'status': 'ONLINE', 'cpu': 20, 'mem': 40, 'temp': 30},
        'D007': {'name': '中关村卡口1号', 'status': 'ONLINE', 'cpu': 48, 'mem': 60, 'temp': 40},
        'D008': {'name': '中关村摄像头1号', 'status': 'MAINTENANCE', 'cpu': 0, 'mem': 0, 'temp': 25},
        'D009': {'name': '学院路地磁传感器1号', 'status': 'ONLINE', 'cpu': 15, 'mem': 38, 'temp': 29},
        'D010': {'name': '世纪大道雷达1号', 'status': 'ONLINE', 'cpu': 42, 'mem': 58, 'temp': 44},
        'D011': {'name': '世纪大道卡口1号', 'status': 'ONLINE', 'cpu': 50, 'mem': 65, 'temp': 39},
        'D012': {'name': '张江路摄像头1号', 'status': 'OFFLINE', 'cpu': 0, 'mem': 0, 'temp': 26},
        'D013': {'name': '天河路卡口1号', 'status': 'ONLINE', 'cpu': 55, 'mem': 72, 'temp': 46},
        'D014': {'name': '天河路雷达1号', 'status': 'ONLINE', 'cpu': 40, 'mem': 56, 'temp': 42},
        'D015': {'name': '深南大道卡口1号', 'status': 'ONLINE', 'cpu': 60, 'mem': 78, 'temp': 48},
        'D016': {'name': '深南大道卡口2号', 'status': 'ONLINE', 'cpu': 58, 'mem': 75, 'temp': 47},
        'D017': {'name': '科技园路摄像头1号', 'status': 'ONLINE', 'cpu': 35, 'mem': 50, 'temp': 33},
        'D018': {'name': '滨海大道雷达1号', 'status': 'ONLINE', 'cpu': 44, 'mem': 60, 'temp': 41},
        'D019': {'name': '滨海大道红绿灯1号', 'status': 'ONLINE', 'cpu': 22, 'mem': 42, 'temp': 31},
        'D020': {'name': '中山大道卡口1号', 'status': 'ONLINE', 'cpu': 46, 'mem': 63, 'temp': 40},
    }
    
    for dev_id, info in devices.items():
        health = 'NORMAL'
        if info['status'] == 'OFFLINE':
            health = 'ABNORMAL'
        elif info['cpu'] > 85 or info['temp'] > 80:
            health = 'WARNING'
        
        r.hset(f'device:status:{dev_id}', mapping={
            'device_name': info['name'],
            'online_flag': info['status'],
            'cpu_usage': info['cpu'],
            'memory_usage': info['mem'],
            'temperature': info['temp'],
            'health_flag': health,
            'last_heartbeat': now,
        })
        r.expire(f'device:status:{dev_id}', 300)
    
    # 4. 数据质量状态
    r.hset('quality:status', mapping={
        'completeness_rate': 99.3,
        'uniqueness_rate': 99.8,
        'validity_rate': 98.7,
        'timeliness_rate': 97.5,
        'kafka_lag_total': 3500,
        'last_check_time': now,
    })
    r.expire('quality:status', 3600)
    
    print("Redis 初始化完成!")
    print(f"  - 15 条道路实时车流")
    print(f"  - 20 台设备状态")
    print(f"  - 数据质量状态")
    print(f"  - 城市实时概览")


def _print_init_commands():
    """打印等效的 Redis CLI 命令"""
    print("=" * 50)
    print("Redis 初始化命令 (可直接粘贴到 redis-cli)")
    print("=" * 50)
    print("""
# 城市概览
HSET traffic:city:overview total_online_vehicles 12500 avg_speed_city 45 jam_road_count 8 severe_jam_count 2
EXPIRE traffic:city:overview 300

# 实时车流 (示例)
HSET traffic:vehicle:R001 road_name "长安街" traffic_count 856 avg_speed 42 window_start "2026-06-08 08:00:00" update_time "2026-06-08 08:00:00"
EXPIRE traffic:vehicle:R001 600

HSET traffic:vehicle:R013 road_name "深南大道" traffic_count 920 avg_speed 25 window_start "2026-06-08 08:00:00" update_time "2026-06-08 08:00:00"
EXPIRE traffic:vehicle:R013 600

# 设备状态 (示例)
HSET device:status:D001 device_name "长安街卡口1号" online_flag "ONLINE" cpu_usage 45 memory_usage 62 temperature 38 health_flag "NORMAL" last_heartbeat "2026-06-08 08:00:00"
EXPIRE device:status:D001 300

# 数据质量
HSET quality:status completeness_rate 99.3 uniqueness_rate 99.8 validity_rate 98.7 timeliness_rate 97.5 kafka_lag_total 3500
EXPIRE quality:status 3600

# TOP 拥堵道路
ZADD traffic:top_jam_roads 78 R001 92 R013 65 R002
EXPIRE traffic:top_jam_roads 600
""")


if __name__ == '__main__':
    init_redis()
