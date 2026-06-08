#!/usr/bin/env python3
# ============================================
# Redis 数据查询 API 脚本
# 用途：供前端看板/监控系统拉取实时指标
# 依赖：pip install redis
# ============================================

import json
import time
from typing import Dict, List, Optional

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("[WARN] redis-py 未安装，使用模拟数据。pip install redis")


class RedisTrafficAPI:
    """交通数据Redis查询接口"""
    
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.use_mock = not REDIS_AVAILABLE
        if not self.use_mock:
            self.client = redis.Redis(
                host=host, port=port, db=db, password=password,
                decode_responses=True, socket_connect_timeout=3
            )
    
    def get_city_overview(self) -> dict:
        """获取城市级实时概览"""
        if self.use_mock:
            return self._mock_city_overview()
        data = self.client.hgetall('traffic:city:overview')
        return {
            'total_online_vehicles': int(data.get('total_online_vehicles', 0)),
            'avg_speed_city': float(data.get('avg_speed_city', 0)),
            'jam_road_count': int(data.get('jam_road_count', 0)),
            'severe_jam_count': int(data.get('severe_jam_count', 0)),
        }
    
    def get_vehicle_count(self, road_id: str) -> dict:
        """获取指定道路实时车流"""
        if self.use_mock:
            return self._mock_vehicle_count(road_id)
        data = self.client.hgetall(f'traffic:vehicle:{road_id}')
        if not data:
            return None
        return {
            'road_name': data.get('road_name', ''),
            'traffic_count': int(data.get('traffic_count', 0)),
            'avg_speed': float(data.get('avg_speed', 0)),
            'window_start': data.get('window_start', ''),
        }
    
    def get_all_vehicle_counts(self) -> List[dict]:
        """获取所有道路实时车流"""
        results = []
        for key in self.client.scan_iter('traffic:vehicle:*'):
            road_id = key.split(':')[-1]
            data = self.get_vehicle_count(road_id)
            if data:
                data['road_id'] = road_id
                results.append(data)
        return sorted(results, key=lambda x: x['traffic_count'], reverse=True)
    
    def get_congestion(self, road_id: str) -> dict:
        """获取指定道路拥堵详情"""
        if self.use_mock:
            return self._mock_congestion(road_id)
        data = self.client.hgetall(f'traffic:congestion:{road_id}')
        if not data:
            return None
        return {
            'jam_level': int(data.get('jam_level', 1)),
            'congestion_rate': float(data.get('congestion_rate', 0)),
            'avg_speed': float(data.get('avg_speed', 0)),
            'traffic_flow': int(data.get('traffic_flow', 0)),
        }
    
    def get_top_jam_roads(self, n: int = 10) -> List[dict]:
        """获取TOP N拥堵道路"""
        if self.use_mock:
            return self._mock_top_jam(n)
        results = self.client.zrevrange('traffic:top_jam_roads', 0, n-1, withscores=True)
        return [{'road_id': r[0], 'congestion_rate': r[1]} for r in results]
    
    def get_device_status(self, device_id: str) -> dict:
        """获取设备实时状态"""
        if self.use_mock:
            return self._mock_device(device_id)
        data = self.client.hgetall(f'device:status:{device_id}')
        if not data:
            return None
        return {
            'device_name': data.get('device_name', ''),
            'online_flag': data.get('online_flag', 'OFFLINE'),
            'cpu_usage': float(data.get('cpu_usage', 0)),
            'memory_usage': float(data.get('memory_usage', 0)),
            'temperature': float(data.get('temperature', 0)),
            'health_flag': data.get('health_flag', 'NORMAL'),
            'last_heartbeat': data.get('last_heartbeat', ''),
        }
    
    def get_all_device_statuses(self) -> List[dict]:
        """获取所有设备实时状态"""
        results = []
        for key in self.client.scan_iter('device:status:*'):
            device_id = key.split(':')[-1]
            data = self.get_device_status(device_id)
            if data:
                data['device_id'] = device_id
                results.append(data)
        return results
    
    def get_cep_alerts(self, limit: int = 20) -> List[dict]:
        """获取CEP异常告警列表"""
        if self.use_mock:
            return self._mock_alerts(limit)
        results = []
        for key in self.client.scan_iter('alert:cep:*'):
            data = self.client.hgetall(key)
            if data:
                results.append(dict(data))
        return sorted(results, key=lambda x: x.get('alert_time', ''), reverse=True)[:limit]
    
    def get_quality_status(self) -> dict:
        """获取数据质量状态"""
        if self.use_mock:
            return self._mock_quality()
        data = self.client.hgetall('quality:status')
        return {
            'completeness_rate': float(data.get('completeness_rate', 0)),
            'uniqueness_rate': float(data.get('uniqueness_rate', 0)),
            'validity_rate': float(data.get('validity_rate', 0)),
            'timeliness_rate': float(data.get('timeliness_rate', 0)),
            'kafka_lag_total': int(data.get('kafka_lag_total', 0)),
            'last_check_time': data.get('last_check_time', ''),
        }
    
    def get_dashboard_data(self) -> dict:
        """聚合返回看板所需全部数据"""
        return {
            'city_overview': self.get_city_overview(),
            'top_jam_roads': self.get_top_jam_roads(10),
            'routes': self.get_all_vehicle_counts(),
            'alerts': self.get_cep_alerts(10),
            'quality': self.get_quality_status(),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        }
    
    # ---- Mock data generators for offline demo ----
    def _mock_city_overview(self):
        import random
        return {
            'total_online_vehicles': random.randint(8000, 15000),
            'avg_speed_city': random.randint(40, 55),
            'jam_road_count': random.randint(4, 12),
            'severe_jam_count': random.randint(1, 3),
        }
    
    def _mock_vehicle_count(self, road_id):
        import random
        names = {'R001': '长安街', 'R002': '东三环路', 'R006': '中关村大街', 'R013': '深南大道'}
        return {
            'road_name': names.get(road_id, f'道路{road_id}'),
            'traffic_count': random.randint(300, 1200),
            'avg_speed': random.randint(20, 68),
            'window_start': time.strftime('%Y-%m-%d %H:05:00'),
        }
    
    def _mock_congestion(self, road_id):
        import random
        return {
            'jam_level': random.randint(1, 5),
            'congestion_rate': random.randint(10, 95),
            'avg_speed': random.randint(15, 65),
            'traffic_flow': random.randint(200, 1000),
        }
    
    def _mock_top_jam(self, n):
        import random
        roads = ['R001', 'R013', 'R002', 'R006', 'R011', 'R004', 'R009', 'R012', 'R003', 'R015']
        return [{'road_id': r, 'congestion_rate': random.randint(50, 95)} for r in roads[:n]]
    
    def _mock_device(self, device_id):
        import random
        return {
            'device_name': f'设备{device_id}',
            'online_flag': random.choice(['ONLINE', 'ONLINE', 'ONLINE', 'OFFLINE']),
            'cpu_usage': random.randint(10, 95),
            'memory_usage': random.randint(20, 90),
            'temperature': random.randint(25, 85),
            'health_flag': random.choice(['NORMAL', 'NORMAL', 'NORMAL', 'WARNING', 'ABNORMAL']),
            'last_heartbeat': time.strftime('%Y-%m-%d %H:%M:%S'),
        }
    
    def _mock_alerts(self, limit):
        return [
            {'alert_id': '2026-06-08_001', 'alert_type': 'OFFLINE_CONTINUOUS', 'device_id': 'D012', 'alert_level': 'CRITICAL', 'alert_time': '2026-06-08 07:30:00', 'detail': '设备D012连续3次心跳超时'},
            {'alert_id': '2026-06-08_002', 'alert_type': 'CPU_HIGH', 'device_id': 'D003', 'alert_level': 'MAJOR', 'alert_time': '2026-06-08 08:15:00', 'detail': '设备D003 CPU持续>90%'},
        ][:limit]
    
    def _mock_quality(self):
        import random
        return {
            'completeness_rate': round(random.uniform(98.5, 99.9), 2),
            'uniqueness_rate': round(random.uniform(99.5, 100), 2),
            'validity_rate': round(random.uniform(97.5, 99.5), 2),
            'timeliness_rate': round(random.uniform(96.0, 99.0), 2),
            'kafka_lag_total': random.randint(500, 9500),
            'last_check_time': time.strftime('%Y-%m-%d %H:%M:%S'),
        }


if __name__ == '__main__':
    api = RedisTrafficAPI()
    data = api.get_dashboard_data()
    print(json.dumps(data, ensure_ascii=False, indent=2))
