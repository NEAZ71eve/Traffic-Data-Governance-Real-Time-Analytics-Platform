#!/usr/bin/env python3
# ============================================
# AI 数据异常检测助手 (增强版)
# 技术：Isolation Forest + 统计方法
# ============================================

import json
import math
import random
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict, Tuple


class IsolationForest:
    """
    简化版 Isolation Forest 实现
    用于交通时序数据异常检测
    核心原理：异常点更容易被随机划分隔离
    """
    
    def __init__(self, n_estimators: int = 100, max_samples: int = 256, contamination: float = 0.05):
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.contamination = contamination
        self.trees = []
        self.threshold = None
    
    class _ITree:
        """隔离树节点"""
        def __init__(self, size, left=None, right=None, split_attr=None, split_value=None):
            self.size = size
            self.left = left
            self.right = right
            self.split_attr = split_attr
            self.split_value = split_value
        
        @property
        def is_leaf(self):
            return self.left is None and self.right is None
    
    def _build_tree(self, X: List[List[float]], depth: int = 0, max_depth: int = None) -> _ITree:
        """递归构建隔离树"""
        if max_depth is None:
            max_depth = int(math.log2(len(X))) + 1
        
        n = len(X)
        if depth >= max_depth or n <= 1:
            return self._ITree(size=n)
        
        # 随机选特征
        n_features = len(X[0])
        attr = random.randint(0, n_features - 1)
        
        # 取该特征的值范围
        values = [x[attr] for x in X]
        min_val, max_val = min(values), max(values)
        
        if min_val == max_val:
            return self._ITree(size=n)
        
        # 随机分割点
        split_val = random.uniform(min_val, max_val)
        
        left_X = [x for x in X if x[attr] < split_val]
        right_X = [x for x in X if x[attr] >= split_val]
        
        return self._ITree(
            size=n,
            left=self._build_tree(left_X, depth + 1, max_depth),
            right=self._build_tree(right_X, depth + 1, max_depth),
            split_attr=attr,
            split_value=split_val
        )
    
    def _path_length(self, x: List[float], tree: _ITree, depth: int = 0) -> float:
        """计算样本在树中的路径长度"""
        if tree.is_leaf:
            # 平均路径修正 (Harmonic number approximation)
            if tree.size <= 1:
                return depth
            return depth + 2 * (math.log(tree.size - 1) + 0.5772156649) - 2 * (tree.size - 1) / tree.size
        
        if x[tree.split_attr] < tree.split_value:
            return self._path_length(x, tree.left, depth + 1)
        else:
            return self._path_length(x, tree.right, depth + 1)
    
    def fit(self, X: List[List[float]]):
        """训练 Isolation Forest"""
        n_samples = min(self.max_samples, len(X))
        self.trees = []
        for _ in range(self.n_estimators):
            sample = random.sample(X, n_samples)
            tree = self._build_tree(sample)
            self.trees.append(tree)
        
        # 计算异常分数阈值
        scores = [self._anomaly_score(x) for x in X]
        scores.sort(reverse=True)
        k = max(1, int(len(X) * self.contamination))
        self.threshold = scores[k - 1] if k <= len(scores) else scores[-1]
    
    def _anomaly_score(self, x: List[float]) -> float:
        """计算异常分数 (越高越异常)"""
        path_lengths = [self._path_length(x, tree) for tree in self.trees]
        avg_path = sum(path_lengths) / len(path_lengths)
        # 归一化
        c = 2 * (math.log(self.max_samples - 1) + 0.5772156649) - 2 * (self.max_samples - 1) / self.max_samples
        return 2 ** (-avg_path / c)
    
    def predict(self, X: List[List[float]]) -> List[int]:
        """预测: 1=正常, -1=异常"""
        scores = [self._anomaly_score(x) for x in X]
        return [-1 if s > self.threshold else 1 for s in scores]
    
    def score_samples(self, X: List[List[float]]) -> List[float]:
        """返回异常分数"""
        return [self._anomaly_score(x) for x in X]


class TrafficAnomalyDetector:
    """交通数据异常检测助手"""
    
    def __init__(self):
        self.flow_model = None
        self.speed_model = None
        self.device_model = None
    
    # ========== 1. 车流量异常检测 ==========
    def detect_flow_anomaly(self, hourly_data: List[Dict], contamination: float = 0.05) -> List[Dict]:
        """
        检测车流量异常
        hourly_data: [{"hour": 8, "flow": 1200, "road_id": "R001"}, ...]
        """
        if len(hourly_data) < 24:
            return []
        
        # 特征工程：车流量 + 时段one-hot
        features = []
        for d in hourly_data:
            feat = [
                float(d['flow']),
                float(d['hour']),
                1.0 if 7 <= d['hour'] <= 9 else 0.0,   # 早高峰
                1.0 if 17 <= d['hour'] <= 19 else 0.0,  # 晚高峰
            ]
            features.append(feat)
        
        model = IsolationForest(contamination=contamination)
        model.fit(features)
        scores = model.score_samples(features)
        
        results = []
        for i, d in enumerate(hourly_data):
            is_anomaly = scores[i] > model.threshold
            results.append({
                **d,
                'anomaly_score': round(scores[i], 4),
                'is_anomaly': is_anomaly,
                'anomaly_type': '流量异常' if is_anomaly else '正常',
            })
        return results
    
    # ========== 2. 设备状态异常检测 ==========
    def detect_device_anomaly(self, device_data: List[Dict], contamination: float = 0.05) -> List[Dict]:
        """
        检测设备状态异常
        device_data: [{"device_id": "D001", "cpu": 45, "mem": 62, "temp": 38}, ...]
        """
        if len(device_data) < 10:
            return []
        
        features = []
        for d in device_data:
            feat = [float(d.get('cpu', 0)), float(d.get('mem', 0)), float(d.get('temp', 0))]
            features.append(feat)
        
        model = IsolationForest(contamination=contamination)
        model.fit(features)
        scores = model.score_samples(features)
        
        results = []
        for i, d in enumerate(device_data):
            is_anomaly = scores[i] > model.threshold
            # 判断异常类型
            a_type = '正常'
            if is_anomaly:
                cpu, mem, temp = float(d.get('cpu', 0)), float(d.get('mem', 0)), float(d.get('temp', 0))
                if temp > 80:
                    a_type = '温度过高'
                elif cpu > 90:
                    a_type = 'CPU高负载'
                elif mem > 90:
                    a_type = '内存高负载'
                else:
                    a_type = '综合异常'
            
            results.append({
                **d,
                'anomaly_score': round(scores[i], 4),
                'is_anomaly': is_anomaly,
                'anomaly_type': a_type,
            })
        return results
    
    # ========== 3. 时序断档检测 ==========
    def detect_time_gaps(self, timestamps: List[str], expected_interval_minutes: int = 5,
                         max_gap_minutes: int = 30) -> List[Dict]:
        """
        检测时序数据断档
        timestamps: ["2026-06-08 08:00:00", "2026-06-08 08:05:00", ...]
        """
        if len(timestamps) < 2:
            return []
        
        gaps = []
        ts_list = [datetime.strptime(t, '%Y-%m-%d %H:%M:%S') for t in sorted(timestamps)]
        
        for i in range(1, len(ts_list)):
            gap_minutes = (ts_list[i] - ts_list[i - 1]).total_seconds() / 60
            if gap_minutes > max_gap_minutes:
                gaps.append({
                    'gap_start': ts_list[i - 1].strftime('%Y-%m-%d %H:%M:%S'),
                    'gap_end': ts_list[i].strftime('%Y-%m-%d %H:%M:%S'),
                    'gap_minutes': round(gap_minutes, 1),
                    'severity': 'CRITICAL' if gap_minutes > 60 else 'MAJOR' if gap_minutes > 30 else 'WARNING',
                })
        
        return gaps
    
    # ========== 4. 批量异常报告 ==========
    def generate_anomaly_report(self, vehicle_data: List[Dict] = None,
                                device_data: List[Dict] = None,
                                timestamps: List[str] = None) -> Dict:
        """生成综合异常检测报告"""
        report = {
            'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'flow_anomalies': [],
            'device_anomalies': [],
            'time_gaps': [],
            'summary': {},
        }
        
        if vehicle_data:
            flow_results = self.detect_flow_anomaly(vehicle_data)
            anomalies = [r for r in flow_results if r['is_anomaly']]
            report['flow_anomalies'] = anomalies
            report['summary']['flow_anomaly_count'] = len(anomalies)
        
        if device_data:
            dev_results = self.detect_device_anomaly(device_data)
            anomalies = [r for r in dev_results if r['is_anomaly']]
            report['device_anomalies'] = anomalies
            report['summary']['device_anomaly_count'] = len(anomalies)
        
        if timestamps:
            gaps = self.detect_time_gaps(timestamps)
            report['time_gaps'] = gaps
            report['summary']['time_gap_count'] = len(gaps)
        
        total = sum([
            report['summary'].get('flow_anomaly_count', 0),
            report['summary'].get('device_anomaly_count', 0),
            report['summary'].get('time_gap_count', 0),
        ])
        report['summary']['total_anomalies'] = total
        report['summary']['status'] = 'NORMAL' if total == 0 else 'WARNING' if total < 5 else 'CRITICAL'
        
        return report


# ========== 演示 ==========
if __name__ == '__main__':
    detector = TrafficAnomalyDetector()
    
    # 模拟数据：24小时车流量 (假设某天8-9点异常高峰)
    vehicle_data = []
    for h in range(24):
        base = 500 if h < 7 or h > 19 else 1000
        flow = base + random.randint(-100, 200)
        # 注入异常：8点流量异常飙升
        if h == 8:
            flow = 2500
        vehicle_data.append({'hour': h, 'flow': flow, 'road_id': 'R001'})
    
    # 模拟设备数据
    device_data = [
        {'device_id': 'D001', 'cpu': 45, 'mem': 62, 'temp': 38},
        {'device_id': 'D002', 'cpu': 52, 'mem': 70, 'temp': 42},
        {'device_id': 'D003', 'cpu': 95, 'mem': 88, 'temp': 85},  # 异常
        {'device_id': 'D004', 'cpu': 38, 'mem': 55, 'temp': 35},
        {'device_id': 'D005', 'cpu': 92, 'mem': 91, 'temp': 78},  # 异常
        {'device_id': 'D006', 'cpu': 48, 'mem': 60, 'temp': 40},
        {'device_id': 'D007', 'cpu': 42, 'mem': 58, 'temp': 36},
        {'device_id': 'D008', 'cpu': 88, 'mem': 85, 'temp': 82},  # 异常
        {'device_id': 'D009', 'cpu': 50, 'mem': 65, 'temp': 39},
        {'device_id': 'D010', 'cpu': 44, 'mem': 60, 'temp': 37},
    ]
    
    # 模拟时间序列
    base_time = datetime(2026, 6, 8, 0, 0, 0)
    timestamps = []
    for i in range(288):  # 24h * 12 (每5分钟)
        timestamps.append((base_time + timedelta(minutes=i * 5)).strftime('%Y-%m-%d %H:%M:%S'))
    # 注入断档：删除一段数据模拟
    timestamps = timestamps[:100] + timestamps[120:]  # 100分钟断档
    
    report = detector.generate_anomaly_report(vehicle_data, device_data, timestamps)
    print(json.dumps(report, ensure_ascii=False, indent=2))
