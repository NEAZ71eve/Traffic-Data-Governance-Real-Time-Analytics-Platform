#!/usr/bin/env python3
# ============================================
# NL2SQL 增强助手
# 升级版：支持更复杂的自然语言查询转 SQL
# ============================================

import json
import re
from typing import Dict, List, Optional, Tuple


class NL2SQLConverter:
    """
    自然语言 → Hive SQL 转换器
    支持：交通查询、设备查询、数据质量查询
    """
    
    # 实体映射
    ENTITIES = {
        '道路': 'dim_road_zip',
        '路段': 'dim_road_zip',
        '卡口': 'dim_device_zip',
        '设备': 'dim_device_zip',
        '区域': 'dim_area',
        '摄像头': 'dim_device_zip',
        '雷达': 'dim_device_zip',
        '传感器': 'dim_device_zip',
        '红绿灯': 'dim_device_zip',
    }
    
    # 指标映射
    METRICS = {
        '车流量': 'traffic_count',
        '通行量': 'traffic_count',
        '车速': 'avg_speed',
        '速度': 'avg_speed',
        '平均速度': 'avg_speed',
        '拥堵指数': 'congestion_rate',
        '拥堵率': 'congestion_rate',
        '拥堵等级': 'jam_level',
        '健康评分': 'health_score',
        'CPU': 'avg_cpu_usage',
        '内存': 'avg_memory_usage',
        '温度': 'avg_temperature',
        '在线率': 'online_rate',
        'MTBF': 'mtbf',
        'MTTR': 'mttr',
        '故障': 'alarm_count',
        '告警': 'alarm_count',
    }
    
    # 时间映射
    TIME_PATTERNS = {
        r'今天': "dt = '${today}'",
        r'昨天': "dt = '${yesterday}'",
        r'最近(\d+)天': r"dt >= DATE_SUB('${today}', \1)",
        r'本周': "dt >= DATE_SUB('${today}', 7)",
        r'本月': "dt >= DATE_SUB('${today}', 30)",
    }
    
    # 排序映射
    ORDER_MAP = {
        '最高': 'DESC', '最多': 'DESC', '最大': 'DESC', '最低': 'ASC',
        '最少': 'ASC', '最小': 'ASC', '最拥堵': 'DESC',
        '拥堵': 'DESC', '严重': 'DESC', '通畅': 'ASC',
    }
    
    def __init__(self):
        self.context = {}
    
    def parse(self, query: str) -> Dict:
        """解析自然语言查询"""
        result = {
            'query': query,
            'intent': self._detect_intent(query),
            'entities': self._extract_entities(query),
            'metrics': self._extract_metrics(query),
            'filters': self._extract_filters(query),
            'time_range': self._extract_time(query),
            'limit': self._extract_limit(query),
            'order': self._extract_order(query),
        }
        return result
    
    def to_sql(self, query: str) -> str:
        """自然语言 → SQL"""
        parsed = self.parse(query)
        
        if parsed['intent'] == 'road_jam_rank':
            return self._road_jam_rank_sql(parsed)
        elif parsed['intent'] == 'road_flow':
            return self._road_flow_sql(parsed)
        elif parsed['intent'] == 'device_health':
            return self._device_health_sql(parsed)
        elif parsed['intent'] == 'device_fault':
            return self._device_fault_sql(parsed)
        elif parsed['intent'] == 'area_congestion':
            return self._area_congestion_sql(parsed)
        elif parsed['intent'] == 'device_offline':
            return self._device_offline_sql(parsed)
        elif parsed['intent'] == 'peak_analysis':
            return self._peak_analysis_sql(parsed)
        elif parsed['intent'] == 'quality_check':
            return self._quality_check_sql(parsed)
        else:
            return f"-- 未能识别查询意图，原始查询: {query}\n-- 支持: 拥堵排行、车流量、设备健康、故障统计、区域拥堵、离线设备、高峰分析、数据质量"
    
    def _detect_intent(self, query: str) -> str:
        """检测查询意图"""
        patterns = [
            (r'拥堵.*道路|拥堵.*排行|拥堵.*TOP|最.*堵', 'road_jam_rank'),
            (r'车流量|通行.*统计|.*流量.*排行', 'road_flow'),
            (r'设备.*健康|健康.*评分|健康.*排行', 'device_health'),
            (r'故障.*统计|告警.*统计|故障.*设备|告警.*设备|故障.*最多|温度过高|CPU.*高', 'device_fault'),
            (r'区域.*拥堵|哪个区|各区.*拥堵', 'area_congestion'),
            (r'离线.*设备|断连|不在线|设备.*离线|哪些设备.*离线', 'device_offline'),
            (r'高峰|早晚高峰|高峰.*分析', 'peak_analysis'),
            (r'数据质量|质量.*检查|质量.*报表', 'quality_check'),
        ]
        for pattern, intent in patterns:
            if re.search(pattern, query):
                return intent
        return 'unknown'
    
    def _extract_entities(self, query: str) -> List[str]:
        entities = []
        for k in self.ENTITIES:
            if k in query:
                entities.append(k)
        return entities
    
    def _extract_metrics(self, query: str) -> List[str]:
        metrics = []
        for k in self.METRICS:
            if k in query:
                metrics.append(k)
        return metrics
    
    def _extract_filters(self, query: str) -> Dict:
        filters = {}
        # 设备类型过滤
        for t in ['卡口', '摄像头', '雷达', '传感器', '红绿灯']:
            if t in query:
                filters['device_type'] = t
        
        # 区域过滤
        area_match = re.search(r'(朝阳|海淀|浦东|天河|南山)区', query)
        if area_match:
            filters['area_name'] = area_match.group(1) + '区'
        
        # 拥堵等级过滤
        for lvl in ['严重拥堵', '中度拥堵', '轻度拥堵']:
            if lvl in query:
                filters['jam_level'] = {'严重拥堵': 5, '中度拥堵': 4, '轻度拥堵': 3}[lvl]
        
        # 数目限制
        num_match = re.search(r'(\d+)条|(\d+)个|(\d+)台|前(\d+)', query)
        if num_match:
            filters['limit'] = int([g for g in num_match.groups() if g][0])
        
        return filters
    
    def _extract_time(self, query: str) -> str:
        for pattern, replacement in self.TIME_PATTERNS.items():
            if re.search(pattern, query):
                return replacement
        return "dt = '${today}'"
    
    def _extract_limit(self, query: str) -> int:
        match = re.search(r'前(\d+)|TOP\s*(\d+)|(\d+)条|(\d+)个', query, re.IGNORECASE)
        if match:
            return int([g for g in match.groups() if g][0])
        return 10
    
    def _extract_order(self, query: str) -> str:
        for k, v in self.ORDER_MAP.items():
            if k in query:
                return v
        return 'DESC'
    
    # ========== SQL 生成模板 ==========
    
    def _road_jam_rank_sql(self, parsed: Dict) -> str:
        limit = parsed['limit']
        time_filter = parsed['time_range']
        return f"""-- 查询: {parsed['query']}
-- 意图: 拥堵路段排行
SELECT
    r.road_id,
    r.road_name,
    r.road_type,
    a.area_name,
    AVG(j.jam_level) AS avg_jam_level,
    AVG(j.congestion_rate) AS avg_congestion_rate,
    AVG(j.avg_speed) AS avg_speed,
    SUM(j.traffic_flow) AS total_traffic_flow
FROM traffic_db.dws_area_jam_hour j
JOIN traffic_db.dim_road_zip r ON j.area_id = r.area_id AND r.is_current = 'Y'
JOIN traffic_db.dim_area a ON j.area_id = a.area_id
WHERE j.{time_filter}
GROUP BY r.road_id, r.road_name, r.road_type, a.area_name
ORDER BY avg_congestion_rate DESC
LIMIT {limit};"""
    
    def _road_flow_sql(self, parsed: Dict) -> str:
        limit = parsed['limit']
        time_filter = parsed['time_range']
        return f"""-- 查询: {parsed['query']}
-- 意图: 车流量统计排行
SELECT
    r.road_name,
    r.road_type,
    SUM(f.traffic_count) AS total_flow,
    AVG(f.avg_speed) AS avg_speed,
    MAX(f.traffic_count) AS peak_flow,
    COUNT(DISTINCT f.hour) AS active_hours
FROM traffic_db.dws_road_hour_flow f
JOIN traffic_db.dim_road_zip r ON f.road_id = r.road_id AND r.is_current = 'Y'
WHERE f.{time_filter}
GROUP BY r.road_name, r.road_type
ORDER BY total_flow DESC
LIMIT {limit};"""
    
    def _device_health_sql(self, parsed: Dict) -> str:
        limit = parsed['limit']
        filters = parsed['filters']
        device_filter = f"AND d.device_type = '{filters['device_type']}'" if 'device_type' in filters else ''
        
        return f"""-- 查询: {parsed['query']}
-- 意图: 设备健康评分
SELECT
    d.device_id,
    d.device_name,
    d.device_type,
    d.road_id,
    s.health_score,
    s.online_rate * 100 AS online_rate_pct,
    s.avg_cpu_usage,
    s.avg_memory_usage,
    s.avg_temperature,
    CASE 
        WHEN s.health_score >= 90 THEN 'EXCELLENT'
        WHEN s.health_score >= 75 THEN 'GOOD'
        WHEN s.health_score >= 60 THEN 'FAIR'
        WHEN s.health_score >= 40 THEN 'POOR'
        ELSE 'CRITICAL'
    END AS health_level
FROM traffic_db.ads_device_health_score s
JOIN traffic_db.dim_device_zip d ON s.device_id = d.device_id AND d.is_current = 'Y'
WHERE s.dt = '${{today}}'
  {device_filter}
ORDER BY s.health_score {parsed['order']}
LIMIT {limit};"""
    
    def _device_fault_sql(self, parsed: Dict) -> str:
        limit = parsed['limit']
        time_filter = parsed['time_range']
        return f"""-- 查询: {parsed['query']}
-- 意图: 设备故障统计
SELECT
    d.device_name,
    d.device_type,
    a.alarm_type,
    a.alarm_level,
    SUM(a.total_alarm_count) AS alarm_count,
    SUM(a.recovered_count) AS recovered_count,
    ROUND(AVG(a.avg_recover_minutes), 1) AS avg_recover_minutes,
    ROUND(SUM(a.recovered_count) * 100.0 / SUM(a.total_alarm_count), 1) AS recovery_rate
FROM traffic_db.dws_alarm_day a
JOIN traffic_db.dim_device_zip d ON a.device_id = d.device_id AND d.is_current = 'Y'
WHERE a.{time_filter}
GROUP BY d.device_name, d.device_type, a.alarm_type, a.alarm_level
ORDER BY alarm_count DESC
LIMIT {limit};"""
    
    def _area_congestion_sql(self, parsed: Dict) -> str:
        return f"""-- 查询: {parsed['query']}
-- 意图: 区域拥堵分析
SELECT
    a.area_name,
    a.city_name,
    COUNT(DISTINCT j.area_id) AS affected_roads,
    AVG(j.avg_congestion_rate) AS avg_congestion,
    MAX(j.avg_congestion_rate) AS max_congestion,
    SUM(CASE WHEN j.jam_level >= 4 THEN 1 ELSE 0 END) AS severe_jam_count,
    SUM(j.total_traffic_flow) AS total_flow
FROM traffic_db.dws_area_jam_hour j
JOIN traffic_db.dim_area a ON j.area_id = a.area_id
WHERE j.{parsed['time_range']}
GROUP BY a.area_name, a.city_name
ORDER BY avg_congestion DESC;"""

    def _device_offline_sql(self, parsed: Dict) -> str:
        return f"""-- 查询: {parsed['query']}
-- 意图: 离线设备查询
SELECT
    d.device_id,
    d.device_name,
    d.device_type,
    d.road_id,
    d.manufacturer,
    d.status,
    h.offline_count,
    h.last_heartbeat
FROM traffic_db.dws_device_health_day h
JOIN traffic_db.dim_device_zip d ON h.device_id = d.device_id AND d.is_current = 'Y'
WHERE h.dt = '${{today}}'
  AND h.offline_count > 0
ORDER BY h.offline_count DESC;"""
    
    def _peak_analysis_sql(self, parsed: Dict) -> str:
        filters = parsed['filters']
        area_filter = f"AND a.area_name = '{filters['area_name']}'" if 'area_name' in filters else ''
        
        return f"""-- 查询: {parsed['query']}
-- 意图: 高峰时段分析
SELECT
    j.hour,
    j.peak_period,
    a.area_name,
    COUNT(DISTINCT j.area_id) AS jam_road_count,
    AVG(j.avg_congestion_rate) AS avg_congestion,
    AVG(j.jam_level) AS avg_jam_level,
    SUM(j.total_traffic_flow) AS total_flow
FROM traffic_db.dws_area_jam_hour j
JOIN traffic_db.dim_area a ON j.area_id = a.area_id
WHERE j.{parsed['time_range']}
  AND j.peak_period IN ('MORNING_PEAK', 'EVENING_PEAK')
  {area_filter}
GROUP BY j.hour, j.peak_period, a.area_name
ORDER BY avg_congestion DESC;"""
    
    def _quality_check_sql(self, parsed: Dict) -> str:
        return f"""-- 查询: {parsed['query']}
-- 意图: 数据质量检查
-- 执行: python python/data_quality_monitor.py
SELECT '数据质量检查需通过 Python 脚本执行' AS note;

-- 以下为可执行的质量检查SQL示例:
-- 完整率检查
SELECT 
    'ods_vehicle_pass_di' AS table_name,
    COUNT(1) AS total,
    SUM(CASE WHEN vehicle_id IS NULL OR road_id IS NULL THEN 1 ELSE 0 END) AS null_count,
    ROUND((1 - SUM(CASE WHEN vehicle_id IS NULL OR road_id IS NULL THEN 1 ELSE 0 END) / COUNT(1)) * 100, 2) AS completeness
FROM traffic_db.ods_vehicle_pass_di
WHERE dt = '${{today}}';

-- 唯一性检查
SELECT 
    COUNT(1) AS total,
    COUNT(DISTINCT vehicle_id, pass_time) AS unique_count,
    ROUND(COUNT(DISTINCT vehicle_id, pass_time) * 100.0 / COUNT(1), 2) AS uniqueness
FROM traffic_db.dwd_vehicle_pass_di
WHERE dt = '${{today}}';"""


# ========== 演示 ==========
if __name__ == '__main__':
    converter = NL2SQLConverter()
    
    queries = [
        "今天最拥堵的5条道路",
        "最近7天车流量最高的10条路",
        "长安街的设备健康评分是多少",
        "最近一个月故障最多的5台设备",
        "哪个区域拥堵最严重",
        "有哪些设备离线了",
        "今天早晚高峰堵了多少条路",
        "检查昨天的数据质量",
        "朝阳区最近7天的高峰拥堵情况",
        "CPU温度过高的摄像头有哪些",
    ]
    
    for q in queries:
        print(f"\n{'='*60}")
        print(f"查询: {q}")
        print(f"{'='*60}")
        parsed = converter.parse(q)
        print(f"意图: {parsed['intent']}")
        print(f"实体: {parsed['entities']}")
        print(f"指标: {parsed['metrics']}")
        print(f"过滤: {parsed['filters']}")
        print(f"时间: {parsed['time_range']}")
        print(f"数量: {parsed['limit']}")
        print(f"\nSQL:")
        print(converter.to_sql(q))
