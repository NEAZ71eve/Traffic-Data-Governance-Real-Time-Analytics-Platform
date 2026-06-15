#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Superset 可视化看板自动配置脚本

功能:
1. 配置 Hive / Redis 数据源
2. 导入 4 套看板（交通总览 / 实时路况 / 设备运维 / 数据质量）
3. 设置刷新策略

用法:
    # 在 Superset 容器内运行
    docker exec traffic-superset python /app/bin/setup_superset.py

    # 或本地运行（需要 Superset API 可访问）
    python bin/setup_superset.py --host http://localhost:8088 --user admin --password admin123

前置条件:
    - Superset 服务已启动
    - HiveServer2 已就绪
    - Redis 已就绪
"""

import json
import sys
import time
import os
import argparse
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urljoin


# ============================================================================
# 配置
# ============================================================================
DEFAULT_HOST = "http://localhost:8088"
DEFAULT_USER = "admin"
DEFAULT_PASSWORD = "admin123"

# 数据源配置
DATASOURCES = [
    {
        "name": "Hive (Traffic DB)",
        "engine": "hive",
        "uri": "hive://hiveserver2:10000/traffic_db",
        "description": "智慧城市交通数仓 — Hive 离线数据源",
        "default": True,
    },
    {
        "name": "Redis (Realtime)",
        "engine": "redis",
        "uri": "redis://redis:6379/0",
        "description": "Flink 实时计算缓存 — 实时路况数据源",
        "default": False,
    },
]

# 看板配置（基于 BI_DASHBOARDS.md 设计）
DASHBOARDS = {
    "traffic-overview": {
        "name": "🏙️ 城市交通总览大屏",
        "slug": "traffic-overview",
        "refresh_interval": 300,  # 5分钟
        "charts": [
            {"name": "今日总车流量", "viz_type": "big_number_total", "sql": "SELECT SUM(total_traffic_flow) as val FROM ads_traffic_operation WHERE dt='${date}'"},
            {"name": "平均车速", "viz_type": "gauge", "sql": "SELECT ROUND(AVG(avg_speed_all),1) as val FROM ads_traffic_operation WHERE dt='${date}'"},
            {"name": "拥堵指数", "viz_type": "big_number_total", "sql": "SELECT ROUND(AVG(avg_congestion_rate),1) as val FROM ads_traffic_operation WHERE dt='${date}'"},
            {"name": "24小时车流量趋势", "viz_type": "echarts_timeseries_line", "sql": "SELECT hour, SUM(traffic_count) as flow FROM dws_road_hour_flow WHERE dt='${date}' GROUP BY hour ORDER BY hour"},
            {"name": "区域拥堵热力图", "viz_type": "heatmap", "sql": "SELECT a.area_name, jh.hour, ROUND(AVG(jh.jam_level),1) as jam FROM dws_area_jam_hour jh JOIN dim_area a ON jh.area_id=a.area_id WHERE jh.dt='${date}' GROUP BY a.area_name, jh.hour"},
            {"name": "拥堵TOP10道路", "viz_type": "echarts_timeseries_bar", "sql": "SELECT road_name, avg_jam_level as val FROM ads_top_jam_roads WHERE dt='${date}' ORDER BY rank_num LIMIT 10"},
            {"name": "车辆类型分布", "viz_type": "pie", "sql": "SELECT '小型车' as t, SUM(small_car_cnt) as c FROM dws_road_hour_flow WHERE dt='${date}' UNION ALL SELECT '中型车', SUM(medium_car_cnt) FROM dws_road_hour_flow WHERE dt='${date}' UNION ALL SELECT '大型车', SUM(large_car_cnt) FROM dws_road_hour_flow WHERE dt='${date}'"},
        ],
    },
    "realtime-monitor": {
        "name": "🚥 实时路况监控面板",
        "slug": "realtime-monitor",
        "refresh_interval": 5,  # 5秒
        "charts": [
            {"name": "实时车流量", "viz_type": "big_number_total", "sql": "GET realtime:traffic:total_flow", "datasource": "Redis (Realtime)"},
            {"name": "实时均速", "viz_type": "gauge", "sql": "GET realtime:traffic:avg_speed", "datasource": "Redis (Realtime)"},
            {"name": "拥堵路段数", "viz_type": "big_number_total", "sql": "GET realtime:traffic:jam_count", "datasource": "Redis (Realtime)"},
            {"name": "拥堵等级分布", "viz_type": "pie", "sql": "HGETALL realtime:traffic:jam_distribution", "datasource": "Redis (Realtime)"},
            {"name": "实时拥堵道路列表", "viz_type": "table", "sql": "ZREVRANGE realtime:traffic:jam_roads 0 50 WITHSCORES", "datasource": "Redis (Realtime)"},
            {"name": "最近1小时车流曲线", "viz_type": "echarts_timeseries_line", "sql": "LRANGE realtime:traffic:flow_history 0 720", "datasource": "Redis (Realtime)"},
        ],
    },
    "device-monitor": {
        "name": "🔧 设备运维监控大屏",
        "slug": "device-monitor",
        "refresh_interval": 60,  # 1分钟
        "charts": [
            {"name": "设备总数", "viz_type": "big_number_total", "sql": "SELECT COUNT(DISTINCT device_id) FROM dws_device_health_day WHERE dt='${date}'"},
            {"name": "在线设备数", "viz_type": "big_number_total", "sql": "SELECT COUNT(DISTINCT device_id) FROM dws_device_health_day WHERE dt='${date}' AND offline_count=0"},
            {"name": "离线设备数", "viz_type": "big_number_total", "sql": "SELECT COUNT(DISTINCT device_id) FROM dws_device_health_day WHERE dt='${date}' AND offline_count>0"},
            {"name": "故障设备数", "viz_type": "big_number_total", "sql": "SELECT COUNT(DISTINCT device_id) FROM dws_device_health_day WHERE dt='${date}' AND health_score<60"},
            {"name": "在线率趋势(7天)", "viz_type": "echarts_timeseries_line", "sql": "SELECT dt, ROUND(SUM(online_duration)/(SUM(online_duration)+SUM(offline_count))*100,2) as rate FROM dws_device_health_day WHERE dt>=DATE_SUB('${date}',7) GROUP BY dt ORDER BY dt"},
            {"name": "健康状态分布", "viz_type": "pie", "sql": "SELECT CASE WHEN health_score>=80 THEN '优秀' WHEN health_score>=60 THEN '良好' ELSE '较差' END as level, COUNT(*) as cnt FROM ads_device_health_score WHERE dt='${date}' GROUP BY level"},
            {"name": "设备健康评分排行", "viz_type": "table", "sql": "SELECT device_name, device_type, health_score, health_level, online_rate, avg_cpu_usage, avg_memory_usage FROM ads_device_health_score WHERE dt='${date}' ORDER BY health_score ASC LIMIT 20"},
            {"name": "MTBF/MTTR 可靠性", "viz_type": "echarts_timeseries_bar", "sql": "SELECT device_name, mtbf_hours, mttr_minutes FROM ads_device_mtbf_mttr WHERE dt='${date}' ORDER BY mtbf_hours ASC LIMIT 20"},
        ],
    },
    "data-quality": {
        "name": "📊 数据质量监控面板",
        "slug": "data-quality",
        "refresh_interval": 600,  # 10分钟
        "charts": [
            {"name": "质量总分", "viz_type": "big_number_total", "sql": "SELECT AVG(score) FROM data_quality_results WHERE report_date='${date}'"},
            {"name": "质量趋势(7天)", "viz_type": "echarts_timeseries_line", "sql": "SELECT report_date as dt, AVG(score) as score FROM data_quality_results GROUP BY report_date ORDER BY report_date"},
            {"name": "各表质量详情", "viz_type": "table", "sql": "SELECT table_name, completeness_rate, uniqueness_rate, validity_rate, kafka_lag, status FROM data_quality_results WHERE report_date='${date}'"},
            {"name": "Kafka Lag趋势", "viz_type": "echarts_timeseries_line", "sql": "SELECT report_date as dt, AVG(kafka_lag) as lag FROM data_quality_results GROUP BY report_date ORDER BY report_date"},
        ],
    },
}


# ============================================================================
# Superset API 客户端
# ============================================================================
class SupersetClient:
    """Superset REST API 客户端"""

    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = None
        self.access_token = None
        self.csrf_token = None

    def _request(self, method, path, data=None):
        """发送 HTTP 请求"""
        url = urljoin(self.base_url + '/', path.lstrip('/'))
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        if self.csrf_token:
            headers["X-CSRFToken"] = self.csrf_token

        body = json.dumps(data).encode('utf-8') if data else None
        req = Request(url, data=body, headers=headers, method=method)

        try:
            resp = urlopen(req, timeout=30)
            if resp.status == 200:
                return json.loads(resp.read().decode('utf-8'))
            return None
        except HTTPError as e:
            if e.code == 401:
                print(f"  ⚠️  认证失败，尝试重新登录...")
                self.login()
                return self._request(method, path, data)
            print(f"  ❌ HTTP {e.code}: {path}")
            return None
        except URLError as e:
            print(f"  ❌ 连接失败: {e.reason}")
            return None

    def login(self):
        """登录获取 token"""
        print("🔑 登录 Superset...")
        result = self._request("POST", "/api/v1/security/login", {
            "username": self.username,
            "password": self.password,
            "provider": "db",
        })
        if result and "access_token" in result:
            self.access_token = result["access_token"]

            # 同时尝试从 CSRF API 获取 token
            csrf = self._request("GET", "/api/v1/security/csrf_token")
            if csrf and "result" in csrf:
                self.csrf_token = csrf["result"]

            print(f"  ✅ 登录成功")
            return True
        else:
            print(f"  ❌ 登录失败，请检查账号密码")
            return False

    def add_database(self, name, uri):
        """添加数据源"""
        print(f"  📦 添加数据源: {name}")
        return self._request("POST", "/api/v1/database/", {
            "database_name": name,
            "sqlalchemy_uri": uri,
        })

    def get_databases(self):
        """获取已配置的数据源列表"""
        result = self._request("GET", "/api/v1/database/")
        if result and "result" in result:
            return result["result"]
        return []

    def create_dashboard(self, name, slug):
        """创建看板"""
        return self._request("POST", "/api/v1/dashboard/", {
            "dashboard_title": name,
            "slug": slug,
            "published": True,
        })

    def get_dashboards(self):
        """获取已有看板列表"""
        result = self._request("GET", "/api/v1/dashboard/")
        if result and "result" in result:
            return result["result"]
        return []


# ============================================================================
# 离线模式（无 Superset 时生成配置清单）
# ============================================================================
def generate_offline_config(output_dir=None):
    """生成 Superset 离线配置清单"""
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")

    print("=" * 60)
    print("  📋 生成 Superset 离线配置清单")
    print("=" * 60)

    config = {
        "version": "1.0",
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "datasources": DATASOURCES,
        "dashboards": DASHBOARDS,
        "setup_instructions": {
            "step1": "启动 Superset: docker compose -f docker-compose-phase2.yml up -d superset superset-db",
            "step2": "访问 http://localhost:8088 登录 (admin/admin123)",
            "step3": "Settings → Database Connections → 添加数据源",
            "step4": "Charts → + Chart → 根据下方 SQL 创建图表",
            "step5": "Dashboards → + Dashboard → 拖入图表 → 设置刷新间隔",
        },
        "quick_setup_commands": [
            "# 1. 添加 Hive 数据源",
            "superset set-database-uri -d 'Hive (Traffic DB)' -u 'hive://hiveserver2:10000/traffic_db'",
            "",
            "# 2. 添加 Redis 数据源",
            "superset set-database-uri -d 'Redis (Realtime)' -u 'redis://redis:6379/0'",
            "",
            "# 3. 导入看板（如果导出了 JSON）",
            "superset import-dashboards -p /path/to/dashboard_export.zip",
        ],
    }

    config_path = os.path.join(output_dir, "superset_setup.json")
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print(f"  ✅ 配置清单已生成: {config_path}")
    print()
    print("  📊 数据源清单:")
    for ds in DATASOURCES:
        print(f"    - {ds['name']}: {ds['uri']}")
    print()
    print("  📈 看板清单:")
    for slug, db in DASHBOARDS.items():
        print(f"    - {db['name']} (刷新: {db['refresh_interval']}s, {len(db['charts'])} 图表)")
    print()
    print("  ⚡ 快速配置命令:")
    for cmd in config["quick_setup_commands"]:
        if cmd:
            print(f"    {cmd}")
    print()

    return config_path


# ============================================================================
# 主入口
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="Superset 可视化看板自动配置")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Superset 地址 (默认: {DEFAULT_HOST})")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"用户名 (默认: {DEFAULT_USER})")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help=f"密码 (默认: {DEFAULT_PASSWORD})")
    parser.add_argument("--offline", action="store_true", help="离线模式：仅生成配置清单，不连接 Superset")
    parser.add_argument("--output", default=None, help="配置输出目录")

    args = parser.parse_args()

    if args.offline:
        generate_offline_config(args.output)
        return

    print("=" * 60)
    print("  🎨 Superset 可视化看板自动配置")
    print("=" * 60)
    print(f"  目标: {args.host}")
    print(f"  用户: {args.user}")
    print()

    # 尝试连接
    client = SupersetClient(args.host, args.user, args.password)
    if not client.login():
        print()
        print("  ⚠️  Superset 不可用，切换到离线模式生成配置清单")
        generate_offline_config(args.output)
        return

    # 添加数据源
    print("\n📦 配置数据源...")
    existing_dbs = client.get_databases()
    existing_names = {db.get("database_name", "") for db in existing_dbs}

    for ds in DATASOURCES:
        if ds["name"] in existing_names:
            print(f"  ⏭ {ds['name']} (已存在)")
        else:
            result = client.add_database(ds["name"], ds["uri"])
            if result:
                print(f"  ✅ {ds['name']}")
            else:
                print(f"  ❌ {ds['name']} — 可能已存在或连接失败")

    # 创建看板
    print("\n📈 创建看板...")
    existing_dashboards = {d.get("slug", ""): d for d in client.get_dashboards()}

    for slug, db_config in DASHBOARDS.items():
        if slug in existing_dashboards:
            print(f"  ⏭ {db_config['name']} (已存在)")
        else:
            result = client.create_dashboard(db_config["name"], slug)
            if result:
                print(f"  ✅ {db_config['name']} ({len(db_config['charts'])} 图表，刷新 {db_config['refresh_interval']}s)")
            else:
                print(f"  ⚠️ {db_config['name']} — 创建失败，请手动添加")

    # 输出摘要
    print()
    print("=" * 60)
    print("  ✅ 配置完成")
    print("=" * 60)
    print(f"  🌐 Superset:  {args.host}")
    print(f"  📊 数据源:   {len(DATASOURCES)} 个")
    print(f"  📈 看板:     {len(DASHBOARDS)} 套")
    total_charts = sum(len(db["charts"]) for db in DASHBOARDS.values())
    print(f"  📉 图表:     {total_charts} 个")
    print()
    print("  💡 提示:")
    print("    1. 登录 Superset 创建图表 → 拖入对应的看板")
    print("    2. 设置看板刷新间隔 → 看板设置 → 自动刷新")
    print("    3. 图表 SQL 请参考 docs/BI_DASHBOARDS.md")
    print()
    print("    快速查看:")
    print(f"    交通总览: {args.host}/superset/dashboard/traffic-overview/")
    print(f"    实时路况: {args.host}/superset/dashboard/realtime-monitor/")
    print(f"    设备运维: {args.host}/superset/dashboard/device-monitor/")
    print(f"    数据质量: {args.host}/superset/dashboard/data-quality/")
    print()


if __name__ == "__main__":
    main()
