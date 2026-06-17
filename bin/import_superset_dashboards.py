#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Superset 仪表盘自动创建器 — 通过 REST API 创建数据源 + 数据集 + 图表 + 大屏

用法:
  python bin/import_superset_dashboards.py --host http://localhost:8089
"""
import argparse, json, os, sys
import requests


class SupersetAPI:
    def __init__(self, host, username="admin", password="admin123"):
        self.host = host.rstrip("/")
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.access_token = None

    def login(self):
        r = self.session.post(
            f"{self.host}/api/v1/security/login",
            json={"username": self.username, "password": self.password, "provider": "db"},
        )
        if r.status_code != 200:
            print(f"[FAIL] 登录失败: {r.status_code}")
            return False
        self.access_token = r.json().get("access_token")
        self.session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        })
        # 获取 CSRF token (Superset 3.x 需要尾部斜杠)
        try:
            csrf_r = self.session.get(f"{self.host}/api/v1/security/csrf_token/")
            if csrf_r.status_code == 200:
                csrf = csrf_r.json().get("result", "")
                if csrf:
                    self.session.headers["X-CSRFToken"] = csrf
        except Exception:
            pass
        print(f"[OK] 登录成功")
        return True

    def add_database(self, name, uri):
        try:
            r = self.session.post(
                f"{self.host}/api/v1/database/",
                json={"database_name": name, "sqlalchemy_uri": uri,
                      "expose_in_sqllab": True, "allow_csv_upload": True},
            )
            if r.status_code in (200, 201):
                db_id = r.json().get("id")
                print(f"[OK] 数据源 '{name}' (id={db_id})")
                return db_id
            body = r.text
            if "already exists" in body or "unique" in body:
                print(f"[SKIP] 数据源 '{name}' 已存在")
                r2 = self.session.get(f"{self.host}/api/v1/database/")
                for db in r2.json().get("result", []):
                    if db.get("database_name") == name:
                        return db.get("id")
            print(f"[WARN] 添加数据源失败: {body[:120]}")
        except Exception as e:
            print(f"[WARN] 数据源异常: {e}")
        return None

    def create_dataset(self, db_id, schema, table_name):
        try:
            r = self.session.post(
                f"{self.host}/api/v1/dataset/",
                json={"database": db_id, "schema": schema, "table_name": table_name},
            )
            if r.status_code in (200, 201):
                ds_id = r.json().get("id")
                print(f"  [OK] 数据集 '{table_name}' (id={ds_id})")
                return ds_id
            if "already exists" in r.text:
                print(f"  [SKIP] 数据集 '{table_name}' 已存在")
                return None
            print(f"  [WARN] 数据集失败: {r.text[:100]}")
        except Exception as e:
            print(f"  [WARN] 数据集异常: {e}")
        return None

    def create_chart(self, name, viz_type, ds_id, params=None):
        try:
            payload = {
                "slice_name": name,
                "viz_type": viz_type,
                "datasource_id": ds_id,
                "datasource_type": "table",
                "params": json.dumps(params or {}),
            }
            r = self.session.post(f"{self.host}/api/v1/chart/", json=payload)
            if r.status_code in (200, 201):
                cid = r.json().get("id")
                print(f"  [OK] 图表 '{name}' (id={cid})")
                return cid
            if "already exists" in r.text:
                print(f"  [SKIP] 图表 '{name}' 已存在")
                # 查找已有 chart ID
                r2 = self.session.get(f"{self.host}/api/v1/chart/", params={"q": f"(page_size:200)"})
                for c in r2.json().get("result", []):
                    if c.get("slice_name") == name:
                        return c.get("id")
                return None
            print(f"  [WARN] 图表失败: {r.text[:100]}")
        except Exception as e:
            print(f"  [WARN] 图表异常: {e}")
        return None

    def create_dashboard(self, title, slug):
        try:
            r = self.session.post(
                f"{self.host}/api/v1/dashboard/",
                json={"dashboard_title": title, "slug": slug, "published": True},
            )
            if r.status_code in (200, 201):
                did = r.json().get("id")
                print(f"[OK] 仪表盘 '{title}' (id={did})")
                return did
            if "already exists" in r.text:
                print(f"[SKIP] 仪表盘 '{title}' 已存在")
                # 查找已有 ID
                r2 = self.session.get(f"{self.host}/api/v1/dashboard/")
                for d in r2.json().get("result", []):
                    if d.get("dashboard_title") == title:
                        return d.get("id")
            print(f"[WARN] 仪表盘失败: {r.text[:100]}")
        except Exception as e:
            print(f"[WARN] 仪表盘异常: {e}")
        return None

    def place_charts_on_dashboard(self, dashboard_id, chart_ids):
        """将图表放置到仪表盘上（配置 positions 布局）"""
        try:
            # 构造 positions 布局（grid 排列：每行 3 个图表）
            positions = {"DASHBOARD_VERSION_KEY": "v2"}
            # 根节点
            root_children = []

            for i, cid in enumerate(chart_ids):
                child_id = f"CHART-{cid}"
                root_children.append(child_id)

                positions[child_id] = {
                    "type": "CHART",
                    "id": child_id,
                    "children": [],
                    "meta": {
                        "chartId": cid,
                        "width": 4,
                        "height": 16,
                    },
                    "parents": ["ROOT_ID"],
                }

            positions["ROOT_ID"] = {
                "type": "GRID",
                "id": "ROOT_ID",
                "children": root_children,
                "meta": {},
                "parents": [],
            }

            payload = {
                "positions": json.dumps(positions),
                "published": True,
            }

            r2 = self.session.put(
                f"{self.host}/api/v1/dashboard/{dashboard_id}",
                json=payload
            )
            if r2.status_code in (200, 201):
                print(f"  [OK] 已将 {len(chart_ids)} 个图表放置到仪表盘 (id={dashboard_id})")
                return True
            print(f"  [WARN] 放置图表失败: {r2.text[:200]}")
            return False
        except Exception as e:
            print(f"  [WARN] 放置图表异常: {e}")
            return False


# ============================================================
# 仪表盘定义
# ============================================================

TRAFFIC_OVERVIEW_CHARTS = [
    ("交通总览KPI", "big_number_total", {
        "metric": {"expressionType": "SQL", "sqlExpression": "SUM(traffic_count)", "label": "总车流量"},
        "subheader": "今日总车流量",
    }),
    ("24小时车流趋势", "echarts_timeseries_line", {
        "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(traffic_count)", "label": "车流量"}],
        "groupby": ["hour"],
        "x_axis_format": "%H",
        "time_range": "Last day",
        "contributionMode": None,
    }),
    ("区域流量与拥堵对比", "table", {
        "all_columns": ["area_name", "total_traffic_flow", "avg_congestion_rate"],
        "page_length": 10,
    }),
    ("拥堵TOP10道路", "table", {
        "all_columns": ["road_name", "avg_jam_level", "rank_num"],
        "page_length": 10,
    }),
]

DEVICE_MONITOR_CHARTS = [
    ("设备健康KPI", "big_number_total", {
        "metric": {"expressionType": "SQL", "sqlExpression": "AVG(health_score)", "label": "平均健康分"},
        "subheader": "设备平均健康评分",
    }),
    ("设备详情表", "table", {
        "all_columns": ["device_name", "device_type", "health_score", "health_level", "online_rate", "avg_cpu_usage", "avg_mem_usage"],
        "page_length": 20,
    }),
    ("MTBF/MTTR表", "table", {
        "all_columns": ["device_name", "mtbf_hours", "mttr_minutes"],
        "page_length": 20,
    }),
]

QUALITY_CHARTS = [
    ("质量KPI", "big_number_total", {
        "metric": {"expressionType": "SQL", "sqlExpression": "AVG(score)", "label": "质量总分"},
        "subheader": "数据质量评分",
    }),
    ("质量详情表", "table", {
        "all_columns": ["table_name", "completeness_rate", "uniqueness_rate", "validity_rate", "kafka_lag", "status"],
        "page_length": 20,
    }),
    ("质量趋势", "echarts_timeseries_line", {
        "metrics": [{"expressionType": "SQL", "sqlExpression": "AVG(score)", "label": "质量评分"}],
        "groupby": ["report_date"],
        "time_range": "Last week",
    }),
]


def setup_all(host, user, password):
    api = SupersetAPI(host, user, password)
    if not api.login():
        return False

    # 1. 添加 SQLite 数据源
    print("\n--- 数据源 ---")
    db_id = api.add_database("Traffic_SQLite", "sqlite:////app/traffic_data.db")
    if not db_id:
        print("[FAIL] 无法创建数据源，请确认 SQLite 文件存在于容器内")
        print("  提示: docker exec traffic-app ls /app/data/traffic_data.db")
        return False

    # 2. 创建数据集 (针对每张表)
    print("\n--- 数据集 ---")
    tables = [
        "dws_road_hour_flow", "dim_area", "ads_traffic_operation",
        "ads_top_jam_roads", "ads_device_health_score",
        "ads_device_mtbf_mttr", "dws_device_health_day",
        "data_quality_results",
    ]
    ds_map = {}
    for t in tables:
        ds_id = api.create_dataset(db_id, "main", t)
        if ds_id:
            ds_map[t] = ds_id

    if not ds_map:
        print("[FAIL] 无法创建任何数据集")
        return False

    # 3. 创建仪表盘和图表
    print("\n--- 仪表盘 & 图表 ---")

    # 交通总览大屏
    dash1 = api.create_dashboard("城市交通总览大屏", "traffic-overview")
    dash1_charts = []
    if dash1 and "dws_road_hour_flow" in ds_map:
        for name, viz, params in TRAFFIC_OVERVIEW_CHARTS:
            cid = api.create_chart(name, viz, ds_map["dws_road_hour_flow"], params)
            if cid:
                dash1_charts.append(cid)

    # 实时监控大屏
    dash2 = api.create_dashboard("实时路况监控面板", "realtime-monitor")
    dash2_charts = []
    if dash2 and "dws_road_hour_flow" in ds_map:
        for name, viz, params in [
            ("实时流量卡", "big_number_total",
             {"metric": {"expressionType": "SQL", "sqlExpression": "SUM(traffic_count)", "label": "实时流量"}}),
            ("拥堵分布", "table",
             {"all_columns": ["road_name", "jam_level", "avg_speed", "traffic_count"]}),
        ]:
            cid = api.create_chart(name, viz, ds_map["dws_road_hour_flow"], params)
            if cid:
                dash2_charts.append(cid)

    # 设备运维大屏
    dash3 = api.create_dashboard("设备运维监控大屏", "device-monitor")
    dash3_charts = []
    if dash3 and "ads_device_health_score" in ds_map:
        for name, viz, params in DEVICE_MONITOR_CHARTS:
            cid = api.create_chart(name, viz, ds_map["ads_device_health_score"], params)
            if cid:
                dash3_charts.append(cid)
    if dash3 and "ads_device_mtbf_mttr" in ds_map:
        cid = api.create_chart("MTBF/MTTR可靠性", "table",
                                ds_map["ads_device_mtbf_mttr"],
                                {"all_columns": ["device_name", "mtbf_hours", "mttr_minutes"]})
        if cid:
            dash3_charts.append(cid)

    # 数据质量大屏
    dash4 = api.create_dashboard("数据质量监控面板", "data-quality")
    dash4_charts = []
    if dash4 and "data_quality_results" in ds_map:
        for name, viz, params in QUALITY_CHARTS:
            cid = api.create_chart(name, viz, ds_map["data_quality_results"], params)
            if cid:
                dash4_charts.append(cid)

    # 4. 将图表放置到仪表盘
    print("\n--- 图表布局 ---")
    if dash1 and dash1_charts:
        api.place_charts_on_dashboard(dash1, dash1_charts)
    if dash2 and dash2_charts:
        api.place_charts_on_dashboard(dash2, dash2_charts)
    if dash3 and dash3_charts:
        api.place_charts_on_dashboard(dash3, dash3_charts)
    if dash4 and dash4_charts:
        api.place_charts_on_dashboard(dash4, dash4_charts)

    print(f"\n[OK] 全部完成!")
    print(f"  访问: {host}")
    print(f"  登录: {user} / {password}")
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="http://localhost:8089")
    p.add_argument("--user", default="admin")
    p.add_argument("--password", default="admin123")
    args = p.parse_args()

    print("=" * 55)
    print("  Superset 仪表盘自动创建器")
    print(f"  Host: {args.host}")
    print("=" * 55)
    setup_all(args.host, args.user, args.password)


if __name__ == "__main__":
    main()
