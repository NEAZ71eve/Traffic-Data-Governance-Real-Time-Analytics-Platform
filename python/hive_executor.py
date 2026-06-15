"""
Hive 查询执行器
- 连接真实 Hive（beeline subprocess）
- 获取表结构注入 LLM
- 不可用时返回 mock 数据
"""

import subprocess
import re
from typing import List, Dict, Optional

HIVE_CMD = "beeline -u jdbc:hive2://localhost:10000/traffic_db"


class HiveExecutor:
    """Hive 查询执行器"""

    def __init__(self, hive_cmd: str = HIVE_CMD):
        self.hive_cmd = hive_cmd
        self._online = None

    @property
    def online(self) -> bool:
        """检查 Hive 是否可连"""
        if self._online is not None:
            return self._online
        try:
            result = subprocess.run(
                self.hive_cmd.split() + ["-e", "SELECT 1"],
                capture_output=True, text=True, timeout=10
            )
            self._online = "1 row selected" in result.stdout or "1" in result.stdout
        except Exception:
            self._online = False
        return self._online

    def execute(self, sql: str) -> dict:
        """
        在 Hive 上执行 SQL
        返回: {"success": bool, "rows": list, "columns": list, "error": str, "sql": str}
        """
        if not self.online:
            return self._mock_result(sql)

        try:
            result = subprocess.run(
                self.hive_cmd.split() + ["-e", sql],
                capture_output=True, text=True, timeout=60
            )
            if result.returncode != 0:
                return {"success": False, "rows": [], "columns": [], "error": result.stderr, "sql": sql}

            return self._parse_beeline_output(result.stdout, sql)
        except subprocess.TimeoutExpired:
            return {"success": False, "rows": [], "columns": [], "error": "查询超时（60秒）", "sql": sql}
        except Exception as e:
            return {"success": False, "rows": [], "columns": [], "error": str(e), "sql": sql}

    def table_schemas(self) -> str:
        """获取所有表结构，用于注入 LLM prompt"""
        tables = [
            "ods_vehicle_pass_di", "ods_traffic_status_di", "ods_device_status_di", "ods_alarm_log_di",
            "dwd_vehicle_pass_di", "dwd_traffic_status_di", "dwd_device_status_di", "dwd_alarm_log_di",
            "dws_road_hour_flow", "dws_area_jam_hour", "dws_device_health_day", "dws_alarm_day",
            "ads_traffic_operation", "ads_top_jam_roads", "ads_device_health_score",
            "ads_device_mtbf_mttr", "ads_device_fault_top",
            "dim_road_zip", "dim_device_zip", "dim_area", "dim_time",
        ]

        if self.online:
            schemas = []
            for t in tables:
                try:
                    r = subprocess.run(
                        self.hive_cmd.split() + ["-e", f"DESCRIBE {t}"],
                        capture_output=True, text=True, timeout=10
                    )
                    cols = re.findall(r"^\| (\w+) \| (\w+)", r.stdout, re.MULTILINE)
                    if cols:
                        col_str = ", ".join(f"{c[0]} {c[1]}" for c in cols)
                        schemas.append(f"- {t}: {col_str}")
                except Exception:
                    pass
            return "\n".join(schemas) if schemas else self._default_schemas()
        return self._default_schemas()

    def _default_schemas(self) -> str:
        """离线模式：返回预设表结构"""
        return """- ods_vehicle_pass_di: vehicle_id STRING, road_id STRING, device_id STRING, pass_time STRING, speed INT, direction STRING, plate_number STRING, vehicle_type STRING, lane INT (分区: dt STRING)
- ods_traffic_status_di: road_id STRING, avg_speed INT, traffic_flow INT, jam_level INT, congestion_rate DECIMAL, sample_time STRING (分区: dt STRING)
- ods_device_status_di: device_id STRING, cpu_usage DECIMAL, memory_usage DECIMAL, temperature DECIMAL, online_flag STRING, heartbeat_time STRING (分区: dt STRING)
- dwd_vehicle_pass_di: vehicle_id STRING, road_id STRING, pass_time TIMESTAMP, speed INT, vehicle_type STRING, hour INT (分区: dt STRING)
- dwd_traffic_status_di: road_id STRING, avg_speed INT, traffic_flow INT, jam_level INT, sample_time TIMESTAMP (分区: dt STRING)
- dws_road_hour_flow: road_id STRING, hour INT, traffic_count BIGINT, avg_speed DECIMAL, small_car_cnt BIGINT, medium_car_cnt BIGINT, large_car_cnt BIGINT (分区: dt STRING)
- dws_area_jam_hour: area_id STRING, hour INT, jam_level INT, total_traffic_flow BIGINT, avg_congestion_rate DECIMAL (分区: dt STRING)
- dws_device_health_day: device_id STRING, online_duration BIGINT, offline_count BIGINT, avg_cpu_usage DECIMAL, avg_memory_usage DECIMAL, abnormal_count BIGINT (分区: dt STRING)
- dws_alarm_day: alarm_type STRING, alarm_level STRING, total_alarm_count BIGINT, recovery_rate DECIMAL (分区: dt STRING)
- ads_traffic_operation: city_id STRING, total_vehicle_flow BIGINT, avg_speed_all DECIMAL, jam_road_count BIGINT, severe_jam_count BIGINT (分区: dt STRING)
- ads_top_jam_roads: rank_num INT, road_id STRING, road_name STRING, avg_jam_level DECIMAL, total_traffic_flow BIGINT (分区: dt STRING)
- ads_device_health_score: device_id STRING, health_score DECIMAL, health_level STRING, online_rate DECIMAL, avg_cpu_usage DECIMAL (分区: dt STRING)
- dim_road_zip: road_id STRING, road_name STRING, road_type STRING, area_id STRING (分区: dt STRING)
- dim_device_zip: device_id STRING, device_name STRING, device_type STRING (分区: dt STRING)
- dim_area: area_id STRING, area_name STRING, city_name STRING"""

    def _parse_beeline_output(self, output: str, sql: str) -> dict:
        """解析 beeline 的表格输出"""
        lines = output.strip().split("\n")
        rows = []
        columns = []
        started = False
        for line in lines:
            if line.startswith("|") and "|" in line[1:]:
                parts = [p.strip() for p in line.split("|")[1:-1]]
                if not started:
                    columns = parts
                    started = True
                elif set(parts) != {"--"} and parts != columns:
                    rows.append(parts)
        return {
            "success": True,
            "columns": columns,
            "rows": rows[:50],  # 最多返回50行
            "error": "",
            "sql": sql
        }

    def _mock_result(self, sql: str) -> dict:
        """Hive 不可用时返回 mock 结果"""
        return {
            "success": True,
            "columns": ["(查询结果)"],
            "rows": [["Hive 未连接，无法执行查询"]],
            "error": "",
            "sql": sql,
            "note": "Hive 离线模式 — 仅展示生成的 SQL，未实际执行"
        }


if __name__ == "__main__":
    h = HiveExecutor()
    print(f"Hive 在线: {h.online}")
    if h.online:
        r = h.execute("SELECT COUNT(*) FROM ods_vehicle_pass_di")
        print(r)
