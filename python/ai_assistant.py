"""
AI 数据查询助手

三合一：
1. 尝试 Ollama (Qwen3) 生成 SQL → 真实 AI
2. 降级到规则引擎 NL2SQL → 兜底
3. 在 Hive 上执行 → 返回结果

用法：
  python ai_assistant.py "昨天最拥堵的5条路"
  python ai_assistant.py "设备健康评分最低的3台设备" --no-ollama
"""

import json
import sys
import urllib.request
import urllib.error
from typing import Optional

from hive_executor import HiveExecutor
from nl2sql_enhanced import NL2SQLConverter

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen3:8b"


class AIAssistant:
    """AI 数据查询助手"""

    def __init__(self):
        self.hive = HiveExecutor()
        self.fallback = NL2SQLConverter()
        self._ollama_ok = None

    @property
    def ollama_online(self) -> bool:
        """检查 Ollama 是否可用"""
        if self._ollama_ok is not None:
            return self._ollama_ok
        try:
            req = urllib.request.Request(
                "http://localhost:11434/api/tags",
                method="GET"
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                data = json.loads(resp.read())
                models = [m["name"] for m in data.get("models", [])]
                self._ollama_ok = any(OLLAMA_MODEL in m for m in models)
        except Exception:
            self._ollama_ok = False
        return self._ollama_ok

    def ask(self, question: str, use_ollama: bool = True) -> dict:
        """
        问问题，返回结果

        返回: {
            "question": str,       # 原始问题
            "sql": str,            # 生成的 SQL
            "result": {...},       # Hive 执行结果
            "mode": str,           # "ollama" / "fallback" / "error"
            "error": str,          # 错误信息（如果有）
        }
        """
        sql = None
        mode = None
        error = None

        # 第1步：尝试 Ollama
        if use_ollama and self.ollama_online:
            sql, error = self._ask_ollama(question)

        # 第2步：降级到规则引擎
        if sql is None:
            try:
                sql = self.fallback.to_sql(question)
                mode = "fallback"
            except Exception as e:
                error = f"规则引擎也失败了: {e}"
                mode = "error"

        if sql is None:
            return {
                "question": question,
                "sql": None,
                "result": None,
                "mode": "error",
                "error": error or "无法生成 SQL"
            }

        # 第3步：在 Hive 上执行
        result = self.hive.execute(sql)

        return {
            "question": question,
            "sql": sql,
            "result": result,
            "mode": mode or ("ollama" if use_ollama and self.ollama_online else "fallback"),
            "error": error or result.get("error", "")
        }

    def _ask_ollama(self, question: str) -> tuple:
        """调用 Ollama 生成 SQL——简化版，只保留核心表名和字段"""
        prompt = f"""你是 Hive SQL 专家。根据问题生成 Hive SQL。

核心表：
- ads_top_jam_roads(rank_num,road_id,road_name,avg_jam_level,total_traffic_flow)
- ads_device_health_score(device_id,health_score,online_rate,health_level)
- dws_road_hour_flow(road_id,hour,traffic_count,avg_speed)
- dws_device_health_day(device_id,online_duration,offline_count,avg_cpu_usage,abnormal_count)
- dws_alarm_day(alarm_type,total_alarm_count,recovery_rate)
- dim_road_zip(road_id,road_name,road_type)
- dim_device_zip(device_id,device_name,device_type)
- dim_area(area_id,area_name)

要求：
1. 只返回 SQL，不要解释或 markdown
2. 分区用 dt = '2026-06-10'
3. LIMIT <= 20
4. 不加 traffic_db. 前缀

问题：{question}
SQL："""

        payload = json.dumps({
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "temperature": 0.1,
            "num_predict": 512
        }).encode("utf-8")

        try:
            req = urllib.request.Request(
                OLLAMA_URL,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read())
                raw = data.get("response", "").strip()

            debug_log = raw  # 保存原始响应用于调试

            # 去掉 markdown ``` 包裹
            sql = raw
            if "```" in sql:
                for part in sql.split("```"):
                    p = part.strip().lstrip("sql").lstrip("SQL").strip()
                    if p:
                        sql = p
                        break

            # 去掉可能的 "SQL:" 前缀
            sql = sql.strip()
            if sql.upper().startswith("SQL"):
                sql = sql[sql.index(":")+1:] if ":" in sql[:6] else sql[3:]
            sql = sql.strip()
            if not sql.endswith(";"):
                sql += ";"

            # 验证包含 SQL 关键词
            if any(k in sql.upper() for k in ["SELECT", "SHOW"]):
                return sql, None

            return None, f"Ollama 未生成有效 SQL: {debug_log[:200]}"

        except urllib.error.HTTPError as e:
            return None, f"Ollama HTTP 错误: {e.code}"
        except urllib.error.URLError as e:
            return None, f"Ollama 连接失败: {e.reason}"
        except Exception as e:
            return None, f"Ollama 调用异常: {e}"

    def ask_simple(self, question: str) -> str:
        """简化接口：返回可读的结果文本"""
        answer = self.ask(question)

        lines = []
        lines.append(f"问题: {answer['question']}")
        lines.append(f"模式: {'🤖 AI模型(Ollama)' if answer['mode'] == 'ollama' else '⚙️ 规则引擎'}")
        lines.append("")

        if answer['sql']:
            lines.append(f"SQL:")
            lines.append(f"  {answer['sql']}")
            lines.append("")

        if answer['result']:
            r = answer['result']
            if r.get('note'):
                lines.append(f"⚠️ {r['note']}")
                lines.append("")
            if r.get('columns') and r.get('rows'):
                # 表头
                header = "  " + " | ".join(str(c) for c in r['columns'])
                lines.append(header)
                lines.append("  " + "-" * len(header))
                # 数据行
                for row in r['rows'][:20]:
                    lines.append("  " + " | ".join(str(c) for c in row))
                lines.append(f"  ({len(r['rows'])} 行)")
            if r.get('error'):
                lines.append(f"错误: {r['error']}")

        if answer['error'] and not answer['result']:
            lines.append(f"错误: {answer['error']}")

        return "\n".join(lines)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="AI 数据查询助手")
    parser.add_argument("question", nargs="?", help="自然语言问题")
    parser.add_argument("--no-ollama", action="store_true", help="强制使用规则引擎")
    parser.add_argument("--interactive", "-i", action="store_true", help="交互模式")

    args = parser.parse_args()
    assistant = AIAssistant()

    if args.interactive:
        print("🤖 AI 数据查询助手 (输入 'exit' 退出)")
        print(f"   模型: {OLLAMA_MODEL}")
        print(f"   Ollama: {'✅ 在线' if assistant.ollama_online else '❌ 离线（将用规则引擎）'}")
        print(f"   Hive: {'✅ 在线' if assistant.hive.online else '❌ 离线（仅展示 SQL）'}")
        print()
        while True:
            try:
                q = input("请输入问题 > ").strip()
                if q.lower() in ("exit", "quit", "q"):
                    break
                if not q:
                    continue
                print()
                print(assistant.ask_simple(q))
                print()
            except KeyboardInterrupt:
                break
            except EOFError:
                break
        return

    if args.question:
        print(assistant.ask_simple(args.question))
    else:
        # Demo 模式
        demo_questions = [
            "昨天最拥堵的5条路",
            "今天车流量最大的3条道路",
            "设备健康评分最低的3台设备",
        ]
        print("🤖 AI 数据查询助手 Demo\n")
        for q in demo_questions:
            print("=" * 60)
            print(assistant.ask_simple(q))
            print()


if __name__ == "__main__":
    main()
