#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
告警 Webhook 模拟接收器 — 本地开发调试用

模拟钉钉、邮件、短信等告警渠道的接收端。
接收到的所有告警会：
1. 打印到控制台（彩色）
2. 保存到本地 JSON 文件（alert_history.json）
3. 提供简单的 Web UI 查看历史

用法:
    python python/alert_webhook_server.py
    # 然后配置 alert_config.json 中的 webhook_url 为 http://localhost:9999/dingtalk
"""

import json
import os
import sys
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

HISTORY_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "alert_history.json")
PORT = 9999
alerts_received = []


def load_history():
    global alerts_received
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                alerts_received = json.load(f)
        except Exception:
            alerts_received = []


def save_history():
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(alerts_received[-1000:], f, ensure_ascii=False, indent=2)


def format_alert(alert_data, channel):
    """格式化告警输出"""
    severity_colors = {
        "CRITICAL": "\033[1;41m",  # 红底
        "MAJOR": "\033[1;31m",     # 红色
        "MINOR": "\033[1;33m",     # 黄色
        "INFO": "\033[1;34m",      # 蓝色
    }
    reset = "\033[0m"
    color = severity_colors.get(alert_data.get("severity", "INFO"), "")

    lines = [
        f"\n{'='*70}",
        f"{color}[{alert_data.get('severity', 'INFO')}] {alert_data.get('title', 'No Title')}{reset}",
        f"  Channel:   {channel}",
        f"  Time:      {alert_data.get('timestamp', 'N/A')}",
        f"  Source:    {alert_data.get('source', 'unknown')}",
        f"  Message:   {alert_data.get('message', '')}",
        f"  Suggestion:{alert_data.get('suggestion', 'N/A')}",
        f"{'='*70}",
    ]
    return "\n".join(lines)


class AlertHandler(BaseHTTPRequestHandler):
    """HTTP 请求处理器"""

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8')

        channel = self.path.strip('/') or "unknown"
        timestamp = datetime.now().isoformat()

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"raw": body[:500]}

        # 提取告警信息
        if "msgtype" in payload:  # 钉钉格式
            alert_data = {
                "title": payload.get("markdown", {}).get("title", "No Title"),
                "message": payload.get("markdown", {}).get("text", body[:200]),
                "source": "dingtalk_webhook",
                "timestamp": timestamp,
            }
        else:
            alert_data = {
                "title": payload.get("title", "Webhook Alert"),
                "message": payload.get("message", json.dumps(payload, ensure_ascii=False)),
                "source": payload.get("source", f"webhook_{channel}"),
                "timestamp": timestamp,
                "severity": payload.get("severity", "INFO"),
                "suggestion": payload.get("suggestion", ""),
            }

        # 记录并输出
        record = {"received_at": timestamp, "channel": channel, "alert": alert_data}
        alerts_received.append(record)
        print(format_alert(alert_data, channel))
        save_history()

        # 响应
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({"errcode": 0, "errmsg": "ok"}).encode())

    def do_GET(self):
        path = self.path.strip('/')

        if path == "" or path == "status":
            # 状态页
            data = {
                "service": "alert-webhook-simulator",
                "status": "running",
                "total_received": len(alerts_received),
                "recent_alerts": alerts_received[-10:],
            }
        elif path == "history":
            # 历史页 - 简单 HTML
            html = f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8">
<title>告警历史</title><style>
body{{background:#0a0e27;color:#ccc;font-family:monospace;padding:20px}}
h1{{color:#4fc3f7}} table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{background:#1a2744;padding:8px;text-align:left}}
td{{padding:6px 8px;border-bottom:1px solid #1a2744}}
.crit{{background:#4a1a1a}}.maj{{background:#5d4037}}.min{{background:#1a2744}}
.rec{{background:transparent}}
</style></head><body><h1>🔔 告警历史 (最近 100 条)</h1>
<table><tr><th>时间</th><th>级别</th><th>渠道</th><th>标题</th><th>消息</th></tr>
"""
            for r in reversed(alerts_received[-100:]):
                a = r.get("alert", {})
                sev = a.get("severity", "INFO")
                cls = "crit" if sev == "CRITICAL" else "maj" if sev == "MAJOR" else "min"
                msg = a.get("message", "")[:80]
                html += f'<tr class="{cls}"><td>{r["received_at"][:19]}</td><td>{sev}</td><td>{r["channel"]}</td><td>{a.get("title","")[:40]}</td><td>{msg}</td></tr>'
            html += "</table></body></html>"
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(html.encode())
            return
        else:
            data = {"error": "not found", "path": path}

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False, indent=2).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def log_message(self, format, *args):
        pass  # 禁止标准 HTTP 日志


def main():
    load_history()
    print("=" * 70)
    print("  🔔 告警 Webhook 模拟接收器")
    print("=" * 70)
    print(f"  监听端口: {PORT}")
    print(f"  Web UI:   http://localhost:{PORT}/history")
    print(f"  状态 API: http://localhost:{PORT}/status")
    print(f"  已接收:   {len(alerts_received)} 条历史告警")
    print()
    print("  可用的 webhook 端点:")
    print(f"    钉钉:  http://localhost:{PORT}/dingtalk")
    print(f"    邮件:  http://localhost:{PORT}/email")
    print(f"    短信:  http://localhost:{PORT}/sms")
    print(f"    通用:  http://localhost:{PORT}/generic")
    print()
    print("  配置方法:")
    print(f"    将 config/alert_config.json 中的 webhook_url")
    print(f"    改为 http://localhost:{PORT}/dingtalk")
    print()
    print("  按 Ctrl+C 停止")
    print("=" * 70)

    server = HTTPServer(('0.0.0.0', PORT), AlertHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n\n服务器已停止。共接收 {} 条告警。".format(len(alerts_received)))
        save_history()
        server.server_close()


if __name__ == "__main__":
    main()
