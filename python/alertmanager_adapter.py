#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlertManager → 钉钉 告警桥接适配器

接收 Prometheus AlertManager 的 Webhook 告警，转换为钉钉 Markdown 格式并发送。
支持 HMAC-SHA256 签名验证。

启动: python python/alertmanager_adapter.py [--port 5000]
"""

import json
import os
import sys
import time
import urllib.request
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

# 尝试加载配置
_alert_config = None


def load_config():
    global _alert_config
    if _alert_config is not None:
        return _alert_config
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                               "config", "alert_config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            _alert_config = json.load(f).get("alert_config", {})
    except Exception:
        _alert_config = {}
    return _alert_config


def get_dingtalk_webhook():
    """获取带签名的钉钉 Webhook URL"""
    config = load_config()
    dingtalk_cfg = config.get("channels", {}).get("dingtalk", {})
    webhook_url = dingtalk_cfg.get("webhook_url", "")
    signature_enabled = dingtalk_cfg.get("signature_enabled", False)
    secret = dingtalk_cfg.get("signature_secret", "")

    if signature_enabled and secret and "YOUR" not in secret:
        try:
            from python.dingtalk_signer import DingTalkSigner
            signer = DingTalkSigner(secret)
            return signer.sign_url(webhook_url)
        except ImportError:
            pass
    return webhook_url


def format_alertmanager_to_dingtalk(payload: dict) -> dict:
    """将 AlertManager Webhook JSON 转换为钉钉 Markdown 消息格式"""
    status = payload.get("status", "firing")
    alerts = payload.get("alerts", [])
    external_url = payload.get("externalURL", "")

    if not alerts:
        return None

    status_emoji = "🔴" if status == "firing" else "🟢"
    status_text = "告警触发" if status == "firing" else "告警恢复"

    markdown_lines = [
        f"## {status_emoji} {status_text}",
        f"**时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**告警数量:** {len(alerts)} 条",
        "",
    ]

    for i, alert in enumerate(alerts[:10], 1):  # 最多10条
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        alertname = labels.get("alertname", "Unknown")
        severity = labels.get("severity", "unknown")
        instance = labels.get("instance", "-")
        summary = annotations.get("summary", "")
        description = annotations.get("description", "")
        suggestion = annotations.get("suggestion", "")

        sev_emoji = {"critical": "🔴", "major": "🟠", "warning": "🟡"}.get(severity, "⚪")

        markdown_lines.append(f"### {sev_emoji} [{severity.upper()}] {alertname}")
        markdown_lines.append(f"- **实例:** {instance}")
        if summary:
            markdown_lines.append(f"- **摘要:** {summary}")
        if description:
            markdown_lines.append(f"- **详情:** {description}")
        if suggestion:
            markdown_lines.append(f"- **建议:** {suggestion}")
        markdown_lines.append("")

    if len(alerts) > 10:
        markdown_lines.append(f"> ... 还有 {len(alerts) - 10} 条告警未显示")

    title = f"[{status.upper()}] {alerts[0].get('labels', {}).get('alertname', 'Alert')}"
    return {
        "msgtype": "markdown",
        "markdown": {
            "title": title,
            "text": "\n".join(markdown_lines),
        },
    }


def send_to_dingtalk(webhook_url: str, dingtalk_payload: dict) -> bool:
    """发送消息到钉钉机器人"""
    if not webhook_url or "localhost" in webhook_url or "9999" in webhook_url:
        # 开发模式 — 打印到控制台
        print(f"[DEV MODE] 钉钉消息:")
        print(json.dumps(dingtalk_payload, ensure_ascii=False, indent=2))
        return True

    try:
        data = json.dumps(dingtalk_payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            result = json.loads(body)
            if result.get("errcode") == 0:
                print(f"[DingTalk] 发送成功: {dingtalk_payload['markdown']['title']}")
                return True
            else:
                print(f"[DingTalk] 发送失败: {result.get('errmsg', body)}")
                return False
    except Exception as e:
        print(f"[DingTalk] 发送异常: {e}")
        return False


class AlertManagerHandler(BaseHTTPRequestHandler):
    """AlertManager Webhook HTTP 处理器"""

    def do_POST(self):
        if self.path in ("/alertmanager/dingtalk", "/dingtalk"):
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            try:
                alertmanager_payload = json.loads(body.decode("utf-8"))
                dingtalk_payload = format_alertmanager_to_dingtalk(alertmanager_payload)
                if dingtalk_payload:
                    webhook_url = get_dingtalk_webhook()
                    send_to_dingtalk(webhook_url, dingtalk_payload)
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'{"status":"ok"}')
                else:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'{"status":"no_alerts"}')
            except Exception as e:
                print(f"[Adapter] 处理失败: {e}")
                self.send_response(400)
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

    def do_GET(self):
        if self.path in ("/health", "/"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps({
                "service": "alertmanager-dingtalk-adapter",
                "status": "ok",
                "dingtalk_webhook": "localhost" in get_dingtalk_webhook() and "(dev mode)" or "(production)",
            }).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # 静默日志


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    webhook_url = get_dingtalk_webhook()
    is_dev = "localhost" in webhook_url or "9999" in webhook_url

    print("=" * 60)
    print("  AlertManager → 钉钉 告警桥接适配器")
    print(f"  端口: {port}")
    print(f"  模式: {'开发(控制台输出)' if is_dev else '生产(真实钉钉)'}")
    print(f"  Webhook: POST /alertmanager/dingtalk")
    print(f"  Health:  GET  /health")
    print("=" * 60)

    server = HTTPServer(("0.0.0.0", port), AlertManagerHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Adapter] 已停止")
        server.shutdown()


if __name__ == "__main__":
    main()
