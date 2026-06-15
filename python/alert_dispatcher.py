#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
告警通知调度引擎 — 本地开发/生产通用的告警分发中心

功能：
1. 模拟钉钉/邮件/短信 webhook 接收端（本地开发）
2. 告警规则引擎：根据 severity 自动升级
3. 去重抑制：同一告警 30 分钟内不重复发送
4. 日汇总报告：每天早上 9:00 生成汇总
5. 对接 config/alert_config.json 配置

用法：
    # 启动告警服务器（开发模式）
    python python/alert_dispatcher.py

    # 发送测试告警
    python python/alert_dispatcher.py --test

    # 生产模式（真实发送）
    python python/alert_dispatcher.py --production
"""

import json
import time
import os
import sys
import hashlib
import threading
from datetime import datetime, timedelta
from collections import defaultdict

# ============================================================================
# 配置加载
# ============================================================================
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "alert_config.json")

def load_config():
    """加载告警配置"""
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    # 默认配置
    return {
        "alert_config": {
            "enabled": True,
            "channels": {
                "dingtalk": {"enabled": True, "webhook_url": "http://localhost:9999/dingtalk"},
                "email": {"enabled": True, "smtp_host": "localhost", "smtp_port": 25},
                "sms": {"enabled": False}
            },
            "escalation_policy": {
                "CRITICAL": {"channels": ["dingtalk", "email", "sms"], "retry_interval_minutes": 5, "max_retry": 3},
                "MAJOR": {"channels": ["dingtalk", "email"], "retry_interval_minutes": 15, "max_retry": 2},
                "MINOR": {"channels": ["email"], "retry_interval_minutes": 0, "max_retry": 0}
            },
            "silence_rules": {
                "duplicate_suppression_minutes": 30,
                "daily_summary": {"enabled": True, "send_time": "09:00"}
            }
        },
        "quality_alert_rules": []
    }


# ============================================================================
# 告警记录器
# ============================================================================
class AlertHistory:
    """告警历史记录，提供去重和统计"""

    def __init__(self):
        self.alerts = []  # 全部告警记录
        self.sent_hashes = {}  # hash -> timestamp, 用于去重
        self.suppress_minutes = 30

    def should_send(self, alert_data):
        """检查是否应该发送（去重）"""
        alert_hash = hashlib.md5(
            json.dumps(alert_data, sort_keys=True).encode()
        ).hexdigest()

        now = datetime.now()
        if alert_hash in self.sent_hashes:
            last_sent = self.sent_hashes[alert_hash]
            if (now - last_sent).total_seconds() < self.suppress_minutes * 60:
                return False

        self.sent_hashes[alert_hash] = now
        return True

    def record(self, alert_data, channel, success):
        """记录告警发送历史"""
        self.alerts.append({
            "timestamp": datetime.now().isoformat(),
            "alert": alert_data,
            "channel": channel,
            "success": success
        })
        # 保留最近 10000 条
        if len(self.alerts) > 10000:
            self.alerts = self.alerts[-5000:]

    def get_summary(self, date=None):
        """获取指定日期的告警汇总"""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        day_alerts = [a for a in self.alerts if a["timestamp"].startswith(date)]
        severity_counts = defaultdict(int)
        channel_counts = defaultdict(int)

        for a in day_alerts:
            severity = a["alert"].get("severity", "MINOR")
            severity_counts[severity] += 1
            channel_counts[a["channel"]] += 1

        return {
            "date": date,
            "total": len(day_alerts),
            "by_severity": dict(severity_counts),
            "by_channel": dict(channel_counts),
            "alerts": day_alerts[-20:]  # 最近 20 条
        }


# ============================================================================
# 通知渠道
# ============================================================================
class NotificationChannel:
    """通知渠道基类"""

    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.enabled = config.get("enabled", True)

    def send(self, alert_data):
        """发送告警，返回 (success, message)"""
        raise NotImplementedError


class DingTalkChannel(NotificationChannel):
    """钉钉通知渠道"""

    def __init__(self, config):
        super().__init__("dingtalk", config)

    def _format_markdown(self, alert_data):
        severity_emoji = {"CRITICAL": "🔴", "MAJOR": "🟠", "MINOR": "🟡", "INFO": "🔵"}
        emoji = severity_emoji.get(alert_data.get("severity", "INFO"), "⚪")
        title = alert_data.get("title", "告警通知")
        message = alert_data.get("message", "")
        severity = alert_data.get("severity", "INFO")
        source = alert_data.get("source", "unknown")
        timestamp = alert_data.get("timestamp", datetime.now().isoformat())

        return f"""## {emoji} [{severity}] {title}

**消息**: {message}
**来源**: {source}
**时间**: {timestamp}
**建议**: {alert_data.get('suggestion', '请检查相关服务')}

---
*智慧城市交通数据治理平台 · 自动告警*
"""

    def send(self, alert_data):
        webhook_url = self.config.get("webhook_url", "")
        if not webhook_url or "YOUR_TOKEN" in webhook_url:
            # 开发模式：打印到控制台
            print(f"\n{'='*60}")
            print(f"[DingTalk Webhook] 告警发送中...")
            print(f"  URL: {webhook_url}")
            md = self._format_markdown(alert_data)
            print(md)
            print(f"{'='*60}\n")
            return True, "printed to console (dev mode)"

        try:
            import urllib.request
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "title": alert_data.get("title", "告警"),
                    "text": self._format_markdown(alert_data)
                }
            }
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(webhook_url, data=data,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
            return True, "sent"
        except Exception as e:
            return False, str(e)


class EmailChannel(NotificationChannel):
    """邮件通知渠道"""

    def __init__(self, config):
        super().__init__("email", config)

    def send(self, alert_data):
        smtp_host = self.config.get("smtp_host", "localhost")
        sender = self.config.get("sender", "noreply@traffic.com")
        recipients = self.config.get("recipients", {})

        subject = f"[{alert_data.get('severity', 'INFO')}] {alert_data.get('title', '告警')}"
        body = f"""
告警通知
========
级别: {alert_data.get('severity', 'INFO')}
标题: {alert_data.get('title', '')}
消息: {alert_data.get('message', '')}
来源: {alert_data.get('source', 'unknown')}
时间: {alert_data.get('timestamp', datetime.now().isoformat())}
建议: {alert_data.get('suggestion', '请检查相关服务')}

---
智慧城市交通数据治理平台 · 自动告警
"""

        if smtp_host == "localhost" and not os.environ.get("SMTP_HOST"):
            # 开发模式
            print(f"\n[Email] To: {recipients}")
            print(f"Subject: {subject}")
            print(body[:200] + "...")
            return True, "printed to console (dev mode)"

        # 真实 SMTP 发送（需要 smtplib）
        try:
            import smtplib
            from email.mime.text import MIMEText
            server = smtplib.SMTP(smtp_host, self.config.get("smtp_port", 25), timeout=10)
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = subject
            msg["From"] = sender
            to_addr = []
            for group in recipients.values():
                to_addr.extend(group if isinstance(group, list) else [group])
            msg["To"] = ", ".join(to_addr)
            server.sendmail(sender, to_addr, msg.as_string())
            server.quit()
            return True, "sent"
        except Exception as e:
            return False, str(e)


# ============================================================================
# 告警分发引擎
# ============================================================================
class AlertDispatcher:
    """告警分发中心"""

    def __init__(self, config=None):
        self.config = config or load_config()
        self.alert_config = self.config.get("alert_config", {})
        self.quality_rules = self.config.get("quality_alert_rules", [])
        self.history = AlertHistory()

        # 初始化通知渠道
        channels_config = self.alert_config.get("channels", {})
        self.channels = {
            "dingtalk": DingTalkChannel(channels_config.get("dingtalk", {})),
            "email": EmailChannel(channels_config.get("email", {}))
        }

        # 告警升级策略
        self.escalation = self.alert_config.get("escalation_policy", {})
        self.history.suppress_minutes = self.alert_config.get(
            "silence_rules", {}).get("duplicate_suppression_minutes", 30)

        self._daily_summary_timer = None
        self._start_daily_summary()

    def dispatch(self, alert_data):
        """分发告警到所有配置的渠道"""
        if not self.alert_config.get("enabled", True):
            print("[AlertDispatcher] 告警功能已禁用")
            return []

        severity = alert_data.get("severity", "MINOR")

        # 去重检查
        if not self.history.should_send(alert_data):
            print(f"[AlertDispatcher] 告警已抑制（去重）: {alert_data.get('title', '')}")
            return []

        # 获取升级策略
        policy = self.escalation.get(severity, self.escalation.get("MINOR", {}))
        target_channels = policy.get("channels", ["email"])

        results = []
        for ch_name in target_channels:
            if ch_name in self.channels:
                channel = self.channels[ch_name]
                success, msg = channel.send(alert_data)
                self.history.record(alert_data, ch_name, success)
                results.append({"channel": ch_name, "success": success, "message": msg})
                if success:
                    print(f"[AlertDispatcher] ✅ {severity} → {ch_name}: {alert_data.get('title', '')}")
                else:
                    print(f"[AlertDispatcher] ❌ {severity} → {ch_name}: {msg}")

        # 升级处理
        if policy.get("escalation_timeout_minutes"):
            self._schedule_escalation(alert_data, policy)

        return results

    def dispatch_quality_alert(self, rule_id, actual_value, table_name, dt):
        """根据质量规则触发告警"""
        rule = None
        for r in self.quality_rules:
            if r["rule_id"] == rule_id:
                rule = r
                break

        if not rule:
            print(f"[AlertDispatcher] 规则不存在: {rule_id}")
            return []

        threshold = rule.get("threshold_pct", rule.get("threshold_lag", 0))
        severity = rule.get("severity", "MAJOR")

        # 判断是否触发
        if isinstance(actual_value, (int, float)) and actual_value <= threshold:
            print(f"[AlertDispatcher] 指标正常，不触发告警: {rule_id} actual={actual_value} threshold={threshold}")
            return []

        message_template = rule.get("message_template", "")
        message = message_template.format(
            table=table_name, dt=dt, actual=actual_value,
            threshold=threshold, null_fields="N/A",
            abnormal_pct=actual_value, duplicate_count=actual_value,
            topic=rule.get("target_topic", ""), lag=actual_value
        )

        alert_data = {
            "rule_id": rule_id,
            "title": rule.get("name", "质量告警"),
            "message": message,
            "severity": severity,
            "source": "data_quality_monitor",
            "timestamp": datetime.now().isoformat(),
            "suggestion": "请检查数据质量并排查上游问题"
        }

        return self.dispatch(alert_data)

    def send_custom_alert(self, title, message, severity="MINOR", source="manual", suggestion=""):
        """发送自定义告警"""
        return self.dispatch({
            "title": title,
            "message": message,
            "severity": severity,
            "source": source,
            "timestamp": datetime.now().isoformat(),
            "suggestion": suggestion or "请检查相关服务"
        })

    def _schedule_escalation(self, alert_data, policy):
        """安排告警升级"""
        timeout = policy.get("escalation_timeout_minutes", 30)
        escalate_to = policy.get("escalation_recipients", [])

        def escalate():
            time.sleep(timeout * 60)
            escalated = dict(alert_data)
            escalated["severity"] = "CRITICAL"
            escalated["title"] = f"[升级] {alert_data.get('title', '')}"
            escalated["message"] = f"告警超过{timeout}分钟未处理，已自动升级\n原消息: {alert_data.get('message', '')}"
            self.dispatch(escalated)

        t = threading.Thread(target=escalate, daemon=True)
        t.start()

    def _start_daily_summary(self):
        """启动每日汇总定时器"""
        daily_cfg = self.alert_config.get("silence_rules", {}).get("daily_summary", {})
        if not daily_cfg.get("enabled", True):
            return

        def daily_task():
            while True:
                now = datetime.now()
                target = now.replace(hour=9, minute=0, second=0, microsecond=0)
                if now >= target:
                    target += timedelta(days=1)
                wait = (target - now).total_seconds()
                time.sleep(wait)

                summary = self.history.get_summary()
                self.send_custom_alert(
                    title="每日告警汇总",
                    message=f"""
昨日告警总数: {summary['total']}
按级别: {json.dumps(summary['by_severity'], ensure_ascii=False)}
按渠道: {json.dumps(summary['by_channel'], ensure_ascii=False)}
                    """.strip(),
                    severity="INFO",
                    source="daily_summary"
                )

        self._daily_summary_timer = threading.Thread(target=daily_task, daemon=True)
        self._daily_summary_timer.start()

    def get_history(self, date=None):
        """获取告警历史"""
        return self.history.get_summary(date)


# ============================================================================
# Webhook 模拟服务器（开发模式）
# ============================================================================
def run_webhook_server(port=9999):
    """启动本地 webhook 接收服务器，用于开发调试"""
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class WebhookHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')

            print(f"\n{'='*60}")
            print(f"[Webhook] {self.path}")
            print(f"Headers: {dict(self.headers)}")
            try:
                data = json.loads(body)
                print(f"Payload: {json.dumps(data, indent=2, ensure_ascii=False)}")
            except json.JSONDecodeError:
                print(f"Body: {body[:500]}")

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"errcode": 0, "errmsg": "ok"}).encode())

        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok","service":"alert-webhook-simulator"}')

        def log_message(self, format, *args):
            pass  # 禁止 HTTP 日志

    server = HTTPServer(('0.0.0.0', port), WebhookHandler)
    print(f"\n🔔 告警 Webhook 模拟服务器启动在 http://localhost:{port}")
    print(f"   钉钉:  http://localhost:{port}/dingtalk")
    print(f"   邮件:  http://localhost:{port}/email")
    print(f"   (按 Ctrl+C 停止)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n服务器已停止")


# ============================================================================
# 命令行入口
# ============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="告警通知调度引擎")
    parser.add_argument("--test", action="store_true", help="发送测试告警")
    parser.add_argument("--webhook", action="store_true", help="启动 Webhook 模拟服务器")
    parser.add_argument("--production", action="store_true", help="生产模式")
    parser.add_argument("--port", type=int, default=9999, help="Webhook 服务器端口")
    args = parser.parse_args()

    if args.webhook:
        run_webhook_server(args.port)
        sys.exit(0)

    if args.test:
        dispatcher = AlertDispatcher()
        print("=" * 60)
        print("  发送测试告警")
        print("=" * 60)

        # 测试 1: MINOR 告警
        print("\n--- 测试 1: MINOR 告警（仅邮件）---")
        dispatcher.send_custom_alert(
            title="数据完整率略降",
            message="ods_vehicle_pass_di 完整率 98.5%，低于阈值 99%",
            severity="MINOR",
            source="data_quality_monitor",
            suggestion="建议检查上游数据采集"
        )

        time.sleep(1)

        # 测试 2: MAJOR 告警
        print("\n--- 测试 2: MAJOR 告警（钉钉+邮件）---")
        dispatcher.send_custom_alert(
            title="Kafka 消费延迟过高",
            message="traffic_vehicle Topic 消费延迟 12500 条，超过阈值 10000 条",
            severity="MAJOR",
            source="kafka_monitor",
            suggestion="请检查 Flink Consumer 是否正常"
        )

        time.sleep(1)

        # 测试 3: CRITICAL 告警
        print("\n--- 测试 3: CRITICAL 告警（全渠道+升级）---")
        dispatcher.send_custom_alert(
            title="Flink JobManager 不可用",
            message="Flink JobManager :8081 端口无法访问，实时计算已中断！",
            severity="CRITICAL",
            source="service_monitor",
            suggestion="立即检查 Flink 集群状态，准备切换备用 JM"
        )

        time.sleep(1)

        # 测试 4: 去重
        print("\n--- 测试 4: 去重验证（同一告警应被抑制）---")
        dispatcher.send_custom_alert(
            title="数据完整率略降",
            message="ods_vehicle_pass_di 完整率 98.5%",
            severity="MINOR",
            source="data_quality_monitor"
        )

        # 显示历史
        print("\n" + "=" * 60)
        print("  告警历史")
        print("=" * 60)
        summary = dispatcher.get_history()
        print(f"  总数: {summary['total']}")
        print(f"  按级别: {summary['by_severity']}")
        print(f"  按渠道: {summary['by_channel']}")
        sys.exit(0)

    if args.production:
        dispatcher = AlertDispatcher()
        print("🚨 告警分发引擎已启动（生产模式）")
        print("   配置来源: config/alert_config.json")
        print("   按 Ctrl+C 停止")
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            print("\n引擎已停止")
        sys.exit(0)

    # 默认：启动交互式测试
    print("用法:")
    print("  python python/alert_dispatcher.py --test       # 发送测试告警")
    print("  python python/alert_dispatcher.py --webhook    # 启动 webhook 模拟服务器")
    print("  python python/alert_dispatcher.py --production # 生产模式")
