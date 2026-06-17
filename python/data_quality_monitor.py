import os
import sys
import json
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from collections import defaultdict

try:
    from pyhive import hive
except ImportError:
    hive = None
try:
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None


class AlertNotifier:
    """告警通知模块：支持钉钉/邮件/短信多渠道推送"""

    def __init__(self, config_path='config/alert_config.json'):
        self.config = self._load_config(config_path)
        self.alert_history = defaultdict(list)  # 去重抑制

    def _load_config(self, config_path):
        default = {
            "enabled": True,
            "channels": {
                "dingtalk": {"enabled": False, "webhook_url": ""},
                "email": {"enabled": False, "smtp_host": "smtp.company.com",
                          "smtp_port": 587, "sender": "data-platform@company.com"}
            },
            "escalation_policy": {
                "CRITICAL": {"channels": ["dingtalk", "email"]},
                "MAJOR": {"channels": ["dingtalk", "email"]},
                "MINOR": {"channels": ["email"]}
            },
            "silence_rules": {
                "duplicate_suppression_minutes": 30
            }
        }
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                return loaded.get('alert_config', default)
        except (FileNotFoundError, json.JSONDecodeError):
            return default

    def send_dingtalk(self, title, content, severity='MAJOR', at_all=False):
        """钉钉机器人推送（支持markdown格式 + HMAC-SHA256签名）"""
        dingtalk_cfg = self.config.get('channels', {}).get('dingtalk', {})
        webhook = dingtalk_cfg.get('webhook_url', '')
        if not webhook:
            print("[DingTalk] Webhook未配置，跳过")
            return False

        # HMAC-SHA256 签名
        signature_enabled = dingtalk_cfg.get('signature_enabled', False)
        secret = dingtalk_cfg.get('signature_secret', '')
        if signature_enabled and secret and 'YOUR' not in secret:
            try:
                from python.dingtalk_signer import DingTalkSigner
                signer = DingTalkSigner(secret)
                webhook = signer.sign_url(webhook)
            except ImportError:
                pass

        markdown_text = f"## {title}\n\n{content}\n\n> 告警级别: {severity}\n> 告警时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        payload = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": markdown_text},
            "at": {"isAtAll": at_all and severity == 'CRITICAL'}
        }
        try:
            resp = requests.post(webhook, json=payload, timeout=10)
            if resp.status_code == 200:
                print(f"[DingTalk] 推送成功: {title}")
                return True
            print(f"[DingTalk] 推送失败: {resp.text}")
            return False
        except Exception as e:
            print(f"[DingTalk] 推送异常: {e}")
            return False

    def send_email(self, subject, content, recipients, severity='MAJOR'):
        """邮件推送"""
        email_cfg = self.config.get('channels', {}).get('email', {})
        if not email_cfg.get('enabled'):
            print("[Email] 未启用，跳过")
            return False

        msg = MIMEMultipart()
        msg['From'] = email_cfg.get('sender', 'data-platform@company.com')
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = f"[{severity}] {subject}"

        html_content = f"""
        <h2>{subject}</h2>
        <pre>{content}</pre>
        <hr>
        <p>告警级别: {severity} | 告警时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>此邮件由交通数据治理平台自动发送，请勿回复。</p>
        """
        msg.attach(MIMEText(html_content, 'html', 'utf-8'))

        try:
            with smtplib.SMTP(email_cfg.get('smtp_host'), email_cfg.get('smtp_port')) as server:
                server.starttls()
                server.login(email_cfg.get('sender'), email_cfg.get('password', ''))
                server.sendmail(email_cfg.get('sender'), recipients, msg.as_string())
            print(f"[Email] 发送成功: {subject}")
            return True
        except Exception as e:
            print(f"[Email] 发送异常: {e}")
            return False

    def notify(self, title, content, severity='MAJOR', at_all=False):
        """统一告警入口，根据告警级别路由到不同渠道"""
        if not self.config.get('enabled', True):
            print(f"[Alert] 告警已全局关闭，跳过: {title}")
            return

        # 去重抑制：30分钟内相同告警不重复推送
        dedup_key = f"{severity}:{title}"
        now = datetime.now()
        if dedup_key in self.alert_history:
            last_time = max(self.alert_history[dedup_key])
            if (now - last_time).total_seconds() < self.config.get('silence_rules', {}).get(
                    'duplicate_suppression_minutes', 30) * 60:
                print(f"[Alert] 重复告警抑制: {title}")
                return
        self.alert_history[dedup_key].append(now)

        # 清理过期历史（保留最近1小时）
        self.alert_history[dedup_key] = [
            t for t in self.alert_history[dedup_key]
            if (now - t).total_seconds() < 3600
        ]

        # 根据告警级别选择推送渠道
        channels = self.config.get('escalation_policy', {}).get(severity, {}).get('channels', ['email'])

        for channel in channels:
            if channel == 'dingtalk':
                self.send_dingtalk(title, content, severity, at_all)
            elif channel == 'email':
                recipients_cfg = self.config.get('channels', {}).get('email', {}).get('recipients', {})
                recipients = recipients_cfg.get('data_team', ['data-team@company.com'])
                self.send_email(title, content, recipients, severity)

        print(f"[Alert] 告警已推送: [{severity}] {title}")


class DataQualityMonitor:
    """数据质量监控器（增强版：含告警联动）"""

    def __init__(self, hive_host='localhost', hive_port=10000, kafka_broker='localhost:9092'):
        self.hive_host = hive_host
        self.hive_port = hive_port
        self.kafka_broker = kafka_broker
        self.quality_results = []
        self.alert_notifier = AlertNotifier()

    def check_completeness(self, table_name, partition_date):
        """完整性检查：空值率"""
        try:
            conn = hive.Connection(host=self.hive_host, port=self.hive_port)
            cursor = conn.cursor()

            query = f"""
                SELECT 
                    COUNT(*) as total_count,
                    SUM(CASE WHEN vehicle_id IS NULL THEN 1 ELSE 0 END) as vehicle_id_null,
                    SUM(CASE WHEN road_id IS NULL THEN 1 ELSE 0 END) as road_id_null,
                    SUM(CASE WHEN device_id IS NULL THEN 1 ELSE 0 END) as device_id_null,
                    SUM(CASE WHEN pass_time IS NULL THEN 1 ELSE 0 END) as pass_time_null
                FROM {table_name}
                WHERE dt = '{partition_date}'
            """
            cursor.execute(query)
            result = cursor.fetchone()

            total_count, vehicle_null, road_null, device_null, time_null = result
            completeness_rate = (total_count - vehicle_null - road_null - device_null - time_null) / max(total_count, 1) * 100

            record = {
                'check_type': 'completeness',
                'table_name': table_name,
                'partition_date': partition_date,
                'total_count': total_count,
                'null_counts': {
                    'vehicle_id': vehicle_null,
                    'road_id': road_null,
                    'device_id': device_null,
                    'pass_time': time_null
                },
                'completeness_rate': round(completeness_rate, 2),
                'status': 'PASS' if completeness_rate >= 99 else 'FAIL'
            }
            self.quality_results.append(record)
            conn.close()

            # 不通过时触发告警
            if record['status'] == 'FAIL':
                null_fields = [k for k, v in record['null_counts'].items() if v > 0]
                self.alert_notifier.notify(
                    f"[数据质量告警] {table_name} 完整率不达标",
                    f"表: {table_name}\n分区: {partition_date}\n完整率: {completeness_rate:.2f}% (阈值 99%)\n"
                    f"空值字段: {', '.join(null_fields)}\n总记录数: {total_count}",
                    severity='MAJOR'
                )

        except Exception as e:
            self.quality_results.append({
                'check_type': 'completeness', 'table_name': table_name,
                'partition_date': partition_date, 'error': str(e), 'status': 'ERROR'
            })

    def check_uniqueness(self, table_name, partition_date, unique_columns):
        """唯一性检查：重复记录"""
        try:
            conn = hive.Connection(host=self.hive_host, port=self.hive_port)
            cursor = conn.cursor()

            columns_str = ', '.join(unique_columns)
            query = f"""
                SELECT COUNT(*) as duplicate_count
                FROM (
                    SELECT {columns_str}, COUNT(*) as cnt
                    FROM {table_name}
                    WHERE dt = '{partition_date}'
                    GROUP BY {columns_str}
                    HAVING cnt > 1
                ) t
            """
            cursor.execute(query)
            duplicate_count = cursor.fetchone()[0]

            query_total = f"SELECT COUNT(*) FROM {table_name} WHERE dt = '{partition_date}'"
            cursor.execute(query_total)
            total_count = cursor.fetchone()[0]

            uniqueness_rate = (total_count - duplicate_count) / max(total_count, 1) * 100

            record = {
                'check_type': 'uniqueness', 'table_name': table_name,
                'partition_date': partition_date, 'unique_columns': unique_columns,
                'duplicate_count': duplicate_count,
                'uniqueness_rate': round(uniqueness_rate, 2),
                'status': 'PASS' if uniqueness_rate >= 99.9 else 'FAIL'
            }
            self.quality_results.append(record)
            conn.close()

            if record['status'] == 'FAIL':
                self.alert_notifier.notify(
                    f"[数据质量告警] {table_name} 存在重复记录",
                    f"表: {table_name}\n分区: {partition_date}\n重复记录数: {duplicate_count}\n"
                    f"唯一率: {uniqueness_rate:.2f}% (阈值 99.9%)",
                    severity='MAJOR'
                )

        except Exception as e:
            self.quality_results.append({
                'check_type': 'uniqueness', 'table_name': table_name,
                'partition_date': partition_date, 'error': str(e), 'status': 'ERROR'
            })

    def check_validity(self, table_name, partition_date):
        """准确性检查：值域校验"""
        try:
            conn = hive.Connection(host=self.hive_host, port=self.hive_port)
            cursor = conn.cursor()

            query = f"""
                SELECT 
                    SUM(CASE WHEN speed < 0 OR speed > 200 THEN 1 ELSE 0 END) as invalid_speed,
                    SUM(CASE WHEN jam_level < 1 OR jam_level > 5 THEN 1 ELSE 0 END) as invalid_jam_level,
                    SUM(CASE WHEN cpu_usage < 0 OR cpu_usage > 100 THEN 1 ELSE 0 END) as invalid_cpu,
                    SUM(CASE WHEN online_flag NOT IN ('ONLINE', 'OFFLINE', 'UNKNOWN') THEN 1 ELSE 0 END) as invalid_online
                FROM {table_name}
                WHERE dt = '{partition_date}'
            """
            cursor.execute(query)
            result = cursor.fetchone()

            invalid_speed, invalid_jam, invalid_cpu, invalid_online = result
            total_invalid = invalid_speed + invalid_jam + invalid_cpu + invalid_online

            query_total = f"SELECT COUNT(*) FROM {table_name} WHERE dt = '{partition_date}'"
            cursor.execute(query_total)
            total_count = cursor.fetchone()[0]

            validity_rate = (total_count - total_invalid) / max(total_count, 1) * 100

            record = {
                'check_type': 'validity', 'table_name': table_name,
                'partition_date': partition_date,
                'invalid_counts': {
                    'invalid_speed': invalid_speed, 'invalid_jam_level': invalid_jam,
                    'invalid_cpu': invalid_cpu, 'invalid_online': invalid_online
                },
                'validity_rate': round(validity_rate, 2),
                'status': 'PASS' if validity_rate >= 99 else 'FAIL'
            }
            self.quality_results.append(record)
            conn.close()

            if record['status'] == 'FAIL':
                self.alert_notifier.notify(
                    f"[数据质量告警] {table_name} 数据合法性不达标",
                    f"表: {table_name}\n分区: {partition_date}\n合法性: {validity_rate:.2f}% (阈值 99%)\n"
                    f"异常值: speed={invalid_speed}, jam={invalid_jam}, cpu={invalid_cpu}, online={invalid_online}",
                    severity='MAJOR'
                )

        except Exception as e:
            self.quality_results.append({
                'check_type': 'validity', 'table_name': table_name,
                'partition_date': partition_date, 'error': str(e), 'status': 'ERROR'
            })

    def check_kafka_lag(self, topic, consumer_group):
        """Kafka延迟监控"""
        if KafkaConsumer is None:
            self.quality_results.append({
                'check_type': 'kafka_lag', 'topic': topic,
                'consumer_group': consumer_group,
                'error': 'kafka-python not installed', 'status': 'ERROR'
            })
            return

        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_broker,
                group_id=consumer_group,
                auto_offset_reset='earliest',
                consumer_timeout_ms=5000
            )

            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                self.quality_results.append({
                    'check_type': 'kafka_lag', 'topic': topic,
                    'consumer_group': consumer_group,
                    'error': 'Topic not found', 'status': 'ERROR'
                })
                consumer.close()
                return

            lags = []
            for partition in partitions:
                end_offset = consumer.end_offsets({(topic, partition)})[(topic, partition)]
                committed = consumer.committed((topic, partition))
                lag = end_offset - (committed if committed else 0)
                lags.append(lag)

            total_lag = sum(lags)
            avg_lag = total_lag / len(lags) if lags else 0

            status = 'PASS' if total_lag < 1000 else 'WARN' if total_lag < 10000 else 'FAIL'
            record = {
                'check_type': 'kafka_lag', 'topic': topic,
                'consumer_group': consumer_group, 'total_lag': total_lag,
                'avg_lag': round(avg_lag, 2), 'status': status
            }
            self.quality_results.append(record)

            if status in ('WARN', 'FAIL'):
                self.alert_notifier.notify(
                    f"[Kafka延迟告警] {topic} 消费延迟超标",
                    f"Topic: {topic}\n消费组: {consumer_group}\n总延迟: {total_lag}\n平均延迟: {avg_lag:.0f}\n"
                    f"阈值: WARN=1000, FAIL=10000",
                    severity='MAJOR' if status == 'FAIL' else 'MINOR'
                )

            consumer.close()

        except Exception as e:
            self.quality_results.append({
                'check_type': 'kafka_lag', 'topic': topic,
                'consumer_group': consumer_group, 'error': str(e), 'status': 'ERROR'
            })

    def generate_report(self, output_file=None):
        """生成监控报告"""
        report = {
            'report_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_checks': len(self.quality_results),
            'pass_count': sum(1 for r in self.quality_results if r['status'] == 'PASS'),
            'fail_count': sum(1 for r in self.quality_results if r['status'] == 'FAIL'),
            'warn_count': sum(1 for r in self.quality_results if r['status'] == 'WARN'),
            'error_count': sum(1 for r in self.quality_results if r['status'] == 'ERROR'),
            'quality_score': round(
                sum(1 for r in self.quality_results if r['status'] == 'PASS') * 100.0 / max(len(self.quality_results), 1), 2
            ),
            'results': self.quality_results
        }

        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

        # 每日汇总邮件（包含数据质量评分）
        if report['quality_score'] < 95:
            self.alert_notifier.notify(
                f"[数据质量日报] 质量评分 {report['quality_score']}% < 95%",
                f"总检查项: {report['total_checks']}\n"
                f"通过: {report['pass_count']} | 失败: {report['fail_count']} | "
                f"警告: {report['warn_count']} | 错误: {report['error_count']}\n"
                f"质量评分: {report['quality_score']}%\n\n"
                f"详情请查看: /tmp/data_quality_report.json",
                severity='MINOR'
            )

        return report


if __name__ == '__main__':
    monitor = DataQualityMonitor()
    today = datetime.now().strftime('%Y-%m-%d')

    # 完整性检查
    monitor.check_completeness('ods_vehicle_pass_di', today)
    monitor.check_completeness('ods_device_status_di', today)

    # 唯一性检查
    monitor.check_uniqueness('ods_vehicle_pass_di', today, ['vehicle_id', 'pass_time'])

    # 合法性检查
    monitor.check_validity('dwd_traffic_status_di', today)

    # Kafka延迟检查
    monitor.check_kafka_lag('traffic_vehicle', 'traffic_vehicle_group')

    # 生成报告
    report = monitor.generate_report('/tmp/data_quality_report.json')
    print(json.dumps(report, indent=2, ensure_ascii=False))
