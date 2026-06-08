import os
import sys
from datetime import datetime, timedelta
from pyhive import hive
from kafka import KafkaConsumer
import json

class DataQualityMonitor:
    def __init__(self, hive_host='localhost', hive_port=10000, kafka_broker='localhost:9092'):
        self.hive_host = hive_host
        self.hive_port = hive_port
        self.kafka_broker = kafka_broker
        self.quality_results = []

    def check_completeness(self, table_name, partition_date):
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
            
            self.quality_results.append({
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
            })
            
            conn.close()
        except Exception as e:
            self.quality_results.append({
                'check_type': 'completeness',
                'table_name': table_name,
                'partition_date': partition_date,
                'error': str(e),
                'status': 'ERROR'
            })

    def check_uniqueness(self, table_name, partition_date, unique_columns):
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
            result = cursor.fetchone()
            duplicate_count = result[0]
            
            query_total = f"SELECT COUNT(*) FROM {table_name} WHERE dt = '{partition_date}'"
            cursor.execute(query_total)
            total_count = cursor.fetchone()[0]
            
            uniqueness_rate = (total_count - duplicate_count) / max(total_count, 1) * 100
            
            self.quality_results.append({
                'check_type': 'uniqueness',
                'table_name': table_name,
                'partition_date': partition_date,
                'unique_columns': unique_columns,
                'duplicate_count': duplicate_count,
                'uniqueness_rate': round(uniqueness_rate, 2),
                'status': 'PASS' if uniqueness_rate >= 99.9 else 'FAIL'
            })
            
            conn.close()
        except Exception as e:
            self.quality_results.append({
                'check_type': 'uniqueness',
                'table_name': table_name,
                'partition_date': partition_date,
                'error': str(e),
                'status': 'ERROR'
            })

    def check_validity(self, table_name, partition_date):
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
            
            self.quality_results.append({
                'check_type': 'validity',
                'table_name': table_name,
                'partition_date': partition_date,
                'invalid_counts': {
                    'invalid_speed': invalid_speed,
                    'invalid_jam_level': invalid_jam,
                    'invalid_cpu': invalid_cpu,
                    'invalid_online': invalid_online
                },
                'validity_rate': round(validity_rate, 2),
                'status': 'PASS' if validity_rate >= 99 else 'FAIL'
            })
            
            conn.close()
        except Exception as e:
            self.quality_results.append({
                'check_type': 'validity',
                'table_name': table_name,
                'partition_date': partition_date,
                'error': str(e),
                'status': 'ERROR'
            })

    def check_kafka_lag(self, topic, consumer_group):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_broker,
                group_id=consumer_group,
                auto_offset_reset='earliest'
            )
            
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                self.quality_results.append({
                    'check_type': 'kafka_lag',
                    'topic': topic,
                    'consumer_group': consumer_group,
                    'error': 'Topic not found',
                    'status': 'ERROR'
                })
                return
            
            lags = []
            for partition in partitions:
                end_offset = consumer.end_offsets({(topic, partition)})[(topic, partition)]
                committed = consumer.committed((topic, partition))
                lag = end_offset - (committed if committed else 0)
                lags.append(lag)
            
            total_lag = sum(lags)
            avg_lag = total_lag / len(lags)
            
            self.quality_results.append({
                'check_type': 'kafka_lag',
                'topic': topic,
                'consumer_group': consumer_group,
                'total_lag': total_lag,
                'avg_lag': round(avg_lag, 2),
                'status': 'PASS' if total_lag < 1000 else 'WARN' if total_lag < 10000 else 'FAIL'
            })
            
            consumer.close()
        except Exception as e:
            self.quality_results.append({
                'check_type': 'kafka_lag',
                'topic': topic,
                'consumer_group': consumer_group,
                'error': str(e),
                'status': 'ERROR'
            })

    def generate_report(self, output_file=None):
        report = {
            'report_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_checks': len(self.quality_results),
            'pass_count': sum(1 for r in self.quality_results if r['status'] == 'PASS'),
            'fail_count': sum(1 for r in self.quality_results if r['status'] == 'FAIL'),
            'warn_count': sum(1 for r in self.quality_results if r['status'] == 'WARN'),
            'error_count': sum(1 for r in self.quality_results if r['status'] == 'ERROR'),
            'results': self.quality_results
        }
        
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
        
        return report

if __name__ == '__main__':
    monitor = DataQualityMonitor()
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    monitor.check_completeness('ods_vehicle_pass_di', today)
    monitor.check_completeness('ods_device_status_di', today)
    
    monitor.check_uniqueness('ods_vehicle_pass_di', today, ['vehicle_id', 'pass_time'])
    
    monitor.check_validity('dwd_traffic_status_di', today)
    
    monitor.check_kafka_lag('traffic_vehicle', 'traffic_vehicle_group')
    
    report = monitor.generate_report('/tmp/data_quality_report.json')
    print(json.dumps(report, indent=2, ensure_ascii=False))