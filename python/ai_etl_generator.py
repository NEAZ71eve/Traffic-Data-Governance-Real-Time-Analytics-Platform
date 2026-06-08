import json
from typing import List, Dict

class ETLScriptGenerator:
    def __init__(self):
        self.ods_templates = {
            'vehicle': """CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
    vehicle_id STRING COMMENT '车辆ID',
    road_id STRING COMMENT '道路ID',
    device_id STRING COMMENT '设备ID',
    pass_time STRING COMMENT '通行时间',
    speed INT COMMENT '车速(km/h)',
    direction STRING COMMENT '行驶方向',
    plate_number STRING COMMENT '车牌号',
    vehicle_type STRING COMMENT '车辆类型',
    lane INT COMMENT '车道号',
    dt STRING COMMENT '分区日期'
) COMMENT '{comment}'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/{table_name}'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai'
);""",
            'traffic': """CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
    road_id STRING COMMENT '道路ID',
    avg_speed INT COMMENT '平均车速(km/h)',
    traffic_flow INT COMMENT '车流量',
    jam_level INT COMMENT '拥堵等级(1-5)',
    congestion_rate DECIMAL(5,2) COMMENT '拥堵率',
    peak_flag STRING COMMENT '高峰标识',
    sample_time STRING COMMENT '采样时间',
    dt STRING COMMENT '分区日期'
) COMMENT '{comment}'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/{table_name}'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai'
);""",
            'device': """CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
    device_id STRING COMMENT '设备ID',
    cpu_usage DECIMAL(5,2) COMMENT 'CPU使用率(%)',
    memory_usage DECIMAL(5,2) COMMENT '内存使用率(%)',
    temperature DECIMAL(4,1) COMMENT '设备温度(℃)',
    online_flag STRING COMMENT '在线状态(ONLINE/OFFLINE)',
    heartbeat_time STRING COMMENT '心跳时间',
    signal_strength INT COMMENT '信号强度',
    device_type STRING COMMENT '设备类型',
    dt STRING COMMENT '分区日期'
) COMMENT '{comment}'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/traffic_db.db/{table_name}'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'partition.timezone' = 'Asia/Shanghai'
);"""
        }

        self.dwd_templates = {
            'default': """SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE {target_table} PARTITION (dt)
SELECT
{columns}
FROM {source_table}
WHERE dt = '${date}'
{filter_conditions}
{distinct_clause};

ALTER TABLE {target_table} ADD IF NOT EXISTS PARTITION (dt='${date}');
MSCK REPAIR TABLE {target_table};"""
        }

    def generate_ods_ddl(self, table_name: str, table_type: str, comment: str) -> str:
        template = self.ods_templates.get(table_type, self.ods_templates['vehicle'])
        return template.format(
            table_name=table_name,
            comment=comment
        )

    def generate_dwd_sql(self, source_table: str, target_table: str, columns: List[Dict], 
                        filter_conditions: str = '', distinct_columns: List[str] = None) -> str:
        col_list = []
        for col in columns:
            name = col['name']
            expr = col.get('expression', name)
            alias = col.get('alias', name)
            col_list.append(f'    {expr} AS {alias}')
        
        cols_str = ',\n'.join(col_list)
        
        distinct_clause = ''
        if distinct_columns:
            distinct_clause = f"\n    AND ROW_NUMBER() OVER(PARTITION BY {', '.join(distinct_columns)} ORDER BY {distinct_columns[0]}) = 1"
        
        return self.dwd_templates['default'].format(
            source_table=source_table,
            target_table=target_table,
            columns=cols_str,
            filter_conditions=filter_conditions,
            distinct_clause=distinct_clause
        )

    def generate_dws_sql(self, source_table: str, target_table: str, group_by_cols: List[str],
                        agg_cols: List[Dict], join_tables: List[str] = None) -> str:
        agg_list = []
        for agg in agg_cols:
            func = agg['func']
            col = agg['column']
            alias = agg.get('alias', f'{func.lower()}_{col}')
            agg_list.append(f'    {func}({col}) AS {alias}')
        
        agg_str = ',\n'.join(agg_list)
        group_by_str = ', '.join(group_by_cols)
        
        join_clause = ''
        if join_tables:
            join_clause = '\n'.join([f'JOIN {t} ON ...' for t in join_tables])
        
        return f"""SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE {target_table} PARTITION (dt)
SELECT
    {group_by_str},
{agg_str},
    dt
FROM {source_table}
{join_clause}
WHERE dt = '${{date}}'
GROUP BY {group_by_str}, dt;

ALTER TABLE {target_table} ADD IF NOT EXISTS PARTITION (dt='${{date}}');
MSCK REPAIR TABLE {target_table};"""

class NL2SQLConverter:
    def __init__(self):
        self.patterns = [
            {
                'pattern': '最拥堵的.*道路',
                'sql': """SELECT r.road_name, ROUND(AVG(jam_level), 2) as avg_jam_level
FROM dwd_traffic_status_di ts
JOIN dim_road_zip r ON ts.road_id = r.road_id AND r.is_current = 'Y'
WHERE ts.dt = '{date}'
GROUP BY r.road_name
ORDER BY avg_jam_level DESC
LIMIT {limit};"""
            },
            {
                'pattern': '车流量.*最高',
                'sql': """SELECT r.road_name, SUM(traffic_flow) as total_flow
FROM dwd_traffic_status_di ts
JOIN dim_road_zip r ON ts.road_id = r.road_id AND r.is_current = 'Y'
WHERE ts.dt = '{date}'
GROUP BY r.road_name
ORDER BY total_flow DESC
LIMIT {limit};"""
            },
            {
                'pattern': '设备.*离线',
                'sql': """SELECT d.device_id, d.device_type, d.area, COUNT(*) as offline_count
FROM dwd_device_status_di ds
JOIN dim_device_zip d ON ds.device_id = d.device_id AND d.is_current = 'Y'
WHERE ds.dt = '{date}'
  AND ds.online_flag = 'OFFLINE'
GROUP BY d.device_id, d.device_type, d.area
ORDER BY offline_count DESC;"""
            },
            {
                'pattern': '平均速度',
                'sql': """SELECT ROUND(AVG(speed), 2) as avg_speed
FROM dwd_vehicle_pass_di
WHERE dt = '{date}';"""
            },
            {
                'pattern': '高峰.*时段',
                'sql': """SELECT hour, COUNT(*) as traffic_count
FROM dwd_vehicle_pass_di
WHERE dt = '{date}'
GROUP BY hour
ORDER BY traffic_count DESC
LIMIT 3;"""
            }
        ]

    def convert(self, natural_query: str, date: str = None, limit: int = 10) -> str:
        if date is None:
            date = '${date}'
        
        for pattern in self.patterns:
            if pattern['pattern'] in natural_query:
                return pattern['sql'].format(date=date, limit=limit)
        
        return f"-- 无法识别查询模式: {natural_query}"

if __name__ == '__main__':
    generator = ETLScriptGenerator()
    
    ddl = generator.generate_ods_ddl('ods_custom_vehicle', 'vehicle', '自定义车辆通行表')
    print("ODS DDL:\n", ddl)
    
    columns = [
        {'name': 'vehicle_id'},
        {'name': 'road_id'},
        {'name': 'speed', 'expression': 'CASE WHEN speed < 0 OR speed > 200 THEN NULL ELSE speed END', 'alias': 'speed'},
        {'name': 'dt'}
    ]
    dwd_sql = generator.generate_dwd_sql('ods_vehicle_pass_di', 'dwd_vehicle_pass_di', columns, 
                                        filter_conditions='AND vehicle_id IS NOT NULL',
                                        distinct_columns=['vehicle_id', 'pass_time'])
    print("\nDWD SQL:\n", dwd_sql)
    
    dws_sql = generator.generate_dws_sql('dwd_vehicle_pass_di', 'dws_road_hour_flow',
                                        group_by_cols=['road_id', 'hour'],
                                        agg_cols=[
                                            {'func': 'COUNT', 'column': '*', 'alias': 'traffic_count'},
                                            {'func': 'AVG', 'column': 'speed', 'alias': 'avg_speed'}
                                        ])
    print("\nDWS SQL:\n", dws_sql)
    
    converter = NL2SQLConverter()
    sql = converter.convert('今天最拥堵的5条道路', date='2024-01-15', limit=5)
    print("\nNL2SQL Result:\n", sql)