import json
from collections import defaultdict

class DataLineageManager:
    def __init__(self):
        self.lineage_graph = defaultdict(list)
        self.table_metadata = {}
        self._build_default_lineage()

    def _build_default_lineage(self):
        self.add_lineage('ods_vehicle_pass_di', 'dwd_vehicle_pass_di')
        self.add_lineage('ods_traffic_status_di', 'dwd_traffic_status_di')
        self.add_lineage('ods_device_status_di', 'dwd_device_status_di')
        self.add_lineage('ods_alarm_log_di', 'dwd_alarm_log_di')
        
        self.add_lineage('dwd_vehicle_pass_di', 'dws_road_hour_flow')
        self.add_lineage('dwd_traffic_status_di', 'dws_area_jam_hour')
        self.add_lineage('dwd_device_status_di', 'dws_device_health_day')
        self.add_lineage('dwd_alarm_log_di', 'dws_alarm_day')
        
        self.add_lineage('dws_road_hour_flow', 'ads_traffic_operation')
        self.add_lineage('dws_area_jam_hour', 'ads_traffic_operation')
        self.add_lineage('dws_area_jam_hour', 'ads_top_jam_roads')
        self.add_lineage('dws_device_health_day', 'ads_device_health_score')
        self.add_lineage('dws_alarm_day', 'ads_device_health_score')
        self.add_lineage('dws_alarm_day', 'ads_device_mtbf_mttr')
        
        self.add_lineage('dim_road_zip', 'dws_area_jam_hour')
        self.add_lineage('dim_road_zip', 'ads_top_jam_roads')
        self.add_lineage('dim_device_zip', 'ads_device_health_score')
        self.add_lineage('dim_device_zip', 'ads_device_mtbf_mttr')
        self.add_lineage('dim_time', 'dws_area_jam_hour')
        self.add_lineage('dim_area', 'ads_traffic_operation')

    def add_lineage(self, source_table, target_table):
        self.lineage_graph[source_table].append(target_table)

    def get_upstream_tables(self, table_name):
        upstream = []
        for source, targets in self.lineage_graph.items():
            if table_name in targets:
                upstream.append(source)
                upstream.extend(self.get_upstream_tables(source))
        return list(set(upstream))

    def get_downstream_tables(self, table_name):
        downstream = []
        visited = set()
        
        def dfs(current):
            if current in visited:
                return
            visited.add(current)
            for target in self.lineage_graph.get(current, []):
                downstream.append(target)
                dfs(target)
        
        dfs(table_name)
        return list(set(downstream))

    def trace_column(self, target_table, target_column):
        trace_results = []
        visited = set()
        
        def dfs(table, column, path):
            if table in visited:
                return
            visited.add(table)
            
            upstream_tables = self.get_upstream_tables(table)
            if not upstream_tables:
                trace_results.append({
                    'column': column,
                    'table': table,
                    'path': path + [(table, column)]
                })
                return
            
            for upstream in upstream_tables:
                dfs(upstream, column, path + [(table, column)])
        
        dfs(target_table, target_column, [])
        return trace_results

    def detect_impact(self, table_name, affected_columns=None):
        downstream = self.get_downstream_tables(table_name)
        impact = {
            'affected_table': table_name,
            'affected_columns': affected_columns or ['*'],
            'impacted_tables': downstream,
            'impact_details': []
        }
        
        for downstream_table in downstream:
            impact['impact_details'].append({
                'table': downstream_table,
                'type': 'DIRECT' if downstream_table in self.lineage_graph.get(table_name, []) else 'INDIRECT',
                'columns': ['*']
            })
        
        return impact

    def export_lineage(self, output_file):
        lineage_data = {
            'graph': dict(self.lineage_graph),
            'metadata': self.table_metadata
        }
        with open(output_file, 'w') as f:
            json.dump(lineage_data, f, indent=2, ensure_ascii=False)

    def visualize_lineage(self):
        dot_lines = ['digraph G {']
        
        for source, targets in self.lineage_graph.items():
            for target in targets:
                dot_lines.append(f'    "{source}" -> "{target}";')
        
        dot_lines.append('}')
        return '\n'.join(dot_lines)

if __name__ == '__main__':
    manager = DataLineageManager()
    
    print("Upstream tables for ads_traffic_operation:")
    print(manager.get_upstream_tables('ads_traffic_operation'))
    
    print("\nDownstream tables for dwd_vehicle_pass_di:")
    print(manager.get_downstream_tables('dwd_vehicle_pass_di'))
    
    print("\nImpact analysis for dwd_traffic_status_di:")
    impact = manager.detect_impact('dwd_traffic_status_di', ['avg_speed', 'jam_level'])
    print(json.dumps(impact, indent=2, ensure_ascii=False))
    
    print("\nColumn trace for avg_speed in ads_traffic_operation:")
    trace = manager.trace_column('ads_traffic_operation', 'avg_speed')
    print(json.dumps(trace, indent=2, ensure_ascii=False))
    
    manager.export_lineage('/tmp/lineage.json')
    print("\nLineage graph exported to /tmp/lineage.json")