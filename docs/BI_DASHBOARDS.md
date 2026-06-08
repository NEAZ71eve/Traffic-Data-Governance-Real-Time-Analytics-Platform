# Superset BI可视化看板设计

---

## 一、看板总览（按角色拆分）

| 看板名称 | 目标角色 | 数据源 | 刷新频率 | 核心指标数 |
|---------|---------|--------|---------|----------|
| 交通运营实时看板 | 交通运营分析师 | Redis(Flink实时写入) | 60s | 12 |
| 设备运维监控看板 | 运维工程师 | Hive ADS层 | 5min | 10 |
| 交通治理决策看板 | 管理层/决策者 | Hive ADS层 | 5min | 8 |
| 数据质量监控看板 | 数据开发工程师 | Python监控报告 | 10min | 6 |

---

## 二、交通运营实时看板（traffic_ops）

### 数据来源
- Redis：Flink 实时计算结果（5分钟滚动窗口）
- 表：dws_road_hour_flow、dws_area_jam_hour（历史对比）

### 图表清单

#### 图表1：全市实时车流统计卡片（Big Number）
- SQL：`SELECT SUM(traffic_count) FROM dws_road_hour_flow WHERE dt='${date}' AND hour=HOUR(NOW())`
- 环比：`(今日-昨日)/昨日 * 100%`

#### 图表2：平均车速仪表盘（Gauge Chart）
- 指标：全市平均车速
- SQL：`SELECT ROUND(AVG(avg_speed),1) FROM dws_road_hour_flow WHERE dt='${date}'`
- 颜色分区：红色 0-20 | 黄色 20-40 | 绿色 40+

#### 图表3：区域拥堵热力图（Heatmap）
- X轴：小时(0-23)，Y轴：区域
- 值：jam_level 平均拥堵等级
- SQL:
```sql
SELECT a.area_name, jh.hour, ROUND(AVG(jh.jam_level), 1) as avg_jam
FROM dws_area_jam_hour jh
JOIN dim_area a ON jh.area_id = a.area_id
WHERE jh.dt = '${date}'
GROUP BY a.area_name, jh.hour
```

#### 图表4：拥堵TOP10道路排行（Bar Chart）
- SQL:
```sql
SELECT road_name, avg_jam_level, avg_congestion_rate
FROM ads_top_jam_roads
WHERE dt = '${date}'
ORDER BY rank_num
LIMIT 10
```

#### 图表5：24小时车流量趋势（Line Chart）
- SQL:
```sql
SELECT hour, SUM(traffic_count) as total_flow
FROM dws_road_hour_flow
WHERE dt = '${date}'
GROUP BY hour ORDER BY hour
```

#### 图表6：车辆类型分布（Pie Chart）
- SQL:
```sql
SELECT '小型车' as type, SUM(small_car_cnt) as cnt FROM dws_road_hour_flow WHERE dt='${date}'
UNION ALL
SELECT '中型车', SUM(medium_car_cnt) FROM dws_road_hour_flow WHERE dt='${date}'
UNION ALL
SELECT '大型车', SUM(large_car_cnt) FROM dws_road_hour_flow WHERE dt='${date}'
```

---

## 三、设备运维监控看板（device_ops）

### 数据来源
- Hive ADS：ads_device_health_score、ads_device_mtbf_mttr、dws_device_health_day

### 图表清单

#### 图表1：设备健康状态总览（Pie Chart）
```sql
SELECT health_level, COUNT(*) as device_count
FROM ads_device_health_score
WHERE dt = '${date}'
GROUP BY health_level
```

#### 图表2：设备在线率趋势（Line Chart）
```sql
SELECT dt, ROUND(SUM(online_duration)/(SUM(online_duration)+SUM(offline_count))*100, 2) as online_rate
FROM dws_device_health_day
WHERE dt >= DATE_SUB('${date}', 7)
GROUP BY dt ORDER BY dt
```

#### 图表3：设备健康评分TOP10/BOTTOM10（Table）
```sql
SELECT device_name, device_type, health_score, health_level,
       online_rate, avg_cpu_usage, avg_memory_usage
FROM ads_device_health_score
WHERE dt = '${date}'
ORDER BY health_score ASC
LIMIT 10
```

#### 图表4：MTBF/MTTR 可靠性排行（Bar Chart）
```sql
SELECT device_name, mtbf_hours, mttr_minutes
FROM ads_device_mtbf_mttr
WHERE dt = '${date}'
ORDER BY mtbf_hours ASC
LIMIT 20
```

#### 图表5：告警类型分布（Pie Chart）
```sql
SELECT alarm_type, SUM(total_alarm_count) as cnt
FROM dws_alarm_day
WHERE dt = '${date}'
GROUP BY alarm_type
```

#### 图表6：告警恢复率趋势（Line Chart）
```sql
SELECT dt, ROUND(SUM(recovered_count)/NULLIF(SUM(total_alarm_count),0)*100, 2) as recovery_rate
FROM dws_alarm_day
WHERE dt >= DATE_SUB('${date}', 7)
GROUP BY dt
```

---

## 四、交通治理决策看板（governance）

### 图表清单

#### 图表1：全市拥堵指数 KPI（Big Number + Trend）
```sql
SELECT ROUND(AVG(avg_congestion_rate), 1) as city_jam_index,
       SUM(total_traffic_flow) as total_flow
FROM ads_traffic_operation
WHERE dt = '${date}'
```

#### 图表2：区域拥堵治理对比（Bar Chart）
```sql
SELECT area_name, ROUND(AVG(avg_congestion_rate), 1) as avg_jam,
       SUM(severe_jam_count) as severe_cnt
FROM ads_traffic_operation
WHERE dt >= DATE_SUB('${date}', 7)
GROUP BY area_name
ORDER BY avg_jam DESC
```

#### 图表3：高峰/非高峰拥堵对比（Grouped Bar）
```sql
SELECT peak_period, ROUND(AVG(avg_congestion_rate), 1) as jam_rate,
       ROUND(AVG(avg_speed_all), 1) as avg_speed
FROM ads_traffic_operation
WHERE dt = '${date}'
GROUP BY peak_period
```

#### 图表4：治理效果对比（同区域不同日期对比）
```sql
SELECT a1.area_name, a1.avg_congestion_rate as today,
       a2.avg_congestion_rate as last_week
FROM ads_traffic_operation a1
LEFT JOIN ads_traffic_operation a2
  ON a1.area_id = a2.area_id
 AND a2.dt = DATE_SUB('${date}', 7)
WHERE a1.dt = '${date}'
```

---

## 五、数据质量监控看板（data_quality）

### 图表清单

#### 图表1：数据质量评分卡片（Big Number）
- 数据源：data_quality_report.json
- 计算：PASS数 / 总检查数 * 100

#### 图表2：各表完整率排行（Bar Chart）
```sql
SELECT table_name, ROUND(completeness_rate, 1) as rate
FROM data_quality_results
WHERE report_date = '${date}'
ORDER BY rate ASC
```

#### 图表3：Kafka消费延迟趋势（Line Chart）
- 数据源：data_quality_monitor.py 输出的 Kafka Lag 监控指标

#### 图表4：质量检查历史趋势（Time Series）
- X轴：日期
- Y轴：通过率(%)
- 系列：completeness / uniqueness / validity

---

## 六、告警联动设计

```
┌─────────────┐     ┌──────────────┐     ┌───────────────┐
│ 数据质量异常  │ ──▶ │ DingTalk推送  │ ──▶ │ 数据开发工程师  │
│ (完整率<99%) │     │ + 邮件通知    │     │ 触发数据重跑    │
└─────────────┘     └──────────────┘     └───────────────┘

┌─────────────┐     ┌──────────────┐     ┌───────────────┐
│ 设备健康<60  │ ──▶ │ 钉钉@所有人   │ ──▶ │ 运维主管+值班  │
│ (一级告警)   │     │ +短信+电话   │     │ 自动生成工单    │
└─────────────┘     └──────────────┘     └───────────────┘

┌─────────────┐     ┌──────────────┐     ┌───────────────┐
│ 拥堵等级=5   │ ──▶ │ 推送交通管控  │ ──▶ │ 交警部门      │
│ 持续>30分钟  │     │ 部门(API对接) │     │ 启动分流/管制   │
└─────────────┘     └──────────────┘     └───────────────┘
```

## 七、Superset部署配置

```bash
# 数据源配置
superset set-database-uri \
  -d traffic_hive \
  -u hive://hive-server:10000/traffic_db

superset set-database-uri \
  -d traffic_redis \
  -u redis://redis:6379/0

# 导入看板JSON（通过Superset Import API）
superset import-dashboards -p /path/to/dashboard_export.zip

# 配置定时刷新（通过Superset Schedule）
# 实时看板: 每60秒刷新
# 设备看板: 每5分钟刷新
# 决策看板: 每5分钟刷新
```
