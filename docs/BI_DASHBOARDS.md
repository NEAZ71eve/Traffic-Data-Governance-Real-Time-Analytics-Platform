# Superset 可视化看板

> 前端呈现层 — 用户实际看到的界面

## 看板总览

| 看板 | 目标用户 | 刷新 | 一句话 |
|------|---------|------|--------|
| 城市交通总览大屏 | 领导/运营 | 5min | 一眼看懂全市交通态势 |
| **实时路况监控面板** | 运营分析师 | **5秒** | 数字跳动、拥堵变红、告警弹窗 |
| 设备运维监控大屏 | 运维工程师 | 1min | 设备在线/离线/CPU/温度/健康评分 |
| 数据质量监控面板 | 数据开发 | 10min | 丢失/重复/异常/Kafka Lag |

## 前端全链路

```
用户浏览器 → Superset 大屏
    ├── Redis 实时缓存 ← Flink (实时链路 <5s)
    ├── Hive ADS ← ODS→DIM→DWD→DWS→ADS (离线链路 T+1)
    └── Python 质量报告 JSON
```

## 核心功能：实时路况监控

**数据链路**: 传感器(30s上报) → Kafka → Flink 窗口计算 → Redis → Superset(5秒刷新)

**端到端延迟**: <5 秒

**前端效果**:
- 车流数字像股票行情每秒跳动
- 拥堵5级 → 红色背景+闪烁；通畅 → 绿色
- 告警从右侧滑入，带浏览器通知
- 持续拥堵 >30分钟自动升级

## 用户交互

| 能力 | 说明 |
|------|------|
| 筛选 | 日期/区域/道路下拉框+日历，全局联动 |
| 下钻 | 点击拥堵道路 → 弹出24h车速曲线 |
| 自动刷新 | 实时5秒/离线5分钟 |
| 暂停刷新 | 冻结数据截图分析 |
| 导出 | PNG/CSV/PDF |
| 全屏 | F11 大屏模式 |

## 面试话术

**"实时路况面板是你这个项目最亮眼的功能吗？"**
> 对，这是我最自豪的部分。数据链路：终端传感器30秒上报 → Kafka → Flink 5分钟窗口聚合 → Redis → Superset 5秒刷新。端到端延迟不到5秒。用户打开浏览器，车流数字像股票一样每秒跳动，拥堵路段自动变红闪烁，告警实时推送钉钉。

**"Flink 怎么保证数据不丢？"**
> 配置了 Checkpoint 每5分钟一次，Exactly-Once 语义。崩溃后自动从最近 Checkpoint 恢复，重启策略是固定延迟3次、每次间隔60秒。

**"数据质量怎么保证？"**
> 四维检测：完整性检查空字段、唯一性检查重复记录、合法性检查值域（车速0-200km/h）、时效性检查Kafka Lag。低于阈值自动推送钉钉/邮件告警，30分钟去重抑制。

## 图表 SQL 清单

### 城市交通总览

```sql
-- 今日车流量
SELECT COUNT(*) AS total_vehicles FROM dwd_vehicle_pass_di WHERE dt = CURRENT_DATE;

-- 当前拥堵指数
SELECT ROUND(AVG(jam_level), 2) AS avg_jam FROM dws_area_jam_hour WHERE dt = CURRENT_DATE;

-- TOP10 拥堵路段
SELECT r.road_name, f.traffic_count, f.avg_speed
FROM dws_road_hour_flow f JOIN dim_road_zip r ON f.road_id = r.road_id
WHERE f.dt = CURRENT_DATE AND r.is_current = 'Y'
ORDER BY f.jam_level DESC LIMIT 10;

-- 24小时车流分布
SELECT HOUR(window_start) AS hour, SUM(traffic_count) AS total
FROM dws_road_hour_flow WHERE dt = CURRENT_DATE
GROUP BY HOUR(window_start) ORDER BY hour;
```

### 实时路况

```sql
-- 各道路实时状态 (Redis)
SELECT road_id, traffic_count, avg_speed, jam_level FROM redis实时数据;

-- 拥堵趋势
SELECT window_start, jam_level FROM dws_road_hour_flow
WHERE road_id = 'R001' AND dt = CURRENT_DATE ORDER BY window_start;

-- 流量突增检测
SELECT road_id, traffic_count, avg_speed
FROM dws_road_hour_flow
WHERE dt = CURRENT_DATE AND traffic_count > (SELECT AVG(traffic_count)*1.5 FROM dws_road_hour_flow WHERE dt = CURRENT_DATE);
```

### 设备运维

```sql
-- 设备在线率
SELECT device_id, ROUND(AVG(online_flag) * 100, 2) AS online_rate
FROM dwd_device_status_di WHERE dt = CURRENT_DATE
GROUP BY device_id ORDER BY online_rate;

-- 健康评分排行
SELECT d.device_name, h.health_score, h.online_rate, h.avg_cpu, h.avg_memory
FROM ads_device_health_score h JOIN dim_device_zip d ON h.device_id = d.device_id
WHERE d.is_current = 'Y' ORDER BY h.health_score;
```

### 数据质量

```sql
-- 空值率
SELECT COUNT(*) AS total, SUM(CASE WHEN speed IS NULL THEN 1 ELSE 0 END) AS null_speed
FROM ods_vehicle_pass_di WHERE dt = CURRENT_DATE;

-- 重复率
SELECT COUNT(*) - COUNT(DISTINCT pass_id) AS duplicate_count
FROM ods_vehicle_pass_di WHERE dt = CURRENT_DATE;

-- 值域校验
SELECT COUNT(*) AS illegal_speed FROM ods_vehicle_pass_di
WHERE dt = CURRENT_DATE AND (speed < 0 OR speed > 200);
```

## Superset 部署

```bash
# 一键配置看板
python bin/setup_superset.py --offline

# 数据源配置
# Prometheus: http://prometheus:9090
# MySQL: mysql:3306 / traffic / traffic123
# Hive: jdbc:hive2://hiveserver2:10000
```
