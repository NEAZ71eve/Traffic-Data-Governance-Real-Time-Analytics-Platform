# ============================================
# Redis 数据结构设计文档
# 智慧城市交通数据治理 — 实时计算缓存层
# ============================================

# ---- 1. 实时车流数据 (Flink TrafficVehicleCount 输出) ----
# Key: traffic:vehicle:{road_id}
# Type: HASH
# TTL: 600s (10分钟)
# 
# Fields:
#   road_name      - 道路名称
#   traffic_count  - 当前5分钟窗口车流量
#   avg_speed      - 平均速度 (km/h)
#   window_start   - 窗口开始时间
#   update_time    - 最后更新时间
#
# 示例:
# HSET traffic:vehicle:R001 road_name "长安街" traffic_count 856 avg_speed 42 window_start "2026-06-08 08:00:00" update_time "2026-06-08 08:05:00"
# EXPIRE traffic:vehicle:R001 600

# ---- 2. 实时拥堵指数 ----
# Key: traffic:congestion:{road_id}
# Type: HASH
# TTL: 600s
#
# Fields:
#   jam_level        - 拥堵等级 (1-5)
#   congestion_rate  - 拥堵率 (0-100)
#   avg_speed        - 平均速度
#   traffic_flow     - 车流量
#   update_time
#
# 示例:
# HSET traffic:congestion:R001 jam_level 4 congestion_rate 78 avg_speed 18 traffic_flow 920 update_time "2026-06-08 08:05:00"

# ---- 3. 实时设备状态 ----
# Key: device:status:{device_id}
# Type: HASH
# TTL: 300s
#
# Fields:
#   device_name   - 设备名称
#   online_flag   - 在线状态 (ONLINE/OFFLINE)
#   cpu_usage     - CPU使用率
#   memory_usage  - 内存使用率
#   temperature   - 温度
#   last_heartbeat - 最后心跳时间
#   health_flag   - 健康标识 (NORMAL/WARNING/ABNORMAL)
#
# 示例:
# HSET device:status:D001 device_name "长安街卡口1号" online_flag "ONLINE" cpu_usage 45 memory_usage 62 temperature 38 last_heartbeat "2026-06-08 08:05:00" health_flag "NORMAL"

# ---- 4. CEP 异常告警 ----
# Key: alert:cep:{alert_id}
# Type: HASH
# TTL: 86400s (24小时)
#
# Fields:
#   alert_type    - 告警类型 (OFFLINE_CONTINUOUS/CPU_HIGH/TEMP_HIGH)
#   device_id     - 设备ID
#   alert_level   - 告警级别 (CRITICAL/MAJOR)
#   alert_time    - 告警时间
#   detail        - 告警详情
#
# 示例:
# HSET alert:cep:20260608_001 alert_type "OFFLINE_CONTINUOUS" device_id "D012" alert_level "CRITICAL" alert_time "2026-06-08 07:30:00" detail "设备D012连续3次心跳超时"

# ---- 5. 实时城市级聚合指标 ----
# Key: traffic:city:overview
# Type: HASH
# TTL: 300s
#
# Fields:
#   total_online_vehicles - 当前在线车辆总数
#   avg_speed_city        - 全市平均速度
#   jam_road_count        - 拥堵路段数
#   severe_jam_count      - 严重拥堵路段数
#
# HSET traffic:city:overview total_online_vehicles 12500 avg_speed_city 45 jam_road_count 8 severe_jam_count 2

# ---- 6. TOP N 拥堵道路 (Sorted Set) ----
# Key: traffic:top_jam_roads
# Type: ZSET
# TTL: 600s
#
# Members: road_id, Score: congestion_rate (倒序)
#
# ZADD traffic:top_jam_roads 92 R001 85 R013 78 R002
# ZREVRANGE traffic:top_jam_roads 0 9 WITHSCORES

# ---- 7. 数据质量状态 ----
# Key: quality:status
# Type: HASH
# TTL: 3600s
#
# Fields:
#   completeness_rate - 完整率
#   uniqueness_rate   - 唯一率
#   validity_rate     - 合法性
#   timeliness_rate   - 及时性
#   kafka_lag_total   - Kafka总积压
#   last_check_time   - 最后检查时间
