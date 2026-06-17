# Redis 数据结构设计

实时计算缓存层 — Flink Sink 输出目标。

## 1. 实时车流

```
Key: traffic:vehicle:{road_id}
Type: HASH
TTL: 600s (10分钟)
Fields: road_name, traffic_count, avg_speed, window_start, update_time
```

示例: `HSET traffic:vehicle:R001 road_name "长安街" traffic_count 856 avg_speed 42 window_start "2026-06-08 08:00:00" update_time "2026-06-08 08:05:00"`

## 2. 实时拥堵指数

```
Key: traffic:congestion:{road_id}
Type: HASH
TTL: 600s
Fields: jam_level(1-5), congestion_rate(0-100), avg_speed
```

## 3. 实时设备状态

```
Key: traffic:device:{device_id}
Type: HASH
TTL: 300s
Fields: device_name, online_flag, cpu_usage, mem_usage, temperature, status(ONLINE/OFFLINE/ALARM), update_time
```

## 4. 区域拥堵汇总

```
Key: traffic:area:{area_id}
Type: HASH
TTL: 600s
Fields: area_name, avg_jam_level, total_vehicles, congestion_rate
```

## 5. 告警列表

```
Key: traffic:alerts
Type: LIST (左进右出, 保留最近100条)
Fields: alert_id, device_id, alert_type(CPU_HIGH/TEMP_HIGH/OFFLINE), alert_time, alert_level(P0-P3), message
```

操作: `LPUSH traffic:alerts "{...}"` → `LTRIM traffic:alerts 0 99`

## 6. 数据统计

```
Key: traffic:stats
Type: HASH
Fields: total_vehicles_today, total_alarms_today, avg_response_time
```

## 数据流

```
Flink TrafficVehicleCount ──HSET──▶ traffic:vehicle:{road_id}
Flink TrafficCongestionDetection ──HSET──▶ traffic:congestion:{road_id}
Flink DeviceStatusCEP ──HSET──▶ traffic:device:{device_id}
                              ──LPUSH──▶ traffic:alerts
                              ──HINCRBY──▶ traffic:stats total_alarms_today
```
