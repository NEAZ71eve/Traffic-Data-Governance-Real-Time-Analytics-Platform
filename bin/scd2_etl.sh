#!/bin/bash
# ============================================================================
# scd2_etl.sh
# SCD2 拉链表完整 ETL 脚本
# 支持: init(初始化) / daily(每日增量更新)
# ============================================================================

set -euo pipefail

HIVE_SERVER="jdbc:hive2://hiveserver2:10000"
HIVE_DB="traffic_db"
DATE="${DATE:-$(date +%Y-%m-%d)}"
YESTERDAY="${YESTERDAY:-$(date -d 'yesterday' +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)}"

echo "=========================================="
echo "SCD2 拉链表 ETL - 日期: $DATE"
echo "=========================================="

# ============================================================================
# 初始化: 首次全量加载
# ============================================================================
init_scd2() {
    echo "--- 初始化 SCD2 拉链表 ---"
    
    # 1. 创建 ODS 基础信息表 (如果 DataX 未同步)
    beeline -u "${HIVE_SERVER}" -e "
        CREATE TABLE IF NOT EXISTS ${HIVE_DB}.ods_road_info (
            road_id         STRING,
            road_name       STRING,
            road_type       STRING,
            road_length     DECIMAL(8,2),
            lane_count      INT,
            speed_limit     INT,
            area_id         STRING,
            direction       STRING,
            status          STRING,
            updated_at      STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        STORED AS ORC
        LOCATION '/user/hive/warehouse/${HIVE_DB}.db/ods_road_info';
    " || true
    
    # 2. 初始化 dim_road_zip 拉链表
    beeline -u "${HIVE_SERVER}/${HIVE_DB}" -e "
        SET hive.exec.dynamic.partition = true;
        SET hive.exec.dynamic.partition.mode = nonstrict;
        
        -- 首次全量加载: 所有记录设为当前有效
        INSERT OVERWRITE TABLE dim_road_zip PARTITION (dt = '${DATE}')
        SELECT
            road_id,
            road_name,
            road_type,
            road_length,
            lane_count,
            speed_limit,
            area_id,
            direction,
            '1970-01-01' AS start_time,
            '9999-12-31' AS end_time,
            'Y'          AS is_current
        FROM ods_road_info
        WHERE dt = '${DATE}';
    " || true
    
    # 3. 初始化 dim_device_zip 拉链表
    beeline -u "${HIVE_SERVER}/${HIVE_DB}" -e "
        SET hive.exec.dynamic.partition = true;
        SET hive.exec.dynamic.partition.mode = nonstrict;
        
        INSERT OVERWRITE TABLE dim_device_zip PARTITION (dt = '${DATE}')
        SELECT
            device_id,
            device_type,
            road_id,
            area_id,
            install_date,
            status,
            ip_address,
            firmware_version,
            '1970-01-01' AS start_time,
            '9999-12-31' AS end_time,
            'Y'          AS is_current
        FROM ods_device_info
        WHERE dt = '${DATE}';
    " || true
    
    echo "✅ SCD2 拉链表初始化完成"
}

# ============================================================================
# 每日增量更新: 拉链表增量逻辑
# ============================================================================
daily_update_scd2() {
    echo "--- SCD2 每日增量更新 (${YESTERDAY} -> ${DATE}) ---"
    
    # dim_road_zip 增量更新
    beeline -u "${HIVE_SERVER}/${HIVE_DB}" -e "
        SET hive.exec.dynamic.partition = true;
        SET hive.exec.dynamic.partition.mode = nonstrict;
        
        -- 步骤1: 创建临时表存储今日增量数据
        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_road_delta AS
        SELECT new.*
        FROM ods_road_info new
        LEFT JOIN (
            SELECT * FROM dim_road_zip 
            WHERE dt = '${YESTERDAY}' AND is_current = 'Y'
        ) old ON new.road_id = old.road_id
        WHERE new.dt = '${DATE}'
          AND (
              old.road_id IS NULL  -- 新增道路
              OR old.road_name <> new.road_name
              OR old.road_type <> new.road_type
              OR old.lane_count <> new.lane_count
              OR old.speed_limit <> new.speed_limit
              OR old.status <> new.status
          );
        
        -- 步骤2: 全量重写今日分区 (闭合旧记录 + 插入新记录)
        INSERT OVERWRITE TABLE dim_road_zip PARTITION (dt = '${DATE}')
        SELECT
            road_id, road_name, road_type, road_length, lane_count, speed_limit,
            area_id, direction, start_time, end_time, is_current
        FROM (
            -- 历史未变更记录: 保持原样
            SELECT
                road_id, road_name, road_type, road_length, lane_count, speed_limit,
                area_id, direction, start_time, end_time, is_current
            FROM dim_road_zip
            WHERE dt = '${YESTERDAY}' AND is_current = 'Y'
              AND road_id NOT IN (SELECT road_id FROM tmp_road_delta)
            
            UNION ALL
            
            -- 变更记录: 闭合旧版本 (is_current='N', end_time=昨天)
            SELECT
                old.road_id, old.road_name, old.road_type, old.road_length,
                old.lane_count, old.speed_limit, old.area_id, old.direction,
                old.start_time, '${YESTERDAY}' AS end_time, 'N' AS is_current
            FROM dim_road_zip old
            INNER JOIN tmp_road_delta new ON old.road_id = new.road_id
            WHERE old.dt = '${YESTERDAY}' AND old.is_current = 'Y'
            
            UNION ALL
            
            -- 变更记录: 插入新版本 (is_current='Y', start_time=今天)
            SELECT
                new.road_id, new.road_name, new.road_type, new.road_length,
                new.lane_count, new.speed_limit, new.area_id, new.direction,
                '${DATE}' AS start_time, '9999-12-31' AS end_time, 'Y' AS is_current
            FROM tmp_road_delta new
            
            UNION ALL
            
            -- 历史已闭合记录: 保持不变
            SELECT
                road_id, road_name, road_type, road_length, lane_count, speed_limit,
                area_id, direction, start_time, end_time, is_current
            FROM dim_road_zip
            WHERE dt = '${YESTERDAY}' AND is_current = 'N'
        ) t;
        
        -- 清理临时表
        DROP TABLE IF EXISTS tmp_road_delta;
    " || true
    
    echo "✅ dim_road_zip 增量更新完成"
    
    # dim_device_zip 增量更新 (类似逻辑)
    beeline -u "${HIVE_SERVER}/${HIVE_DB}" -e "
        SET hive.exec.dynamic.partition = true;
        SET hive.exec.dynamic.partition.mode = nonstrict;
        
        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_device_delta AS
        SELECT new.*
        FROM ods_device_info new
        LEFT JOIN (
            SELECT * FROM dim_device_zip 
            WHERE dt = '${YESTERDAY}' AND is_current = 'Y'
        ) old ON new.device_id = old.device_id
        WHERE new.dt = '${DATE}'
          AND (
              old.device_id IS NULL
              OR old.device_type <> new.device_type
              OR old.road_id <> new.road_id
              OR old.status <> new.status
              OR old.firmware_version <> new.firmware_version
          );
        
        INSERT OVERWRITE TABLE dim_device_zip PARTITION (dt = '${DATE}')
        SELECT
            device_id, device_type, road_id, area_id, install_date, status,
            ip_address, firmware_version, start_time, end_time, is_current
        FROM (
            SELECT
                device_id, device_type, road_id, area_id, install_date, status,
                ip_address, firmware_version, start_time, end_time, is_current
            FROM dim_device_zip
            WHERE dt = '${YESTERDAY}' AND is_current = 'Y'
              AND device_id NOT IN (SELECT device_id FROM tmp_device_delta)
            
            UNION ALL
            
            SELECT
                old.device_id, old.device_type, old.road_id, old.area_id,
                old.install_date, old.status, old.ip_address, old.firmware_version,
                old.start_time, '${YESTERDAY}' AS end_time, 'N' AS is_current
            FROM dim_device_zip old
            INNER JOIN tmp_device_delta new ON old.device_id = new.device_id
            WHERE old.dt = '${YESTERDAY}' AND old.is_current = 'Y'
            
            UNION ALL
            
            SELECT
                new.device_id, new.device_type, new.road_id, new.area_id,
                new.install_date, new.status, new.ip_address, new.firmware_version,
                '${DATE}' AS start_time, '9999-12-31' AS end_time, 'Y' AS is_current
            FROM tmp_device_delta new
            
            UNION ALL
            
            SELECT
                device_id, device_type, road_id, area_id, install_date, status,
                ip_address, firmware_version, start_time, end_time, is_current
            FROM dim_device_zip
            WHERE dt = '${YESTERDAY}' AND is_current = 'N'
        ) t;
        
        DROP TABLE IF EXISTS tmp_device_delta;
    " || true
    
    echo "✅ dim_device_zip 增量更新完成"
}

# ============================================================================
# 验证 SCD2 数据
# ============================================================================
verify_scd2() {
    echo "--- 验证 SCD2 拉链表数据 ---"
    
    beeline -u "${HIVE_SERVER}/${HIVE_DB}" -e "
        -- 统计当前有效记录
        SELECT 'dim_road_zip 当前有效' AS check_item, COUNT(*) AS cnt
        FROM dim_road_zip WHERE is_current = 'Y' AND dt = '${DATE}'
        UNION ALL
        SELECT 'dim_road_zip 历史记录', COUNT(*)
        FROM dim_road_zip WHERE is_current = 'N' AND dt = '${DATE}'
        UNION ALL
        SELECT 'dim_device_zip 当前有效', COUNT(*)
        FROM dim_device_zip WHERE is_current = 'Y' AND dt = '${DATE}'
        UNION ALL
        SELECT 'dim_device_zip 历史记录', COUNT(*)
        FROM dim_device_zip WHERE is_current = 'N' AND dt = '${DATE}';
    " || true
    
    echo "✅ SCD2 验证完成"
}

# ============================================================================
# 主函数
# ============================================================================
case "${1:-init}" in
    init)
        init_scd2
        verify_scd2
        ;;
    daily)
        daily_update_scd2
        verify_scd2
        ;;
    verify)
        verify_scd2
        ;;
    *)
        echo "用法: $0 {init|daily|verify}"
        echo "  init   - 首次初始化拉链表"
        echo "  daily  - 每日增量更新"
        echo "  verify - 验证拉链表数据"
        exit 1
        ;;
esac

echo "=========================================="
echo "SCD2 ETL 完成"
echo "=========================================="
