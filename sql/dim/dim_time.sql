CREATE TABLE IF NOT EXISTS dim_time (
    time_id STRING COMMENT '时间ID(yyyyMMddHHmmss)',
    dt STRING COMMENT '日期(yyyy-MM-dd)',
    hour INT COMMENT '小时(0-23)',
    minute INT COMMENT '分钟(0-59)',
    second INT COMMENT '秒(0-59)',
    day_of_week INT COMMENT '周几(1-7)',
    week_of_year INT COMMENT '周数(1-52)',
    month INT COMMENT '月份(1-12)',
    quarter INT COMMENT '季度(1-4)',
    year INT COMMENT '年份',
    is_weekend STRING COMMENT '是否周末(Y/N)',
    is_holiday STRING COMMENT '是否节假日(Y/N)',
    time_period STRING COMMENT '时段(凌晨/上午/中午/下午/傍晚/晚上)'
) COMMENT '时间维度表'
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);