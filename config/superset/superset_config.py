"""Superset 配置文件 — 允许 SQLite 数据源用于快速部署"""
PREVENT_UNSAFE_DB_CONNECTIONS = False
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
