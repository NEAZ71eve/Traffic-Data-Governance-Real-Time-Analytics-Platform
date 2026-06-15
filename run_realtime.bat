@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo.
echo  ============================================================
echo   实时数据管道 — Kafka→Flink→Hive 全链路可视化
echo  ============================================================
echo.
python realtime_pipeline.py
pause
