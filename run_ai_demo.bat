@echo off
chcp 65001 >nul
cd /d "%~dp0\python"
echo ========================================
echo   🤖 AI 数据查询助手 Demo
echo ========================================
echo.
echo   Ollama: qwen3:8b
echo   Hive: localhost:10000
echo.
echo   demo 问题:
echo     1. 昨天最拥堵的5条路
echo     2. 今天车流量最大的3条道路
echo     3. 设备健康评分最低的3台设备
echo.
echo ========================================
echo.
python -X utf8 ai_assistant.py
pause
