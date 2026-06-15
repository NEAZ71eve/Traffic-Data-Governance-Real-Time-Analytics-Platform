@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo.
echo  ============================================================
echo   数据服务可视化 — 自动验证
echo  ============================================================
echo.
python verify_visualization.py
echo.
pause
