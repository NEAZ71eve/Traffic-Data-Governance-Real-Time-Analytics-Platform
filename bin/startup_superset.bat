@echo off
chcp 65001 >nul
cd /d "%~dp0\.."
echo.
echo  ============================================================
echo   启动 Superset 可视化大屏 (端口 8089)
echo  ============================================================
echo.
echo  [1/3] 检查 Docker 环境...
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo  [FAIL] Docker 未运行
    pause
    exit /b 1
)
echo  [OK] Docker 已就绪

echo.
echo  [2/3] 启动 Superset + PostgreSQL...
docker compose -f docker-compose-phase2.yml up -d superset-db superset
if %errorlevel% neq 0 (
    echo  [FAIL] 启动失败
    pause
    exit /b 1
)

echo  等待 Superset 就绪 (约60秒)...
:check_superset
timeout /t 10 /nobreak >nul
curl -s http://localhost:8089/health >nul 2>&1
if %errorlevel% neq 0 goto check_superset
echo  [OK] Superset 已就绪

echo.
echo  [3/3] 导入仪表盘和数据源...
python bin\import_superset_dashboards.py --host http://localhost:8089

echo.
echo  ============================================================
echo   Superset 可视化大屏已就绪!
echo  ============================================================
echo.
echo   访问地址:  http://localhost:8089
echo   用户名:    admin
echo   密码:      admin123
echo.
echo   Flask 仪表盘:  http://localhost:8088
echo  ============================================================
echo.
pause
