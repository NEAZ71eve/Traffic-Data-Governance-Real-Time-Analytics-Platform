@echo off
chcp 65001 >nul
cd /d "%~dp0\.."
echo.
echo  ============================================================
echo   智慧城市交通数据治理平台 — 带数据模拟的启动
echo  ============================================================
echo.
echo  [1/4] 检查 Docker 环境...

docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo  [FAIL] Docker 未运行，请先启动 Docker Desktop
    pause
    exit /b 1
)
echo  [OK] Docker 已就绪

echo.
echo  [2/4] 启动核心服务 (Kafka + Flink + Redis + App)...
docker compose -p traffic up -d
if %errorlevel% neq 0 (
    echo  [FAIL] 服务启动失败
    pause
    exit /b 1
)

echo  等待服务就绪...
:check_health
timeout /t 5 /nobreak >nul
curl -s http://localhost:8088/api/health | findstr "healthy" >nul
if %errorlevel% neq 0 goto check_health
echo  [OK] 核心服务已就绪

echo.
echo  [3/4] 启动 Kafka 数据采集模拟器 (容器化)...
docker compose -p traffic --profile simulators up -d simulator-kafka

echo  [OK] 数据模拟器已启动 (容器内运行)

echo.
echo  [4/4] 启动实时管道可视化 (端口 8090)...
start "Pipeline-Viz" /MIN python realtime_pipeline.py

echo.
echo  ============================================================
echo   启动完成!
echo  ============================================================
echo.
echo   仪表盘:        http://localhost:8088
echo   管道可视化:    http://localhost:8090
echo   Flink UI:      http://localhost:8081
echo.
echo   Kafka 数据模拟器正在后台生成实时数据
echo   关闭此窗口不会停止服务, 用 docker compose down 停止
echo  ============================================================
echo.
pause
