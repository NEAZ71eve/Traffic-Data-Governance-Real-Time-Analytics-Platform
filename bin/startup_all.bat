@echo off
chcp 65001 >nul
cd /d "%~dp0\.."
echo.
echo  ╔══════════════════════════════════════════════════════════╗
echo  ║  智慧城市交通数据治理平台 — 一键完整启动                    ║
echo  ╚══════════════════════════════════════════════════════════╝
echo.
echo  包含服务:
echo    [核心] Kafka + Flink + Redis + Flask 仪表盘 (:8088)
echo    [数据] Kafka 实时数据采集模拟器
echo    [可视化] 实时管道可视化 (:8090)
echo    [告警] DingTalk 告警桥接适配器 (:5000)
echo    [BI] Superset 可视化大屏 (:8089)
echo.
echo  ─────────────────────────────────────────────────────────
echo.

rem ============================================================
rem Step 1: 核心服务
rem ============================================================
echo  [1/5] 启动核心服务集群...
docker compose -p traffic up -d
if %errorlevel% neq 0 (
    echo  [FAIL] 核心服务启动失败
    pause
    exit /b 1
)

echo  等待核心服务就绪...
:wait_core
timeout /t 5 /nobreak >nul
curl -s http://localhost:8088/api/health 2>&1 | findstr "healthy" >nul
if %errorlevel% neq 0 goto wait_core
echo  [OK] 核心服务就绪 (Kafka + Flink + Redis + App)

rem ============================================================
rem Step 2: Kafka 数据模拟（容器化）
rem ============================================================
echo.
echo  [2/5] 启动 Kafka 实时数据采集模拟器...
docker compose -p traffic --profile simulators up -d simulator-kafka
echo  [OK] 数据模拟器已启动

rem ============================================================
rem Step 3: 管道可视化
rem ============================================================
echo.
echo  [3/5] 启动实时管道可视化 (端口 8090)...
start "Pipeline-Viz" /MIN python realtime_pipeline.py
echo  [OK] 管道可视化已启动

rem ============================================================
rem Step 4: 告警适配器（容器化）
rem ============================================================
echo.
echo  [4/5] 启动 DingTalk 告警桥接适配器 (端口 5000)...
docker compose -f docker-compose-monitoring.yml up -d alertmanager-adapter
echo  [OK] 告警适配器已启动

rem ============================================================
rem Step 5: Superset (可选)
rem ============================================================
echo.
echo  [5/5] 启动 Superset 可视化大屏?
echo.
set /p START_SUPERSET="  是否启动 Superset? (需要额外 ~2GB 内存) [y/N]: "
if /i "%START_SUPERSET%"=="y" (
    echo  启动 Superset + 自动配置仪表盘...
    call bin\startup_superset.bat
) else (
    echo  [SKIP] 跳过 Superset (可稍后运行 bin\startup_superset.bat)
)

echo.
echo  ╔══════════════════════════════════════════════════════════╗
echo  ║  平台启动完成!                                           ║
echo  ╠══════════════════════════════════════════════════════════╣
echo  ║                                                          ║
echo  ║  仪表盘:       http://localhost:8088                      ║
echo  ║  管道可视化:   http://localhost:8090                      ║
echo  ║  Flink UI:     http://localhost:8081                      ║
echo  ║  Superset:     http://localhost:8089  (若已启动)          ║
echo  ║  告警适配器:   http://localhost:5000/health               ║
echo  ║                                                          ║
echo  ╚══════════════════════════════════════════════════════════╝
echo.
echo  后台进程: 管道可视化
echo  容器服务: Kafka模拟器 / 告警适配器 / 核心服务
echo  停止全部:  docker compose -p traffic down
echo.
pause
