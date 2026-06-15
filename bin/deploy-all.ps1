# ============================================================================
# deploy-all.ps1
# 智慧城市交通大数据平台 — Windows PowerShell 一键部署脚本
#
# 用法:
#   .\bin\deploy-all.ps1 quickstart    # 最小化部署 (5容器)
#   .\bin\deploy-all.ps1 deploy        # 完整生产部署 (24容器)
#   .\bin\deploy-all.ps1 status        # 查看服务状态
#   .\bin\deploy-all.ps1 verify        # 全链路验证
#   .\bin\deploy-all.ps1 stop          # 停止所有服务
# ============================================================================

param(
    [Parameter(Position=0)]
    [ValidateSet("quickstart", "qs", "deploy", "full", "status", "ps", "verify", "test", "stop", "down", "restart", "help")]
    [string]$Command = "help"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectHome = Split-Path -Parent $ScriptDir
$ComposeSimple = Join-Path $ProjectHome "docker-compose.yml"
$ComposeProd = Join-Path $ProjectHome "docker-compose-production.yml"
$ComposePhase2 = Join-Path $ProjectHome "docker-compose-phase2.yml"
$ComposeMonitor = Join-Path $ProjectHome "docker-compose-monitoring.yml"

# 颜色函数
function Write-Info { Write-Host "[INFO]    $args" -ForegroundColor Blue }
function Write-Success { Write-Host "[✓]       $args" -ForegroundColor Green }
function Write-Warn { Write-Host "[WARN]    $args" -ForegroundColor Yellow }
function Write-Error { Write-Host "[ERROR]   $args" -ForegroundColor Red }
function Write-Step { Write-Host ""; Write-Host "> $args" -ForegroundColor Cyan }

# 检查 Docker
function Test-Docker {
    Write-Step "🔍 环境检查"
    try {
        $null = docker info 2>$null
        Write-Success "Docker 已就绪"
        $mem = (docker system info --format '{{.MemTotal}}' 2>$null) -as [long]
        $memGB = [math]::Round($mem / 1GB, 1)
        if ($memGB -ge 16) { Write-Success "内存: ${memGB}GB (满足完整部署)" }
        elseif ($memGB -ge 8) { Write-Warn "内存: ${memGB}GB (建议 quickstart)" }
        else { Write-Error "内存不足 ${memGB}GB" }
        return $true
    } catch {
        Write-Error "Docker 未运行，请启动 Docker Desktop"
        return $false
    }
}

# 快速启动
function Start-Quickstart {
    Write-Step "⚡ 快速启动模式 (5容器)"
    docker compose -f $ComposeSimple up -d
    Write-Info "等待服务就绪 (约60秒)..."
    Start-Sleep 30
    Write-Success "快速启动完成!"
    Write-Host ""
    Write-Host "  📊 仪表盘:    http://localhost:8088" -ForegroundColor Green
    Write-Host "  ⚡ Flink UI:  http://localhost:8081" -ForegroundColor Green
    Write-Host "  📨 Kafka:     localhost:9092" -ForegroundColor Green
    Write-Host "  💾 Redis:     localhost:6379" -ForegroundColor Green
}

# 完整部署
function Start-FullDeploy {
    Write-Step "🚀 完整生产级部署 (24服务)"

    Write-Info "第一阶段: 启动核心集群..."
    docker compose -f $ComposeProd up -d
    Write-Info "等待基础设施就绪 (约120秒)..."
    Start-Sleep 60

    Write-Info "第二阶段: 启动数据采集+调度+可视化..."
    docker compose -f $ComposePhase2 up -d
    Write-Info "等待第二阶段就绪 (约60秒)..."
    Start-Sleep 30

    Write-Success "完整部署完成!"
    Write-Host ""
    Write-Host "  📊 仪表盘:           http://localhost:8088" -ForegroundColor Green
    Write-Host "  ⚡ Flink UI:         http://localhost:8081" -ForegroundColor Green
    Write-Host "  🗄️ HDFS:             http://localhost:9870" -ForegroundColor Green
    Write-Host "  🐬 DolphinScheduler: http://localhost:12345" -ForegroundColor Green
    Write-Host "     (admin/dolphinscheduler123)" -ForegroundColor Gray
    Write-Host "  📈 Superset:         http://localhost:8088" -ForegroundColor Green
    Write-Host "     (admin/admin123)" -ForegroundColor Gray
}

# 状态检查
function Show-Status {
    Write-Step "📊 服务状态"
    docker compose -f $ComposeSimple ps 2>$null
    docker compose -f $ComposeProd ps 2>$null
    docker compose -f $ComposePhase2 ps 2>$null
}

# 停止
function Stop-All {
    Write-Step "🛑 停止所有服务"
    docker compose -f $ComposePhase2 down 2>$null
    docker compose -f $ComposeProd down 2>$null
    docker compose -f $ComposeSimple down 2>$null
    Write-Success "所有服务已停止"
}

# 验证
function Invoke-Verify {
    Write-Step "🔬 全链路验证"
    $pass = 0; $fail = 0

    # Kafka
    try { $null = docker exec traffic-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list 2>$null; Write-Success "Kafka [OK]"; $pass++ } catch { Write-Error "Kafka [FAIL]"; $fail++ }

    # Flink
    try { $r = Invoke-WebRequest -Uri http://localhost:8081/overview -TimeoutSec 5; Write-Success "Flink [OK]"; $pass++ } catch { Write-Error "Flink [FAIL]"; $fail++ }

    # Redis
    try { $r = docker exec traffic-redis-1 redis-cli ping 2>$null; Write-Success "Redis [OK]"; $pass++ } catch { Write-Error "Redis [FAIL]"; $fail++ }

    Write-Host ""
    Write-Host "  通过: $pass  /  失败: $fail" -ForegroundColor $(if($fail -eq 0){'Green'}else{'Yellow'})
}

# 帮助
function Show-Help {
    Write-Host @"

  🚦 智慧城市交通大数据平台 — 一键部署工具 (PowerShell)

  用法: .\bin\deploy-all.ps1 <命令>

  命令:
    quickstart    最小化部署 (5容器, Kafka+Flink+Redis+App)
    deploy        完整生产级部署 (24容器, 高可用集群)
    status        查看所有服务状态
    verify        全链路验证测试
    stop          停止所有服务
    restart       重启所有服务

  示例:
    .\bin\deploy-all.ps1 quickstart
    .\bin\deploy-all.ps1 deploy
    .\bin\deploy-all.ps1 status

  注意: 如果在 Git Bash 中运行，也可以用:
    bash bin/deploy-all.sh deploy

"@
}

# ===== 主入口 =====
Set-Location $ProjectHome

switch ($Command) {
    "quickstart" { if (Test-Docker) { Start-Quickstart } }
    "qs"         { if (Test-Docker) { Start-Quickstart } }
    "deploy"     { if (Test-Docker) { Start-FullDeploy } }
    "full"       { if (Test-Docker) { Start-FullDeploy } }
    "status"     { Show-Status }
    "ps"         { Show-Status }
    "verify"     { Invoke-Verify }
    "test"       { Invoke-Verify }
    "stop"       { Stop-All }
    "down"       { Stop-All }
    "restart"    { Stop-All; Start-Sleep 3; if (Test-Docker) { Start-FullDeploy } }
    default      { Show-Help }
}
