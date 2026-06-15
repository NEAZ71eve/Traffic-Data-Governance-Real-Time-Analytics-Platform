@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ==================================
echo   🤖 AI 助手快速测试（3个问题）
echo ==================================
echo.
python -X utf8 -c "
import sys
sys.path.insert(0, 'python')
from ai_assistant import AIAssistant, OLLAMA_MODEL
a = AIAssistant()
print(f'模型: {OLLAMA_MODEL}')
print(f'Ollama: {\"在线\" if a.ollama_online else \"离线\"}')
print(f'Hive:   {\"在线\" if a.hive.online else \"离线\"}')
print()
for q in ['昨天最拥堵的5条路','设备健康评分最低的3台设备','今天车流量最大的道路']:
    r = a.ask(q)
    print(f'问题: {q}')
    print(f'模式: {r[\"mode\"]}')
    print(f'SQL:  {r[\"sql\"]}')
    print(f'错误: {r.get(\"error\",\"\") or \"无\"}')
    print()
"
pause