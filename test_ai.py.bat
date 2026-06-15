@echo off
chcp 65001 >nul
cd /d "%~dp0\python"
echo ========================================
echo   🤖 AI 助手 - 快速验证
echo ========================================
echo.
echo   Step 1: 检查 Ollama 状态
python -X utf8 -c "from ai_assistant import AIAssistant; a=AIAssistant(); print('  Ollama:', '✅ 在线' if a.ollama_online else '❌ 离线'); print('  Hive:', '✅ 在线' if a.hive.online else '❌ 离线')"
echo.
echo   Step 2: 测试问题1 - 最拥堵的路
python -X utf8 -c "from ai_assistant import AIAssistant; a=AIAssistant(); r=a.ask('昨天最拥堵的5条路'); print('  SQL:', r['sql'][:100]); print('  模式:', r['mode'])"
echo.
echo   Step 3: 测试问题2 - 车流量
python -X utf8 -c "from ai_assistant import AIAssistant; a=AIAssistant(); r=a.ask('今天车流量最大的道路'); print('  SQL:', r['sql'][:100]); print('  模式:', r['mode'])"
echo.
echo   Step 4: 测试问题3 - 设备健康
python -X utf8 -c "from ai_assistant import AIAssistant; a=AIAssistant(); r=a.ask('健康评分最低的设备'); print('  SQL:', r['sql'][:100]); print('  模式:', r['mode'])"
echo.
echo ========================================
echo   完成！按任意键退出
pause >nul
