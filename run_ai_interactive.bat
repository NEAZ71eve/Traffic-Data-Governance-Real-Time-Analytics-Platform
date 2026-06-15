@echo off
chcp 65001 >nul
cd /d "%~dp0\python"
python -X utf8 ai_assistant.py --interactive
pause
