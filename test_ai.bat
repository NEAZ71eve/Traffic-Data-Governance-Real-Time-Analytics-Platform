@echo off
chcp 65001 >nul
cd /d "%~dp0"
python -X utf8 test_ai_ollama.py
pause