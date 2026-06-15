#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""调试 AI 助手 — 测试 Ollama 调用链路"""
import sys, os, json
sys.stdout.reconfigure(encoding='utf-8')
os.chdir(os.path.join(os.path.dirname(__file__), "python"))
sys.path.insert(0, os.getcwd())

from ai_assistant import AIAssistant

a = AIAssistant()

# 1. 检查模型列表
print("[1] Ollama 在线检测:")
print(f"    {a.ollama_online}")

# 2. 直接调用 Ollama
print("\n[2] 直接调用 Ollama:")
sql, err = a._ask_ollama("昨天最拥堵的5条路")
print(f"    SQL: {sql}")
print(f"    错误: {err}")

# 3. 看带错误的完整 ask
print("\n[3] ask() 完整返回:")
r = a.ask("昨天最拥堵的5条路")
print(f"    模式: {r['mode']}")
print(f"    SQL:  {str(r['sql'])[:100]}")
print(f"    错误: {r.get('error', '无')}")

input("\n按回车退出...")
