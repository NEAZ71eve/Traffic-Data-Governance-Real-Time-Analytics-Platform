#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验证 AI 助手（Ollama 模式）"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "python"))

from ai_assistant import AIAssistant, OLLAMA_MODEL

a = AIAssistant()
print(f"模型: {OLLAMA_MODEL}")
print(f"Ollama: {'✅ 在线' if a.ollama_online else '❌ 离线'}")
print(f"Hive:   {'✅ 在线' if a.hive.online else '❌ 离线'}")
print()

for q in [
    "昨天最拥堵的5条路",
    "设备健康评分最低的3台设备",
    "今天车流量最大的道路",
]:
    r = a.ask(q)
    print(f"问题: {q}")
    print(f"模式: {r['mode']}")
    print(f"SQL:  {r['sql']}")
    err = r.get("error", "") or "无"
    print(f"错误: {err}")
    print()

input("按回车退出...")
