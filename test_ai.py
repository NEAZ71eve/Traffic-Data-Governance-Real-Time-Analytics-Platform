#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AI 助手快速验证脚本 — 双击运行"""
import subprocess, sys, os
sys.stdout.reconfigure(encoding='utf-8')

os.chdir(os.path.join(os.path.dirname(__file__), "python"))
sys.path.insert(0, os.getcwd())

from ai_assistant import AIAssistant

print("=" * 55)
print("   🤖 AI 数据查询助手 · 快速验证")
print("=" * 55)

a = AIAssistant()
print(f"\n  [1/4] Ollama: {'✅ 在线' if a.ollama_online else '❌ 离线'}")
print(f"  [1/4] Hive:   {'✅ 在线' if a.hive.online else '❌ 离线'}")

questions = [
    "昨天最拥堵的5条路",
    "今天车流量最大的道路",
    "设备健康评分最低的3台设备",
]

for i, q in enumerate(questions, 2):
    print(f"\n  [{i}/4] 问题: {q}")
    try:
        ans = a.ask(q)
        sql_short = (ans['sql'][:80] + '...') if ans['sql'] and len(ans['sql']) > 80 else (ans['sql'] or '无')
        print(f"  模式: {'🤖 Ollama' if ans['mode']=='ollama' else '⚙️ 规则引擎'}")
        print(f"  SQL:  {sql_short}")
    except Exception as e:
        print(f"  ❌ 错误: {e}")

print("\n" + "=" * 55)
print("   完成！按回车退出")
input()
