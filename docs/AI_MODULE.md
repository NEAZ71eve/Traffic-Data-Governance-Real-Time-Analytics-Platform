# AI 模块文档

> v1.0 | 2026-06-10 | Qwen3 8B (Ollama) + 规则引擎降级

## 架构

```
用户输入 → ai_assistant.py
  ① Ollama 在线? → ✅ Qwen3 8B 生成 SQL / ❌ 降级规则引擎
  ② Hive 在线?   → ✅ beeline 执行返回结果 / ❌ 展示 SQL
```

## 文件清单

| 文件 | 功能 | 状态 |
|------|------|------|
| python/ai_assistant.py | AI 查询助手 (Ollama+规则降级+Hive执行) | ✅ |
| python/hive_executor.py | Hive 执行器 (beeline+Mock) | ✅ |
| python/nl2sql_enhanced.py | 规则引擎 NL2SQL (8种意图) | ✅ |
| test_all_ai_modules.py | AI 模块一键验证 | ✅ 6/6 |

## 核心实现

### AIAssistant

```
__init__(ollama_url, model) → 连接 Ollama
query(natural_language, execute) → SQL + 结果
  ├── _generate_sql() → Ollama 或规则引擎
  ├── _execute_sql() → Hive 或 Mock
  └── _format_result() → 表格
```

### 规则引擎降级

| 查询意图 | 匹配关键词 | 示例 |
|---------|-----------|------|
| 拥堵TOP N | 拥堵/最堵/TOP | "最拥堵的5条路" |
| 车流统计 | 车流/流量/车辆数 | "长安街今日车流" |
| 平均车速 | 平均车速/速度 | "各路段平均车速" |
| 设备健康 | 健康/在线/离线 | "健康评分最低的设备" |
| 趋势分析 | 趋势/变化/走势 | "早高峰车流趋势" |
| 对比分析 | 对比/环比/相比 | "今日与昨日车流对比" |
| 告警查询 | 告警/故障/异常 | "今日告警统计" |
| 质量报告 | 质量/完整率/空值 | "数据质量报告" |

## 使用

```bash
# 交互式
python python/ai_assistant.py

# 一键验证全部 AI 模块
python test_all_ai_modules.py

# 启动对话
run_ai_interactive.bat
```
