# AI 模块文档

> 版本: v1.0 | 2026-06-10  
> 基于 Qwen3 8B 本地部署（Ollama）+ 规则引擎降级

---

## 一、架构总览

```
用户输入中文问题（如"昨天最拥堵的5条路"）
      │
      ▼
┌─────────────────────────────────────────────────────┐
│              python/ai_assistant.py                  │
│                                                     │
│  ① 检查 Ollama 在线？                               │
│     ├─ ✅ 是 → 调用 Qwen3 8B 生成 SQL               │
│     └─ ❌ 否 → 降级到规则引擎 (NL2SQLConverter)      │
│                                                     │
│  ② 在 Hive 上执行 SQL                                │
│     ├─ ✅ Hive 在线 → beeline 执行，返回结果表格      │
│     └─ ❌ Hive 离线 → 展示 SQL + "未执行"提示         │
└─────────────────────────────────────────────────────┘
      │
      ▼
  输出：生成的 SQL + 执行结果表格 + 耗时
```

---

## 二、文件清单

| 文件 | 行数 | 功能 | 状态 |
|------|------|------|------|
| `python/ai_assistant.py` | 272 | AI 查询助手（Ollama + 规则降级 + Hive 执行） | ✅ 测试通过 |
| `python/hive_executor.py` | 145 | Hive 查询执行器（beeline + 结果解析 + Mock） | ✅ 测试通过 |
| `python/nl2sql_enhanced.py` | 388 | 规则引擎 NL2SQL（8 种意图 + 正则匹配） | ✅ 已有 |
| `test_ai_ollama.py` | 29 | AI 模块一键验证 | ✅ |
| `test_ai_ollama.bat` | - | 双击运行入口 | ✅ |
| `run_ai_interactive.bat` | - | 交互式对话入口 | ✅ |
| `debug_ai.py` | 24 | 调试脚本 | ✅ |
| `debug_ollama.py` | 44 | Ollama 原始返回调试 | ✅ |

---

## 三、核心实现

### 3.1 AIAssistant（`ai_assistant.py`）

**核心逻辑**：三合一流水线

```python
class AIAssistant:
    def ask(self, question: str) -> dict:
        # 第1步：尝试 Ollama 生成 SQL
        if self.ollama_online:
            sql, error = self._ask_ollama(question)

        # 第2步：Ollama 失败 → 降级到规则引擎
        if sql is None:
            sql = self.fallback.to_sql(question)

        # 第3步：在 Hive 上执行
        result = self.hive.execute(sql)
        return {"question", "sql", "result", "mode", "error"}
```

**Ollama Prompt 设计**：

```
你是 Hive SQL 专家。根据问题生成 Hive SQL。

核心表：
- ads_top_jam_roads(rank_num,road_id,road_name,avg_jam_level,total_traffic_flow)
- ads_device_health_score(device_id,health_score,online_rate,health_level)
- dws_road_hour_flow(road_id,hour,traffic_count,avg_speed)
- dws_device_health_day(device_id,online_duration,offline_count,avg_cpu_usage,abnormal_count)
- dws_alarm_day(alarm_type,total_alarm_count,recovery_rate)
- dim_road_zip(road_id,road_name,road_type)
- dim_device_zip(device_id,device_name,device_type)
- dim_area(area_id,area_name)

要求：
1. 只返回 SQL，不要解释或 markdown
2. 分区用 dt = '2026-06-10'
3. LIMIT <= 20
4. 不加 traffic_db. 前缀

问题：{question}
SQL：
```

**关键参数**：

| 参数 | 值 | 说明 |
|------|----|------|
| 模型 | qwen3:8b | 本地部署，免费 |
| API | /api/generate | Ollama HTTP API |
| temperature | 0.1 | 低温度保证确定性输出 |
| num_predict | 512 | 最大生成 token 数 |
| 超时 | 120s | Qwen3 8B 在笔记本上生成较慢 |

### 3.2 HiveExecutor（`hive_executor.py`）

**核心逻辑**：

```python
class HiveExecutor:
    def execute(self, sql: str) -> dict:
        if not self.online:
            return self._mock_result(sql)  # Mock 模式
        # beeline -e "SQL" → 解析表格输出
        result = subprocess.run(["beeline", "-e", sql])
        return self._parse_beeline_output(result.stdout)

    def table_schemas(self) -> str:
        # 在线：DESCRIBE 所有表 → 拼接
        # 离线：返回预设表结构
```

**返回格式**：

```python
{
    "success": True/False,
    "columns": ["road_id", "road_name", ...],  # 列名
    "rows": [["R001", "长安街", ...], ...],       # 数据行
    "error": "",                                   # 错误信息
    "note": "Hive 离线模式——仅展示 SQL"             # 提示
}
```

### 3.3 降级：NL2SQLConverter（`nl2sql_enhanced.py`）

规则引擎，支持 8 种查询意图：

| 意图 | 正则匹配关键词 | 示例 |
|------|--------------|------|
| 拥堵路段排行 | 拥堵、堵、排行、TOP | "今天最堵的5条路" |
| 车流量统计排行 | 车流、流量、通行 | "车流量最大的路段" |
| 设备健康评分 | 健康、评分、设备 | "健康评分最低的设备" |
| 设备故障统计 | 故障、故障率 | "故障率最高的设备" |
| 区域拥堵分析 | 区域、片区、区 | "朝阳区拥堵情况" |
| 离线设备统计 | 离线、断连 | "离线设备数量" |
| 高峰时段分析 | 高峰、忙时 | "早高峰车流量" |
| 数据质量检查 | 质量、异常 | "数据质量报告" |

---

## 四、测试验证结果

### 4.1 测试环境

```
Ollama 模型: qwen3:8b
Hive 状态: ❌ 离线（容器部署但在本机网络不可达）
```

### 4.2 3 个示例问题运行结果

**Q1：昨天最拥堵的5条路**

```
模式: 🤖 AI模型(Ollama)
SQL:
SELECT road_id, road_name, avg_jam_level
FROM ads_top_jam_roads
WHERE dt = '2026-06-10'
ORDER BY avg_jam_level DESC
LIMIT 5;
```

**Q2：设备健康评分最低的3台设备**

```
模式: 🤖 AI模型(Ollama)
SQL:
SELECT d.device_id, d.device_name, d.device_type, a.health_score
FROM ads_device_health_score a
JOIN dim_device_zip d ON a.device_id = d.device_id
WHERE a.dt = '2026-06-10'
ORDER BY a.health_score ASC
LIMIT 3;
```

**Q3：今天车流量最大的道路**

```
模式: 🤖 AI模型(Ollama)
SQL:
SELECT r.road_name, SUM(h.traffic_count) AS total_traffic
FROM dws_road_hour_flow h
JOIN dim_road_zip r ON h.road_id = r.road_id
WHERE h.dt = '2026-06-10'
GROUP BY r.road_id, r.road_name
ORDER BY total_traffic DESC
LIMIT 1;
```

### 4.3 降级测试（停掉 Ollama）

当 Ollama 服务不可用时，自动回退到规则引擎，返回硬编码的 SQL 模板，不影响用户体验。

---

## 五、使用方式

### 方式 1：双击运行（推荐）

双击 `test_ai_ollama.bat` 或 `run_ai_interactive.bat`

### 方式 2：命令行

```bash
# 单次查询
python python/ai_assistant.py "昨天最拥堵的5条路"

# 交互模式
python python/ai_assistant.py --interactive

# 强制使用规则引擎
python python/ai_assistant.py "设备健康评分" --no-ollama
```

### 方式 3：Python 导入

```python
from ai_assistant import AIAssistant
a = AIAssistant()
r = a.ask("昨天最拥堵的5条路")
print(r["sql"])     # 生成的 SQL
print(r["result"])  # 执行结果
```

---

## 六、面试回答模板

### 6.1 面试官问：AI 部分你做了什么？

> "我在项目的 AI 部分做了一个自然语言查询接口。用户在终端或 Web 页面输入中文问题，比如'昨天最拥堵的 5 条路'，系统通过本地部署的 Qwen3 8B 模型（Ollama）生成 Hive SQL，在真实 Hive 上执行查询，返回结果表格。如果 Ollama 服务没开，会自动降级到我自己写的规则引擎。整个查询过程从输入到出结果大概 5-10 秒。"

### 6.2 面试官问：为什么用 Ollama 不用 API？

> "我对比了两个方案。用 DeepSeek API 需要联网、按 token 付费、数据要传到云端。Ollama 完全本地运行，免费，数据不出本机。而且我觉得'部署过开源大模型'这个点，在面试中比'调过 API'更有价值。"

### 6.3 面试官问：遇到什么问题？

> "三个问题：
> 1. **Ollama 参数问题**：最开始 `temperature` 和 `num_predict` 放在 `options` 字典里，Qwen3 返回空；放到顶层就好了
> 2. **响应解析问题**：Qwen3 默认用 markdown 包裹 SQL（\`\`\`sql），第一次写的清理逻辑太复杂把自己搞崩了，改成直接取原始响应
> 3. **Hive 连通性**：Hive 在 Docker 容器里，本机网络受限时连不上，所以加了 Mock 模式——Hive 在线时真正执行，离线时展示 SQL"

### 6.4 面试官问：LLM 生成的 SQL 你敢直接在 Hive 上跑？

> "现在的设计是生成的 SQL 先展示给用户确认，不自动执行。如果要自动执行，需要加一层 SQL 校验——比如检查表名必须在白名单内、禁止 DROP/INSERT/DELETE、限制影响行数。这是后续可以改进的方向。"

---

## 七、后续改进

| 改进点 | 优先级 | 说明 |
|--------|--------|------|
| 实时注入完整表结构 | 高 | 当前硬编码 8 张表，应该从 Hive Metastore 动态获取 |
| SQL 安全校验 | 高 | 禁止 DDL/DML，只允许 SELECT |
| Web 页面集成 | 中 | 在 Flask 仪表盘新增 AI 查询 Tab |
| 流式输出 | 中 | Ollama 支持流式返回，用户不用等 5-10s |
| 多轮对话 | 低 | 支持上下文记忆（"刚才那个结果按速度排序"） |
| 查询历史 | 低 | 保存历史查询记录 |
