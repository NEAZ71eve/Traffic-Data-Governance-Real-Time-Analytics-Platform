# ============================================================
# 智慧城市交通数据治理 — Python 应用镜像
# 包含: 仪表盘 + Spark/PyFlink ETL + 数据质量监控 + 血缘分析
# ============================================================
FROM python:3.12-slim

LABEL project="Traffic-Data-Governance"
LABEL description="智慧城市交通数据治理平台 — Python运行时"

# 系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    procps \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ============================================================
# Python 依赖
# ============================================================
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip install --no-cache-dir pyspark==3.5.1 apache-flink==1.18.1 -i https://pypi.tuna.tsinghua.edu.cn/simple

# ============================================================
# 复制应用代码
# ============================================================
COPY dashboard_app.py /app/
COPY dashboard.html /app/
COPY demo_full_pipeline.py /app/
COPY config/ /app/config/
COPY sql/ /app/sql/
COPY python/ /app/python/
COPY pseudo_distributed/ /app/pseudo_distributed/
COPY init/ /app/init/

# ============================================================
# 创建数据目录
# ============================================================
RUN mkdir -p /app/data

# ============================================================
# 默认启动仪表盘 (可在 docker-compose 中覆盖)
# ============================================================
EXPOSE 8088

HEALTHCHECK --interval=20s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8088/api/health || exit 1

CMD ["python", "dashboard_app.py"]
