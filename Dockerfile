# syntax=docker/dockerfile:1.7
FROM apache/airflow:3.2.2-python3.12

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /usr/local/bin/uv

COPY --from=vn-news-platform-lib / /opt/airflow/vn-news-platform-lib/

COPY pyproject.toml uv.lock /opt/airflow/orchestration/

USER root

RUN cd /opt/airflow/orchestration \
    && uv export --frozen --no-dev --no-emit-project --output-file /tmp/orchestration.txt >/dev/null \
    && uv pip install --system --requirement /tmp/orchestration.txt \
    && uv pip install --system /opt/airflow/vn-news-platform-lib \
    && rm /tmp/orchestration.txt

USER airflow

COPY dags /opt/airflow/dags
