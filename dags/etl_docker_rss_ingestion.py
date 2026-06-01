from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DAG_ID = os.path.basename(__file__).replace(".py", "")
DAG_OWNER = "data-engineering"
RSS_SOURCE_IDS = ("vnexpress",)

with DAG(
    dag_id=DAG_ID,
    default_args={"owner": DAG_OWNER},
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "docker", "rss"],
) as dag:
    for source_id in RSS_SOURCE_IDS:
        DockerOperator(
            task_id=f"ingest_{source_id}",
            image=os.environ["VN_NEWS_FEED_INGESTOR_IMAGE"],
            command=["--source-id", source_id, "--all-feeds"],
            docker_url="unix://var/run/docker.sock",
            network_mode=os.environ.get(
                "VN_NEWS_DOCKER_NETWORK",
                "vn-news-intelligence_default",
            ),
            mounts=[
                Mount(
                    source=os.environ["VN_NEWS_CONFIG_HOST_DIR"],
                    target="/app/configs",
                    type="bind",
                    read_only=True,
                )
            ],
            environment={
                "TGB_ENV": os.environ.get("TGB_ENV", "local"),
                "VN_NEWS_CONFIG_DIR": "/app/configs",
            },
            mount_tmp_dir=False,
            auto_remove="success",
            force_pull=False,
            retries=2,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=15),
            doc=f"Scrape all configured RSS feeds for {source_id}.",
        )
