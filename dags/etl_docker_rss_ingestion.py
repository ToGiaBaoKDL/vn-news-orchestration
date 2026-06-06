from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from utils.config import (
    dag_id_from_file,
    load_rss_ingestion_config,
)
from utils.env import forwarded_environment, required_env
from utils.sources import load_enabled_rss_source_ids

DAG_ID = dag_id_from_file(__file__)
CONFIG = load_rss_ingestion_config()
CONTAINER_CREDENTIALS_PATH = f"{CONFIG.container_secrets_dir}/{CONFIG.ingestion_credentials_file}"
TASK_LABELS = {
    "com.tgbao.vn-news.component": "rss-ingestion",
    "com.tgbao.vn-news.dag-id": DAG_ID,
    "com.tgbao.vn-news.managed-by": "airflow",
    "com.tgbao.vn-news.service": "feed-ingestor",
}


with DAG(
    dag_id=DAG_ID,
    default_args={"owner": CONFIG.owner},
    schedule=CONFIG.schedule,
    start_date=pendulum.datetime(2026, 6, 1, tz=CONFIG.timezone),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "docker", "rss"],
) as dag:
    for source_id in load_enabled_rss_source_ids():
        DockerOperator(
            task_id=f"ingest_{source_id}",
            image=required_env(CONFIG.feed_ingestor_image_env),
            command=["--source-id", source_id, "--all-feeds"],
            container_name=(
                f"vn-news-feed-ingestor-{source_id}-{{{{ ts_nodash }}}}-try-{{{{ ti.try_number }}}}"
            ),
            labels={**TASK_LABELS, "com.tgbao.vn-news.source-id": source_id},
            docker_url=required_env(CONFIG.docker_daemon_url_env),
            mounts=[
                Mount(
                    source=required_env(CONFIG.host_config_dir_env),
                    target=CONFIG.container_config_dir,
                    type="bind",
                    read_only=True,
                ),
                Mount(
                    source=str(
                        Path(required_env(CONFIG.host_secrets_dir_env))
                        / CONFIG.ingestion_credentials_file
                    ),
                    target=CONTAINER_CREDENTIALS_PATH,
                    type="bind",
                    read_only=True,
                ),
            ],
            environment={
                "AWS_SHARED_CREDENTIALS_FILE": CONTAINER_CREDENTIALS_PATH,
                "VN_NEWS_CONFIG_DIR": CONFIG.container_config_dir,
                **forwarded_environment(CONFIG.forwarded_env),
            },
            mount_tmp_dir=False,
            auto_remove="success",
            force_pull=False,
            retries=CONFIG.retries,
            retry_delay=timedelta(minutes=CONFIG.retry_delay_minutes),
            execution_timeout=timedelta(minutes=CONFIG.execution_timeout_minutes),
            doc=f"Scrape all configured RSS feeds for {source_id}.",
        )
