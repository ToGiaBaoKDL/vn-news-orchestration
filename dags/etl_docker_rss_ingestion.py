from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, TaskGroup
from airflow.task.trigger_rule import TriggerRule
from docker.types import Mount

from utils.config import (
    dag_id_from_file,
    load_rss_ingestion_config,
)
from utils.env import forwarded_environment, required_env
from utils.sources import load_enabled_rss_sources

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
    schedule=timedelta(minutes=CONFIG.schedule_interval_minutes),
    start_date=pendulum.datetime(2026, 6, 1, tz=CONFIG.timezone),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=CONFIG.max_active_tasks,
    tags=["etl", "docker", "rss"],
) as dag:
    for source in load_enabled_rss_sources():
        with TaskGroup(group_id=source.source_id):
            previous_task = None
            for feed_id in source.feed_ids:
                task = DockerOperator(
                    task_id=feed_id,
                    image=required_env(CONFIG.feed_ingestor_image_env),
                    command=["--source-id", source.source_id, "--feed-id", feed_id],
                    container_name=(
                        f"vn-news-feed-ingestor-{source.source_id}-{feed_id}-"
                        "{{ ts_nodash }}-try-{{ ti.try_number }}"
                    ),
                    labels={
                        **TASK_LABELS,
                        "com.tgbao.vn-news.source-id": source.source_id,
                        "com.tgbao.vn-news.feed-id": feed_id,
                    },
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
                    trigger_rule=TriggerRule.ALL_DONE if previous_task else TriggerRule.ALL_SUCCESS,
                    doc=f"Scrape RSS feed {source.source_id}/{feed_id}.",
                )
                if previous_task:
                    previous_task >> task
                previous_task = task
