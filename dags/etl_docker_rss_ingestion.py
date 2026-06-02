from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
import yaml
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DAG_ID = os.path.basename(__file__).replace(".py", "")
DAG_OWNER = "data-engineering"
CONFIG_DIR = Path(os.environ.get("VN_NEWS_CONFIG_DIR", "/opt/vn-news/configs"))
CONTAINER_ENV_NAMES = (
    "VN_NEWS_STORAGE_ENDPOINT_URL",
    "VN_NEWS_REDPANDA_BOOTSTRAP_SERVERS",
    "VN_NEWS_SCHEMA_REGISTRY_URL",
)
PRIVATE_CONTAINER_ENV_NAMES = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")


def load_enabled_rss_source_ids() -> tuple[str, ...]:
    source_ids = []
    for path in sorted((CONFIG_DIR / "sources").glob("*.yaml")):
        source = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if source.get("enabled"):
            source_ids.append(source["source_id"])
    if not source_ids:
        raise ValueError("No enabled RSS sources configured")
    return tuple(source_ids)


def forwarded_environment(names: tuple[str, ...]) -> dict[str, str]:
    return {name: os.environ[name] for name in names}


with DAG(
    dag_id=DAG_ID,
    default_args={"owner": DAG_OWNER},
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "docker", "rss"],
) as dag:
    for source_id in load_enabled_rss_source_ids():
        DockerOperator(
            task_id=f"ingest_{source_id}",
            image=os.environ["VN_NEWS_FEED_INGESTOR_IMAGE"],
            command=["--source-id", source_id, "--all-feeds"],
            docker_url=os.environ.get("VN_NEWS_DOCKER_URL", "tcp://docker-socket-proxy:2375"),
            mounts=[
                Mount(
                    source=os.environ["VN_NEWS_CONFIG_HOST_DIR"],
                    target="/app/configs",
                    type="bind",
                    read_only=True,
                )
            ],
            environment={
                "VN_NEWS_CONFIG_DIR": "/app/configs",
                **forwarded_environment(CONTAINER_ENV_NAMES),
            },
            private_environment=forwarded_environment(PRIVATE_CONTAINER_ENV_NAMES),
            mount_tmp_dir=False,
            auto_remove="success",
            force_pull=False,
            retries=2,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=15),
            doc=f"Scrape all configured RSS feeds for {source_id}.",
        )
