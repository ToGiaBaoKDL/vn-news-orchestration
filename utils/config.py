from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATION_CONFIG_DIR = REPO_ROOT / "configs"


@dataclass(frozen=True)
class RssIngestionConfig:
    owner: str
    schedule_interval_minutes: int
    timezone: str
    max_active_tasks: int
    retries: int
    retry_delay_minutes: int
    execution_timeout_minutes: int
    feed_ingestor_image_env: str
    docker_daemon_url_env: str
    host_config_dir_env: str
    host_secrets_dir_env: str
    ingestion_credentials_file: str
    container_config_dir: str
    container_secrets_dir: str
    forwarded_env: tuple[str, ...]


def dag_id_from_file(file_path: str) -> str:
    return Path(file_path).stem


def load_rss_ingestion_config() -> RssIngestionConfig:
    path = ORCHESTRATION_CONFIG_DIR / "rss_ingestion.yaml"
    config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return RssIngestionConfig(
        owner=config["owner"],
        schedule_interval_minutes=int(config["schedule_interval_minutes"]),
        timezone=config["timezone"],
        max_active_tasks=int(config["max_active_tasks"]),
        retries=int(config["retries"]),
        retry_delay_minutes=int(config["retry_delay_minutes"]),
        execution_timeout_minutes=int(config["execution_timeout_minutes"]),
        feed_ingestor_image_env=config["feed_ingestor_image_env"],
        docker_daemon_url_env=config["docker_daemon_url_env"],
        host_config_dir_env=config["host_config_dir_env"],
        host_secrets_dir_env=config["host_secrets_dir_env"],
        ingestion_credentials_file=config["ingestion_credentials_file"],
        container_config_dir=config["container_config_dir"],
        container_secrets_dir=config["container_secrets_dir"],
        forwarded_env=tuple(config["forwarded_env"]),
    )
