from __future__ import annotations

import os
from pathlib import Path

import yaml

PLATFORM_CONFIG_DIR = Path(os.environ.get("VN_NEWS_CONFIG_DIR", "/opt/vn-news/configs"))


def load_enabled_rss_source_ids() -> tuple[str, ...]:
    source_ids = []
    for path in sorted((PLATFORM_CONFIG_DIR / "sources").glob("*.yaml")):
        source = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if source.get("enabled"):
            source_ids.append(source["source_id"])
    if not source_ids:
        raise ValueError("No enabled RSS sources configured")
    return tuple(source_ids)
