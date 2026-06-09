from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

PLATFORM_CONFIG_DIR = Path(os.environ.get("VN_NEWS_CONFIG_DIR", "/opt/vn-news/configs"))


@dataclass(frozen=True)
class RssSourceFeeds:
    source_id: str
    feed_ids: tuple[str, ...]


def load_enabled_rss_sources() -> tuple[RssSourceFeeds, ...]:
    sources = []
    for path in sorted((PLATFORM_CONFIG_DIR / "sources").glob("*.yaml")):
        source = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if source.get("enabled"):
            feed_ids = tuple(feed["feed_id"] for feed in source["feed_discovery"]["feeds"])
            if not feed_ids:
                raise ValueError(f"Enabled RSS source has no feeds: {source['source_id']}")
            if len(feed_ids) != len(set(feed_ids)):
                raise ValueError(
                    f"Enabled RSS source has duplicate feed IDs: {source['source_id']}"
                )
            sources.append(RssSourceFeeds(source_id=source["source_id"], feed_ids=feed_ids))
    if not sources:
        raise ValueError("No enabled RSS sources configured")
    return tuple(sources)
