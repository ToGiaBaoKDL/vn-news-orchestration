from __future__ import annotations

import os


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def forwarded_environment(names: tuple[str, ...]) -> dict[str, str]:
    return {name: required_env(name) for name in names}
