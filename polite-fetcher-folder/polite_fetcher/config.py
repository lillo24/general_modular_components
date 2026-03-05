"""Ingestion runtime configuration.

Configuration format: TOML.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(slots=True)
class IngestionConfig:
    """Core ingestion scope and politeness settings."""

    seed_urls: list[str]
    allowed_domains: list[str]
    allowed_path_prefixes: list[str] | None = None
    excluded_path_prefixes: list[str] | None = None
    user_agent: str = "thesis-disi-chatbot/0.1"
    max_depth: int | None = None
    download_delay_seconds: float = 0.5
    concurrent_requests: int = 8


def load_config(path: Path) -> IngestionConfig:
    """Load ingestion config from TOML.

    Required keys:
    - seed_urls: list[str]
    - allowed_domains: list[str]
    """
    with path.open("rb") as fh:
        payload = tomllib.load(fh)

    return IngestionConfig(
        seed_urls=_required_str_list(payload, "seed_urls"),
        allowed_domains=_required_str_list(payload, "allowed_domains"),
        allowed_path_prefixes=_optional_str_list(payload, "allowed_path_prefixes"),
        excluded_path_prefixes=_optional_str_list(payload, "excluded_path_prefixes"),
        user_agent=_optional_str(payload, "user_agent", "thesis-disi-chatbot/0.1"),
        max_depth=_optional_int(payload, "max_depth"),
        download_delay_seconds=_optional_float(payload, "download_delay_seconds", 0.5),
        concurrent_requests=_optional_int(payload, "concurrent_requests", 8),
    )


def _required_str_list(payload: dict[str, object], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"Expected '{key}' to be a list of strings.")
    return list(value)


def _optional_str_list(payload: dict[str, object], key: str) -> list[str] | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"Expected '{key}' to be a list of strings when provided.")
    return list(value)


def _optional_str(payload: dict[str, object], key: str, default: str) -> str:
    value = payload.get(key, default)
    if not isinstance(value, str):
        raise ValueError(f"Expected '{key}' to be a string.")
    return value


def _optional_int(payload: dict[str, object], key: str, default: int | None = None) -> int | None:
    value = payload.get(key, default)
    if value is None:
        return None
    if not isinstance(value, int):
        raise ValueError(f"Expected '{key}' to be an integer.")
    return value


def _optional_float(payload: dict[str, object], key: str, default: float) -> float:
    value = payload.get(key, default)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise ValueError(f"Expected '{key}' to be a float.")
    return value
