"""Scrapy settings builder for seed-only crawl runs."""

from __future__ import annotations

from polite_fetcher.config import IngestionConfig


def scrapy_settings_from_config(cfg: IngestionConfig) -> dict[str, object]:
    """Return conservative Scrapy settings for polite seed fetching."""
    download_delay = cfg.download_delay_seconds if cfg.download_delay_seconds > 0 else 1.0
    concurrent_requests = cfg.concurrent_requests if cfg.concurrent_requests > 0 else 4

    return {
        "ROBOTSTXT_OBEY": True,
        "USER_AGENT": cfg.user_agent,
        "DOWNLOAD_DELAY": download_delay,
        "CONCURRENT_REQUESTS": concurrent_requests,
        "CONCURRENT_REQUESTS_PER_DOMAIN": min(2, concurrent_requests),
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_TIMEOUT": 45,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504, 522, 524],
        "ITEM_PIPELINES": {
            "polite_fetcher.crawler.pipelines.RawStoreManifestPipeline": 100,
        },
        "LOG_LEVEL": "INFO",
        "DOWNLOAD_WARNSIZE": 10 * 1024 * 1024,
        "DOWNLOAD_MAXSIZE": 50 * 1024 * 1024,
    }

