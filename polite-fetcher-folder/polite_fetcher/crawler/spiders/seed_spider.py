"""Seed-only spider that fetches configured URLs without discovery."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import scrapy
from scrapy import signals
from scrapy.http import Response

from polite_fetcher.config import load_config
from polite_fetcher.crawler.items import RawFetchItem
from polite_fetcher.storage.ids import canonicalize_url


class SeedSpider(scrapy.Spider):
    """Fetch each seed URL exactly once and emit raw response payloads."""

    name = "seed"

    def __init__(self, config_path: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.config = load_config(Path(config_path))
        self.status_counts: dict[int, int] = defaultdict(int)
        self.content_type_counts: dict[str, int] = defaultdict(int)
        self.total_responses = 0

    @classmethod
    def from_crawler(cls, crawler: scrapy.Crawler, *args: Any, **kwargs: Any) -> "SeedSpider":
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider._on_spider_closed, signal=signals.spider_closed)
        return spider

    async def start(self) -> Any:
        for request in self.start_requests():
            yield request

    def start_requests(self) -> Any:
        for url in self.config.seed_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={"handle_httpstatus_all": True, "seed_url": url},
            )

    def parse(self, response: Response, **kwargs: Any) -> Any:
        del kwargs

        requested_url = response.meta.get("seed_url", response.url)
        final_url = response.url
        content_type = _decode_header(response.headers.get(b"Content-Type"))
        content_length = _parse_int_header(response.headers.get(b"Content-Length"))
        etag = _decode_header(response.headers.get(b"ETag"))
        last_modified = _decode_header(response.headers.get(b"Last-Modified"))

        self.total_responses += 1
        self.status_counts[response.status] += 1
        self.content_type_counts[content_type or "<missing>"] += 1

        yield RawFetchItem(
            canonical_url=canonicalize_url(final_url),
            requested_url=requested_url,
            final_url=final_url,
            status_code=response.status,
            content_type=content_type,
            content_length=content_length,
            etag=etag,
            last_modified=last_modified,
            fetched_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            body=response.body,
        )

    def _on_spider_closed(self, spider: scrapy.Spider, reason: str) -> None:
        if spider != self:
            return

        self.logger.info(
            "Seed crawl summary | reason=%s | total_responses=%d | status_counts=%s | content_type_counts=%s",
            reason,
            self.total_responses,
            dict(sorted(self.status_counts.items())),
            dict(sorted(self.content_type_counts.items())),
        )


def _decode_header(raw_value: bytes | None) -> str | None:
    if raw_value is None:
        return None
    return raw_value.decode("latin-1").strip() or None


def _parse_int_header(raw_value: bytes | None) -> int | None:
    decoded = _decode_header(raw_value)
    if decoded is None:
        return None
    try:
        return int(decoded)
    except ValueError:
        return None

