"""Scrapy item pipelines for persistence into raw storage + manifest."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from urllib.parse import urlsplit

from scrapy.crawler import Crawler

from polite_fetcher.crawler.items import RawFetchItem
from polite_fetcher.storage.ids import (
    canonicalize_url,
    content_sha256,
    doc_id_for_url,
    revision_id_for_bytes,
)
from polite_fetcher.storage.manifest import (
    connect,
    ensure_schema,
    get_current_revision,
    insert_revision,
    mark_current_revision,
    upsert_document,
)
from polite_fetcher.storage.paths import ensure_data_dirs, get_logs_dir, get_manifest_path
from polite_fetcher.storage.raw_store import guess_extension, raw_path_for, write_raw_bytes


def normalize_content_type(content_type: str | None) -> str | None:
    """Normalize HTTP content-type headers to MIME type only."""
    if content_type is None:
        return None
    return content_type.split(";", maxsplit=1)[0].strip().lower() or None


class RawStoreManifestPipeline:
    """Persist fetched seed responses to filesystem and SQLite manifest."""

    def __init__(self) -> None:
        self.crawler: Crawler | None = None
        self.logger = logging.getLogger(__name__)
        self.conn: sqlite3.Connection | None = None
        self.failure_log_path: Path | None = None
        self.stored = 0
        self.skipped_unchanged = 0
        self.failed = 0

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> "RawStoreManifestPipeline":
        pipeline = cls()
        pipeline.crawler = crawler
        return pipeline

    def open_spider(self, spider=None) -> None:
        ensure_data_dirs()
        self.conn = connect(get_manifest_path())
        ensure_schema(self.conn)
        self.failure_log_path = get_logs_dir() / "fetch_failures.jsonl"
        self.stored = 0
        self.skipped_unchanged = 0
        self.failed = 0

    def process_item(self, item: RawFetchItem, spider=None) -> RawFetchItem:
        if not isinstance(item, RawFetchItem):
            return item

        conn = self._require_conn()
        normalized_ct = normalize_content_type(item.content_type)

        if item.status_code != 200:
            self._append_failure_log(
                {
                    "fetched_at": item.fetched_at,
                    "requested_url": item.requested_url,
                    "final_url": item.final_url,
                    "status_code": item.status_code,
                    "content_type": normalized_ct,
                }
            )
            self.failed += 1
            return item

        canonical_url = canonicalize_url(item.final_url)
        doc_id = doc_id_for_url(canonical_url)
        doc_type = "pdf" if normalized_ct == "application/pdf" or _is_pdf_url(item.final_url) else "html"
        revision_id = revision_id_for_bytes(item.body, doc_id=doc_id)
        sha256_value = content_sha256(item.body)

        current_revision = get_current_revision(conn, doc_id)
        if current_revision is not None and current_revision.get("sha256") == sha256_value:
            self.skipped_unchanged += 1
            return item

        extension = guess_extension(normalized_ct, item.final_url)
        raw_path = raw_path_for(doc_id, revision_id, extension)
        write_raw_bytes(raw_path, item.body)

        persisted_doc_id = upsert_document(
            conn,
            canonical_url=canonical_url,
            doc_type=doc_type,
            scope_tags=None,
        )

        insert_revision(
            conn,
            {
                "revision_id": revision_id,
                "doc_id": persisted_doc_id,
                "fetched_at": item.fetched_at,
                "requested_url": item.requested_url,
                "final_url": item.final_url,
                "status_code": item.status_code,
                "content_type": normalized_ct,
                "content_length": item.content_length,
                "etag": item.etag,
                "last_modified": item.last_modified,
                "sha256": sha256_value,
                "raw_path": str(raw_path),
            },
        )
        mark_current_revision(conn, persisted_doc_id, revision_id)
        self.stored += 1
        return item

    def close_spider(self, spider=None) -> None:
        self._spider_logger().info(
            "Storage summary | stored=%d | skipped_unchanged=%d | failed=%d",
            self.stored,
            self.skipped_unchanged,
            self.failed,
        )
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _require_conn(self) -> sqlite3.Connection:
        if self.conn is None:
            raise RuntimeError("RawStoreManifestPipeline connection is not initialized.")
        return self.conn

    def _append_failure_log(self, payload: dict[str, object]) -> None:
        if self.failure_log_path is None:
            raise RuntimeError("RawStoreManifestPipeline failure log path is not initialized.")
        with self.failure_log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=True))
            fh.write("\n")

    def _spider_logger(self) -> logging.Logger:
        spider = self.crawler.spider if self.crawler is not None else None
        if spider is not None:
            return spider.logger
        return self.logger


def _is_pdf_url(url: str) -> bool:
    return Path(urlsplit(url).path).suffix.lower() == ".pdf"

