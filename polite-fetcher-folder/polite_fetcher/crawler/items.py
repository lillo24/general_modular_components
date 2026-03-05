"""Crawler item contracts used before storage wiring."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RawFetchItem:
    """In-memory representation of a single fetched response."""

    canonical_url: str
    requested_url: str
    final_url: str
    status_code: int
    content_type: str | None
    content_length: int | None
    etag: str | None
    last_modified: str | None
    fetched_at: str
    body: bytes
