"""Raw artifact path and atomic-write helpers."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

from polite_fetcher.storage.paths import get_raw_dir


def raw_path_for(doc_id: str, revision_id: str, ext: str) -> Path:
    """Return the canonical raw artifact path for one revision."""
    if not ext.startswith("."):
        raise ValueError("Extension must include a leading dot, e.g. '.html'.")
    return get_raw_dir() / doc_id / f"{revision_id}{ext}"


def write_raw_bytes(path: Path, content: bytes) -> None:
    """Write bytes atomically using a temp file in the destination directory."""
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "wb") as temp_file:
            temp_file.write(content)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def guess_extension(content_type: str | None, url: str | None = None) -> str:
    """Infer extension from content type first, then URL suffix, else fallback to '.bin'."""
    if content_type:
        normalized = content_type.split(";", maxsplit=1)[0].strip().lower()
        if normalized in {"text/html", "application/xhtml+xml"}:
            return ".html"
        if normalized == "application/pdf":
            return ".pdf"

    if url:
        suffix = Path(urlsplit(url).path).suffix.lower()
        if suffix in {".htm", ".html"}:
            return ".html"
        if suffix == ".pdf":
            return ".pdf"

    return ".bin"

