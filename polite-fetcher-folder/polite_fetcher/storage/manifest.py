"""SQLite manifest schema and data access helpers."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from polite_fetcher.storage.ids import canonicalize_url, doc_id_for_url

_VALID_DOC_TYPES = {"html", "pdf"}


def connect(manifest_path: Path) -> sqlite3.Connection:
    """Create a SQLite connection with pragmas suitable for local ingestion metadata."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(manifest_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create manifest tables and indexes if they do not exist."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id TEXT PRIMARY KEY,
            canonical_url TEXT UNIQUE NOT NULL,
            doc_type TEXT NOT NULL,
            scope_tags TEXT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS revisions (
            revision_id TEXT PRIMARY KEY,
            doc_id TEXT NOT NULL REFERENCES documents(doc_id),
            fetched_at TEXT NOT NULL,
            requested_url TEXT NULL,
            final_url TEXT NULL,
            status_code INTEGER NULL,
            content_type TEXT NULL,
            content_length INTEGER NULL,
            etag TEXT NULL,
            last_modified TEXT NULL,
            sha256 TEXT NOT NULL,
            raw_path TEXT NOT NULL,
            is_current INTEGER NOT NULL DEFAULT 0 CHECK (is_current IN (0, 1))
        );

        CREATE INDEX IF NOT EXISTS idx_revisions_doc_id ON revisions(doc_id);
        CREATE INDEX IF NOT EXISTS idx_revisions_is_current ON revisions(is_current);
        CREATE INDEX IF NOT EXISTS idx_documents_canonical_url ON documents(canonical_url);
        """
    )
    _ensure_revisions_requested_url_column(conn)
    conn.commit()


def upsert_document(
    conn: sqlite3.Connection,
    canonical_url: str,
    doc_type: str,
    scope_tags: str | None = None,
) -> str:
    """Insert (or reuse) a document row and return stable doc_id."""
    normalized_url = canonicalize_url(canonical_url)
    _validate_doc_type(doc_type)

    doc_id = doc_id_for_url(normalized_url)
    conn.execute(
        """
        INSERT INTO documents (doc_id, canonical_url, doc_type, scope_tags, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(canonical_url) DO NOTHING
        """,
        (doc_id, normalized_url, doc_type, scope_tags, _utc_now_iso()),
    )
    conn.commit()

    row = conn.execute(
        "SELECT doc_id FROM documents WHERE canonical_url = ?",
        (normalized_url,),
    ).fetchone()
    if row is None:
        raise RuntimeError("Failed to read back document row after upsert.")
    return row["doc_id"] if isinstance(row, sqlite3.Row) else row[0]


def insert_revision(conn: sqlite3.Connection, revision: dict[str, Any]) -> str:
    """Insert a revision row and return revision_id.

    `revision_id` is required and must be doc-scoped to avoid cross-document collisions.
    """
    doc_id = _required_str(revision, "doc_id")
    sha256 = _required_str(revision, "sha256")
    raw_path = _required_str(revision, "raw_path")
    revision_id_raw = revision.get("revision_id")
    if not isinstance(revision_id_raw, str) or revision_id_raw == "":
        raise ValueError(
            "insert_revision requires revision_id (doc-scoped) to avoid cross-doc collisions"
        )
    revision_id = revision_id_raw
    fetched_at = str(revision.get("fetched_at") or _utc_now_iso())
    is_current = 1 if bool(revision.get("is_current", 0)) else 0

    conn.execute(
        """
        INSERT OR IGNORE INTO revisions (
            revision_id,
            doc_id,
            fetched_at,
            requested_url,
            final_url,
            status_code,
            content_type,
            content_length,
            etag,
            last_modified,
            sha256,
            raw_path,
            is_current
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            revision_id,
            doc_id,
            fetched_at,
            revision.get("requested_url"),
            revision.get("final_url"),
            revision.get("status_code"),
            revision.get("content_type"),
            revision.get("content_length"),
            revision.get("etag"),
            revision.get("last_modified"),
            sha256,
            raw_path,
            is_current,
        ),
    )
    conn.commit()
    return revision_id


def mark_current_revision(conn: sqlite3.Connection, doc_id: str, revision_id: str) -> None:
    """Mark one revision as current and clear any previous current revision."""
    with conn:
        conn.execute("UPDATE revisions SET is_current = 0 WHERE doc_id = ?", (doc_id,))
        result = conn.execute(
            """
            UPDATE revisions
            SET is_current = 1
            WHERE doc_id = ? AND revision_id = ?
            """,
            (doc_id, revision_id),
        )
        if result.rowcount != 1:
            raise ValueError(f"Revision '{revision_id}' does not exist for doc_id '{doc_id}'.")


def get_current_revision(conn: sqlite3.Connection, doc_id: str) -> dict[str, Any] | None:
    """Return the current revision row for a document, if available."""
    cursor = conn.execute(
        """
        SELECT *
        FROM revisions
        WHERE doc_id = ? AND is_current = 1
        ORDER BY fetched_at DESC
        LIMIT 1
        """,
        (doc_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return dict(row)
    columns = [column[0] for column in cursor.description or []]
    return dict(zip(columns, row))


def _validate_doc_type(doc_type: str) -> None:
    if doc_type not in _VALID_DOC_TYPES:
        allowed = ", ".join(sorted(_VALID_DOC_TYPES))
        raise ValueError(f"Invalid doc_type '{doc_type}'. Expected one of: {allowed}.")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or value == "":
        raise ValueError(f"Missing required string field: {key}")
    return value


def _ensure_revisions_requested_url_column(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"] if isinstance(row, sqlite3.Row) else row[1]
        for row in conn.execute("PRAGMA table_info(revisions)").fetchall()
    }
    if "requested_url" not in columns:
        conn.execute("ALTER TABLE revisions ADD COLUMN requested_url TEXT NULL;")

