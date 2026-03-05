import pytest

from polite_fetcher.storage import raw_store
from polite_fetcher.storage.ids import content_sha256, revision_id_for_bytes
from polite_fetcher.storage.manifest import (
    connect,
    ensure_schema,
    get_current_revision,
    insert_revision,
    mark_current_revision,
    upsert_document,
)


def test_manifest_schema_creates_tables(tmp_path) -> None:
    conn = connect(tmp_path / "manifest.sqlite")
    ensure_schema(conn)

    table_names = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }

    assert "documents" in table_names
    assert "revisions" in table_names
    conn.close()


def test_upsert_document_is_stable_for_same_url(tmp_path) -> None:
    conn = connect(tmp_path / "manifest.sqlite")
    ensure_schema(conn)

    first_id = upsert_document(
        conn,
        "HTTPS://EXAMPLE.com/path?a=1&utm_source=newsletter#intro",
        "html",
    )
    second_id = upsert_document(
        conn,
        "https://example.com/path?a=1&utm_medium=email",
        "html",
    )

    assert first_id == second_id
    row_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    assert row_count == 1
    conn.close()


def test_revision_insert_and_mark_current(tmp_path) -> None:
    conn = connect(tmp_path / "manifest.sqlite")
    ensure_schema(conn)

    doc_id = upsert_document(conn, "https://example.com/notes", "html")
    content_v1 = b"rev1"
    content_v2 = b"rev2"
    rev1 = revision_id_for_bytes(content_v1, doc_id)
    rev2 = revision_id_for_bytes(content_v2, doc_id)

    insert_revision(
        conn,
        {
            "revision_id": rev1,
            "doc_id": doc_id,
            "fetched_at": "2026-02-25T10:00:00+00:00",
            "requested_url": "https://example.com/source-a",
            "sha256": content_sha256(content_v1),
            "raw_path": f"data/raw/{doc_id}/{rev1}.html",
        },
    )
    insert_revision(
        conn,
        {
            "revision_id": rev2,
            "doc_id": doc_id,
            "fetched_at": "2026-02-25T11:00:00+00:00",
            "requested_url": "https://example.com/source-b",
            "sha256": content_sha256(content_v2),
            "raw_path": f"data/raw/{doc_id}/{rev2}.html",
        },
    )

    mark_current_revision(conn, doc_id, rev1)
    assert get_current_revision(conn, doc_id)["revision_id"] == rev1

    mark_current_revision(conn, doc_id, rev2)
    current = get_current_revision(conn, doc_id)
    assert current is not None
    assert current["revision_id"] == rev2
    assert current["requested_url"] == "https://example.com/source-b"

    current_count = conn.execute(
        "SELECT COUNT(*) FROM revisions WHERE doc_id = ? AND is_current = 1",
        (doc_id,),
    ).fetchone()[0]
    assert current_count == 1
    conn.close()


def test_revision_id_is_doc_scoped_and_avoids_cross_doc_collisions(tmp_path) -> None:
    conn = connect(tmp_path / "manifest.sqlite")
    ensure_schema(conn)

    doc_id_1 = upsert_document(conn, "https://example.com/doc-a", "html")
    doc_id_2 = upsert_document(conn, "https://example.com/doc-b", "html")
    assert doc_id_1 != doc_id_2

    content = b"identical-body"
    revision_id_1 = revision_id_for_bytes(content, doc_id_1)
    revision_id_2 = revision_id_for_bytes(content, doc_id_2)
    assert revision_id_1 != revision_id_2

    sha256_value = content_sha256(content)
    insert_revision(
        conn,
        {
            "revision_id": revision_id_1,
            "doc_id": doc_id_1,
            "fetched_at": "2026-02-25T12:00:00+00:00",
            "sha256": sha256_value,
            "raw_path": f"data/raw/{doc_id_1}/{revision_id_1}.html",
        },
    )
    insert_revision(
        conn,
        {
            "revision_id": revision_id_2,
            "doc_id": doc_id_2,
            "fetched_at": "2026-02-25T12:00:00+00:00",
            "sha256": sha256_value,
            "raw_path": f"data/raw/{doc_id_2}/{revision_id_2}.html",
        },
    )

    total_revisions = conn.execute("SELECT COUNT(*) FROM revisions").fetchone()[0]
    assert total_revisions == 2
    conn.close()


def test_insert_revision_requires_revision_id(tmp_path) -> None:
    conn = connect(tmp_path / "manifest.sqlite")
    ensure_schema(conn)
    doc_id = upsert_document(conn, "https://example.com/missing-revision-id", "html")

    with pytest.raises(
        ValueError,
        match="insert_revision requires revision_id \\(doc-scoped\\) to avoid cross-doc collisions",
    ):
        insert_revision(
            conn,
            {
                "doc_id": doc_id,
                "fetched_at": "2026-02-25T12:00:00+00:00",
                "sha256": content_sha256(b"body"),
                "raw_path": f"data/raw/{doc_id}/missing.html",
            },
        )

    conn.close()


def test_ensure_schema_migrates_requested_url_column(tmp_path) -> None:
    conn = connect(tmp_path / "manifest.sqlite")
    conn.executescript(
        """
        CREATE TABLE documents (
            doc_id TEXT PRIMARY KEY,
            canonical_url TEXT UNIQUE NOT NULL,
            doc_type TEXT NOT NULL,
            scope_tags TEXT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE revisions (
            revision_id TEXT PRIMARY KEY,
            doc_id TEXT NOT NULL REFERENCES documents(doc_id),
            fetched_at TEXT NOT NULL,
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
        """
    )
    conn.commit()

    ensure_schema(conn)

    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(revisions)").fetchall()
    }
    assert "requested_url" in columns
    conn.close()


def test_raw_store_atomic_write_and_path(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(raw_store, "get_raw_dir", lambda: tmp_path / "raw")

    path = raw_store.raw_path_for("doc-123", "rev-999", ".html")
    assert path == tmp_path / "raw" / "doc-123" / "rev-999.html"

    raw_store.write_raw_bytes(path, b"first-version")
    raw_store.write_raw_bytes(path, b"second-version")

    assert path.read_bytes() == b"second-version"
    assert list(path.parent.glob("*.tmp")) == []

