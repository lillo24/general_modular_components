import json
from pathlib import Path

from polite_fetcher.crawler.items import RawFetchItem
from polite_fetcher.crawler.pipelines import RawStoreManifestPipeline, normalize_content_type
from polite_fetcher.storage import paths
from polite_fetcher.storage.manifest import connect


def test_normalize_content_type_strips_parameters() -> None:
    assert normalize_content_type("text/html; charset=UTF-8") == "text/html"
    assert normalize_content_type("application/pdf; qs=0.001") == "application/pdf"
    assert normalize_content_type(None) is None


def test_pipeline_stores_once_and_skips_unchanged(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "_DATA_DIR", tmp_path / "data")

    pipeline = RawStoreManifestPipeline()
    pipeline.open_spider(None)

    item = RawFetchItem(
        canonical_url="https://example.com/a",
        requested_url="https://example.com/start",
        final_url="https://example.com/a?utm_source=campaign",
        status_code=200,
        content_type="text/html; charset=UTF-8",
        content_length=17,
        etag="etag-v1",
        last_modified=None,
        fetched_at="2026-03-05T12:00:00Z",
        body=b"<html>Hello</html>",
    )

    pipeline.process_item(item, None)
    pipeline.process_item(item, None)
    pipeline.close_spider(None)

    assert pipeline.stored == 1
    assert pipeline.skipped_unchanged == 1
    assert pipeline.failed == 0

    conn = connect(paths.get_manifest_path())
    row_count = conn.execute("SELECT COUNT(*) FROM revisions").fetchone()[0]
    assert row_count == 1

    row = conn.execute(
        "SELECT requested_url, final_url, content_type, raw_path FROM revisions LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["requested_url"] == "https://example.com/start"
    assert row["final_url"] == "https://example.com/a?utm_source=campaign"
    assert row["content_type"] == "text/html"
    assert Path(row["raw_path"]).is_file()
    conn.close()


def test_pipeline_logs_non_200_failures(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "_DATA_DIR", tmp_path / "data")

    pipeline = RawStoreManifestPipeline()
    pipeline.open_spider(None)

    item = RawFetchItem(
        canonical_url="https://example.com/not-found",
        requested_url="https://example.com/not-found",
        final_url="https://example.com/not-found",
        status_code=404,
        content_type="text/html; charset=UTF-8",
        content_length=0,
        etag=None,
        last_modified=None,
        fetched_at="2026-03-05T12:00:00Z",
        body=b"",
    )

    pipeline.process_item(item, None)
    pipeline.close_spider(None)

    assert pipeline.stored == 0
    assert pipeline.skipped_unchanged == 0
    assert pipeline.failed == 1

    conn = connect(paths.get_manifest_path())
    revisions_count = conn.execute("SELECT COUNT(*) FROM revisions").fetchone()[0]
    assert revisions_count == 0
    conn.close()

    failure_log = paths.get_logs_dir() / "fetch_failures.jsonl"
    lines = failure_log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["status_code"] == 404
    assert payload["requested_url"] == "https://example.com/not-found"
    assert payload["content_type"] == "text/html"

