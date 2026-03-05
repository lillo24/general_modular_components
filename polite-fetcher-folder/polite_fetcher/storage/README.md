# storage map

## Files
- `__init__.py` (`STABLE`): package marker.
- `paths.py` (`STABLE`): deterministic data contract rooted at `polite_fetcher/data`.
- `ids.py` (`STABLE`): canonical URL, doc_id, sha256, and doc-scoped revision_id helpers.
- `raw_store.py` (`STABLE`): raw artifact paths, extension inference, atomic writes.
- `manifest.py` (`STABLE`): SQLite schema and CRUD helpers for documents/revisions.

## Why grouped
All persistence primitives are colocated so crawler pipeline can remain thin orchestration.