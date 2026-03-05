# polite-fetcher

Reusable seed-only ingestion component that performs polite HTTP fetches and persists:
- immutable raw response bytes under a deterministic raw-store path
- a SQLite manifest with document/revision metadata
- incremental refresh skip logic (unchanged content is not duplicated)
- `requested_url` tracking alongside `final_url`

## Scope
This component includes only:
- Scrapy seed fetcher
- crawler item/pipeline wiring
- raw storage helpers
- SQLite manifest helpers

This component does **not** include:
- extraction (`trafilatura`, `PyMuPDF`)
- chunking
- indexing
- RAG/UI features
- full discovery crawling

## Smoke run
```powershell
poetry install
poetry run python -m polite_fetcher.crawler.run_seed_crawl polite_fetcher/configs/unitn_seed_smoke.toml
```

## Output locations
- `polite_fetcher/data/raw/...`
- `polite_fetcher/data/manifest.sqlite`
- `polite_fetcher/data/logs/...`

## Tests and lint
```powershell
poetry run pytest
poetry run ruff check .
```