# polite_fetcher package map

## Files
- `__init__.py` (`STABLE`): package marker for importable module layout.
- `config.py` (`STABLE`): TOML loader and validated ingestion config dataclass.

## Subfolders
- `crawler/`: seed-only Scrapy runner/spider/pipeline wiring.
- `storage/`: deterministic IDs, raw bytes persistence, SQLite manifest helpers.
- `configs/`: runnable example TOML configurations.

## Why grouped
These files define the reusable fetch/storage component boundary with minimal external contracts.