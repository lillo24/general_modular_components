# spiders map

## Files
- `__init__.py` (`STABLE`): package marker.
- `seed_spider.py` (`STABLE`): seed-only spider emitting `RawFetchItem` for every response.

## Why grouped
Keeps crawl-request/response behavior encapsulated from pipeline persistence code.