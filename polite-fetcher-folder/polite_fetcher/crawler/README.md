# crawler map

## Files
- `__init__.py` (`STABLE`): package marker.
- `items.py` (`STABLE`): item schema passed from spider to pipeline.
- `settings.py` (`STABLE`): Scrapy settings factory from runtime config.
- `pipelines.py` (`STABLE`): persistence pipeline (raw store + manifest, skip unchanged).
- `run_seed_crawl.py` (`STABLE`): CLI/programmatic entrypoint for seed crawl runs.

## Subfolders
- `spiders/`: seed spider implementation.

## Why grouped
Crawler runtime concerns stay isolated from storage implementation internals.