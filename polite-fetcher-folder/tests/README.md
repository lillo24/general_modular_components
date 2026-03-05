# tests map

## Files
- `test_storage_foundation.py` (`STABLE`): unit tests for IDs, manifest schema, and raw store writes.
- `test_crawler_pipeline.py` (`STABLE`): pipeline behavior tests (store once, skip unchanged, failure logging).

## Why grouped
These tests validate the exported component contract end-to-end at storage/pipeline boundaries.