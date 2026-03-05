"""Programmatic seed crawl runner used during collector V0 setup."""

from __future__ import annotations

from pathlib import Path
import sys

from scrapy.crawler import CrawlerProcess

from polite_fetcher.config import load_config
from polite_fetcher.crawler.settings import scrapy_settings_from_config
from polite_fetcher.crawler.spiders.seed_spider import SeedSpider
from polite_fetcher.storage.paths import ensure_data_dirs


def run_seed_crawl(config_path: str) -> int:
    """Run a seed-only crawl from a TOML config path."""
    ensure_data_dirs()
    cfg = load_config(Path(config_path))
    process = CrawlerProcess(settings=scrapy_settings_from_config(cfg))
    process.crawl(SeedSpider, config_path=config_path)
    process.start()
    return 0


def _main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("Usage: python -m polite_fetcher.crawler.run_seed_crawl <config-path>")
        return 2
    return run_seed_crawl(argv[1])


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))

