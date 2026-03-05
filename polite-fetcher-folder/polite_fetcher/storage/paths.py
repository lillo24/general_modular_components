"""Filesystem path helpers for ingestion outputs."""

from pathlib import Path

_PACKAGE_DIR = Path(__file__).resolve().parents[1]
_DATA_DIR = _PACKAGE_DIR / "data"


def get_data_dir() -> Path:
    """Return the root data directory for ingestion artifacts."""
    return _DATA_DIR


def get_raw_dir() -> Path:
    """Return the directory where immutable raw bytes are stored."""
    return get_data_dir() / "raw"


def get_extracted_dir() -> Path:
    """Return the directory where extracted artifacts are stored."""
    return get_data_dir() / "extracted"


def get_logs_dir() -> Path:
    """Return the directory where ingestion logs are written."""
    return get_data_dir() / "logs"


def get_manifest_path() -> Path:
    """Return the manifest SQLite file path."""
    return get_data_dir() / "manifest.sqlite"


def ensure_data_dirs() -> None:
    """Create the ingestion runtime data contract (directories + manifest file)."""
    for path in (get_data_dir(), get_raw_dir(), get_extracted_dir(), get_logs_dir()):
        path.mkdir(parents=True, exist_ok=True)
    get_manifest_path().touch(exist_ok=True)
