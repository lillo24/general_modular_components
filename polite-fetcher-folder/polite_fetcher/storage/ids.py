"""Deterministic identifier helpers for documents and revisions."""

from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "_hsenc",
    "_hsmi",
}


def canonicalize_url(url: str) -> str:
    """Apply conservative URL normalization for stable document identity."""
    stripped = url.strip()
    parsed = urlsplit(stripped)

    # Keep relative or malformed inputs unchanged (except surrounding spaces)
    # to avoid silently changing semantics.
    if not parsed.scheme and not parsed.netloc:
        return stripped

    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()

    port = parsed.port
    use_default_port = (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    host_part = hostname if port is None or use_default_port else f"{hostname}:{port}"

    userinfo = ""
    if parsed.username:
        userinfo = parsed.username
        if parsed.password:
            userinfo = f"{userinfo}:{parsed.password}"
        userinfo = f"{userinfo}@"

    netloc = f"{userinfo}{host_part}"

    # Conservative trailing slash rule: normalize empty path to '/'
    # while preserving non-root paths as-is.
    path = parsed.path or "/"

    filtered_query_pairs: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered.startswith("utm_") or lowered in _TRACKING_PARAMS:
            continue
        filtered_query_pairs.append((key, value))
    query = urlencode(filtered_query_pairs, doseq=True)

    return urlunsplit((scheme, netloc, path, query, ""))


def doc_id_for_url(canonical_url: str) -> str:
    """Return a stable document identifier from canonical URL."""
    return hashlib.sha1(canonical_url.encode("utf-8")).hexdigest()


def content_sha256(content: bytes) -> str:
    """Return content hash used for change detection."""
    return hashlib.sha256(content).hexdigest()


def revision_id_for_bytes(content: bytes, doc_id: str) -> str:
    """Return a doc-scoped stable revision identifier from content bytes."""
    if not doc_id:
        raise ValueError("doc_id is required to compute a doc-scoped revision_id.")
    payload = f"{doc_id}:".encode("utf-8") + content
    return hashlib.sha256(payload).hexdigest()
