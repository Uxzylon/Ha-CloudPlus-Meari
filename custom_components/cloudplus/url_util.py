"""Small URL helpers shared across the integration."""

from __future__ import annotations

from urllib.parse import urlparse


def parse_host(value: str | None) -> str:
    """Return the lowercased hostname from a URL or bare host string."""
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if "://" in raw:
        try:
            return (urlparse(raw).hostname or "").strip().lower()
        except ValueError:
            return ""
    return raw
