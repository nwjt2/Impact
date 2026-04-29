"""Investor slug canonicalization.

Loads alias map from network/catalogue/investor_aliases.csv. Used by combine
steps to rewrite deprecated investor slugs (e.g. LP-scraper variants like
``bio-belgian-investment-company-for-developing-countries``) into their
canonical form (``bio-invest``) so the catalogue and edge tables stay
deduplicated even when scrapers emit name variants.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from network.utils.csv_io import read_rows

_ALIASES_CSV = Path(__file__).resolve().parents[1] / "catalogue" / "investor_aliases.csv"


@lru_cache(maxsize=1)
def _alias_map() -> dict[str, str]:
    if not _ALIASES_CSV.exists():
        return {}
    return {
        r["Deprecated Slug"]: r["Canonical Slug"]
        for r in read_rows(_ALIASES_CSV)
        if r.get("Deprecated Slug") and r.get("Canonical Slug")
    }


def canonicalize_investor_slug(slug: str | None) -> str:
    if not slug:
        return slug or ""
    return _alias_map().get(slug, slug)


def is_deprecated_investor_slug(slug: str | None) -> bool:
    return bool(slug) and slug in _alias_map()
