"""Normalize raw scraper entries into BriefItem objects."""

from __future__ import annotations

import hashlib
import re
from datetime import timezone
from typing import Optional

from .schemas import BriefItem

_TAG_RE = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


def _strip_html(s: str) -> str:
    s = _TAG_RE.sub(" ", s or "")
    s = _WS.sub(" ", s).strip()
    return s[:400]


def _stable_id(url: str, source_id: str) -> str:
    h = hashlib.sha1(f"{source_id}::{url}".encode()).hexdigest()
    return h[:16]


def to_brief_item(
    raw: dict,
    source_id: str,
    source_name: str,
    content_type: Optional[str] = None,
) -> BriefItem | None:
    """Raw dict -> BriefItem. Drops items with no title or url."""
    title = (raw.get("title") or "").strip()
    url = (raw.get("url") or "").strip()
    if not title or not url:
        return None

    pub = raw.get("published_at")
    if pub and pub.tzinfo is None:
        pub = pub.replace(tzinfo=timezone.utc)

    return BriefItem(
        id=_stable_id(url, source_id),
        source_id=source_id,
        source_name=source_name,
        title=title,
        url=url,
        published_at=pub,
        summary=_strip_html(raw.get("summary", "")),
        content_type=content_type or "lp_commitment",
    )
