"""RSS/Atom scraper using feedparser."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterator

import feedparser

from .base import BaseScraper, FetchResult


class RssScraper(BaseScraper):
    def parse(self, fr: FetchResult) -> list[dict]:
        """Parse feed body into raw dicts. Raises on malformed XML."""
        if fr.http_status >= 400 or not fr.body:
            # Caller treats empty as failure; distinguishing happens upstream.
            return []

        feed = feedparser.parse(fr.body)
        # feedparser tolerates a LOT; we treat bozo with no entries as parse error
        if feed.bozo and not feed.entries:
            raise ValueError(
                f"feedparser bozo (no entries recovered): {feed.bozo_exception!r}"
            )

        items: list[dict] = []
        for e in feed.entries:
            published = None
            for key in ("published_parsed", "updated_parsed"):
                if getattr(e, key, None):
                    try:
                        published = datetime(*getattr(e, key)[:6], tzinfo=timezone.utc)
                        break
                    except Exception:
                        pass
            items.append({
                "title": getattr(e, "title", "").strip(),
                "url": getattr(e, "link", "").strip(),
                "summary": getattr(e, "summary", "").strip(),
                "published_at": published,
            })
        return items
