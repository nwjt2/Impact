"""Generic JSON-API scraper.

Hits a REST endpoint, extracts a list of items via a dotted json_path,
then maps each item to (title, link, date, body) via per-item dotted paths.

Concrete configs ship in sources.yml for:
  - SEC EDGAR full-text search (`efts.sec.gov/LATEST/search-index`) —
    requires a non-empty User-Agent header per SEC guidelines; set via
    api_params.headers.
  - GitHub releases (`api.github.com/repos/GIIN/iris/releases`) — free,
    60 req/hr unauth; well within our <1 req/day/source budget.

Fixture swap: JSON fixtures live at tool/fixtures/<id>/snapshot-ok.json.
The base scraper's default extension is .xml; api.py overrides
`fixture_path` to use .json.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from dateutil import parser as dateparser

from .base import BaseScraper, FetchResult, FIXTURES


def _dig(obj: Any, path: str) -> Any:
    """Walk a dotted path into obj. Empty/None path returns obj."""
    if not path:
        return obj
    cur = obj
    for part in path.split("."):
        if cur is None:
            return None
        if isinstance(cur, list):
            # If path segment is an integer, index in; otherwise, stop.
            try:
                idx = int(part)
                cur = cur[idx] if 0 <= idx < len(cur) else None
            except ValueError:
                return None
        elif isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _stringify(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, (list, tuple)):
        return ", ".join(_stringify(v) for v in val)
    if isinstance(val, dict):
        return json.dumps(val, default=str)[:400]
    return str(val).strip()


def _parse_date(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        # epoch seconds
        try:
            return datetime.fromtimestamp(float(val), tz=timezone.utc)
        except (OSError, ValueError):
            return None
    s = _stringify(val)
    if not s:
        return None
    try:
        dt = dateparser.parse(s, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError, OverflowError):
        return None


class ApiScraper(BaseScraper):
    """JSON-API scraper driven by source['api_params']."""

    # Override fixture extension: api scrapers use .json snapshots.
    def fixture_path(self, case: str) -> Path:
        ext = "json"
        if case == "404":
            ext = "txt"
        return FIXTURES / self.source_id / f"snapshot-{case}.{ext}"

    def _fetch_live(self) -> FetchResult:
        """Live GET with api_params.headers + query."""
        import httpx

        params_cfg = (self.source.get("api_params") or {})
        headers = dict(params_cfg.get("headers") or {})
        contact = os.environ.get("INGO_CONTACT", "set-INGO_CONTACT-env-var@example.com")
        headers.setdefault("User-Agent", f"FirstCloseTool {contact}")
        query = params_cfg.get("query") or {}

        try:
            with httpx.Client(http2=True, timeout=20.0, follow_redirects=True) as c:
                r = c.get(self.url, headers=headers, params=query)
                return FetchResult(
                    body=r.content,
                    http_status=r.status_code,
                    url=str(r.url),
                    from_fixture=False,
                )
        except Exception as e:
            return FetchResult(
                body=str(e).encode(),
                http_status=599,
                url=self.url,
                from_fixture=False,
            )

    def parse(self, fr: FetchResult) -> list[dict]:
        if fr.http_status >= 400 or not fr.body:
            return []

        try:
            data = json.loads(fr.body.decode("utf-8", errors="replace"))
        except Exception as e:
            raise ValueError(f"{self.source_id}: JSON parse failed: {e!r}")

        cfg = self.source.get("api_params") or {}
        list_path = cfg.get("json_path") or ""
        title_path = cfg.get("title_path") or "title"
        link_path = cfg.get("link_path") or "url"
        date_path = cfg.get("date_path") or "published_at"
        body_path = cfg.get("body_path") or "body"

        rows = _dig(data, list_path)
        if rows is None:
            # GitHub-style endpoints return a JSON array at the root; _dig of
            # "" returns the object, so this is only None on mismatch.
            return []
        if not isinstance(rows, list):
            raise ValueError(
                f"{self.source_id}: json_path {list_path!r} did not resolve to a list"
            )

        items: list[dict] = []
        for row in rows:
            title = _stringify(_dig(row, title_path))
            link = _stringify(_dig(row, link_path))
            summary = _stringify(_dig(row, body_path))
            pub = _parse_date(_dig(row, date_path))

            if not title or not link:
                continue
            items.append({
                "title": title[:400],
                "url": link,
                "summary": summary[:800],
                "published_at": pub,
            })
        return items
