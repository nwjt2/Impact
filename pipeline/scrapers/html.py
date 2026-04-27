"""HTML list-page scraper.

Uses httpx + selectolax. Reads CSS selectors from each source's
`html_selectors` dict. Subclasses BaseScraper so break_token + fixture-swap
+ incident-writing behavior all work via the existing harness.

Fallbacks:
  - Relative URLs are resolved against the source's base URL.
  - Missing date → current UTC + a "date_missing" marker so downstream
    recency scoring still works but the normalized item is flagged.
  - No list items found → returns []; upstream run.py treats 0 items +
    HTTP-OK as schema_drift (filed as incident).
  - JS-rendered content: selectolax does not execute JS; these sources
    will parse as "0 items" and degrade gracefully via the silent-zero
    path. Not our job to run headless chrome in v1.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin

from dateutil import parser as dateparser
from selectolax.parser import HTMLParser, Node

from .base import BaseScraper, FetchResult


def _first_match(node: Node, selector_group: str) -> Optional[Node]:
    """Return the first node matching any selector in a comma-separated group."""
    if not selector_group:
        return None
    for sel in [s.strip() for s in selector_group.split(",") if s.strip()]:
        try:
            match = node.css_first(sel)
        except Exception:
            continue
        if match is not None:
            return match
    return None


def _text(node: Optional[Node]) -> str:
    if node is None:
        return ""
    return (node.text() or "").strip()


def _attr(node: Optional[Node], *attrs: str) -> str:
    if node is None:
        return ""
    for a in attrs:
        val = node.attributes.get(a) if node.attributes else None
        if val:
            return val.strip()
    return ""


def _parse_date(text: str) -> Optional[datetime]:
    """Lenient date parsing. Accepts ISO-8601, 'Apr 23 2026', 'April 23, 2026'."""
    text = (text or "").strip()
    if not text:
        return None
    try:
        dt = dateparser.parse(text, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError, OverflowError):
        return None


class HtmlScraper(BaseScraper):
    """List-page HTML scraper.

    Expects source dict to contain `html_selectors` with keys:
      list_item (required) — selector for each item row/card
      title    — selector relative to item for the headline text
      link     — selector relative to item for the <a> tag; href taken from
                 [href] or [data-href]. If blank, the item node itself is
                 treated as a link.
      date     — selector for the date; tries datetime= attr then text.
      body     — selector for summary/excerpt text (may be empty).
    """

    def parse(self, fr: FetchResult) -> list[dict]:
        if fr.http_status >= 400 or not fr.body:
            return []

        selectors = self.source.get("html_selectors") or {}
        list_sel = selectors.get("list_item") or ""
        title_sel = selectors.get("title") or ""
        link_sel = selectors.get("link") or ""
        date_sel = selectors.get("date") or ""
        body_sel = selectors.get("body") or ""

        if not list_sel:
            raise ValueError(f"{self.source_id}: html source missing list_item selector")

        try:
            tree = HTMLParser(fr.body.decode("utf-8", errors="replace"))
        except Exception as e:
            raise ValueError(f"{self.source_id}: HTML parse failed: {e!r}")

        base_url = self.url
        items: list[dict] = []

        for item_node in tree.css(list_sel):
            title_node = _first_match(item_node, title_sel) if title_sel else None
            link_node = _first_match(item_node, link_sel) if link_sel else item_node
            date_node = _first_match(item_node, date_sel) if date_sel else None
            body_node = _first_match(item_node, body_sel) if body_sel else None

            title = _text(title_node) or _text(link_node) or _text(item_node)[:200]
            href = _attr(link_node, "href", "data-href")
            if not href:
                # Try looking for any <a> inside the item as last resort.
                a = item_node.css_first("a[href]")
                if a is not None:
                    href = _attr(a, "href")

            # Date: prefer [datetime] attribute, fall back to visible text.
            date_text = _attr(date_node, "datetime") or _text(date_node)
            pub = _parse_date(date_text)

            body = _text(body_node) if body_node is not None else ""

            if not title or not href:
                continue  # skip items with no headline or link

            # Resolve relative URLs
            abs_url = urljoin(base_url, href.strip())

            items.append({
                "title": title,
                "url": abs_url,
                "summary": body,
                "published_at": pub,
                "date_missing": pub is None,
            })

        return items
