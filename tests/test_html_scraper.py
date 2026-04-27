"""HTML scraper: parse a known-shape fixture and confirm item extraction."""

from pathlib import Path

from pipeline.scrapers.html import HtmlScraper
from pipeline.scrapers.base import FetchResult


FIXTURE = """<!doctype html>
<html><body>
<main>
  <article class="news-card">
    <h3 class="news-card__title"><a class="news-card__link" href="/news/item-a">
      BII commits $50m to Africa Credit Fund II
    </a></h3>
    <time datetime="2026-04-18">18 April 2026</time>
    <p class="news-card__summary">BII commits $50m to TLG Capital's vehicle.</p>
  </article>
  <article class="news-card">
    <h3 class="news-card__title"><a class="news-card__link" href="https://example.com/absolute">
      Absolute URL example
    </a></h3>
    <time datetime="2026-04-10">10 April 2026</time>
    <p class="news-card__summary">Summary with absolute link.</p>
  </article>
  <article class="news-card">
    <!-- item with no link (intentionally dropped) -->
    <h3 class="news-card__title">Orphaned headline</h3>
    <time datetime="2026-04-01">1 April 2026</time>
  </article>
</main>
</body></html>
""".encode("utf-8")


def _source():
    return {
        "id": "html-test",
        "name": "HTML Test",
        "url": "https://www.bii.co.uk/en/news/",
        "type": "html",
        "content_type": "lp_commitment",
        "html_selectors": {
            "list_item": "article.news-card",
            "title": "h3.news-card__title",
            "link": "a.news-card__link",
            "date": "time[datetime]",
            "body": "p.news-card__summary",
        },
    }


def test_html_scraper_parses_list_items():
    scraper = HtmlScraper(_source())
    fr = FetchResult(body=FIXTURE, http_status=200, url="test", from_fixture=True)
    items = scraper.parse(fr)
    assert len(items) == 2  # orphan (no link) dropped

    first = items[0]
    assert "BII commits $50m" in first["title"]
    # Relative URL resolved against base
    assert first["url"] == "https://www.bii.co.uk/news/item-a"
    assert first["published_at"] is not None
    assert first["published_at"].year == 2026
    assert first["published_at"].month == 4
    assert first["published_at"].day == 18

    second = items[1]
    # Absolute URL preserved
    assert second["url"] == "https://example.com/absolute"


def test_html_scraper_handles_missing_date():
    src = _source()
    html = b"""<html><body>
      <article class="news-card">
        <h3 class="news-card__title"><a class="news-card__link" href="/x">Headline</a></h3>
        <p class="news-card__summary">body text</p>
      </article>
    </body></html>"""
    fr = FetchResult(body=html, http_status=200, url="test", from_fixture=True)
    items = HtmlScraper(src).parse(fr)
    assert len(items) == 1
    assert items[0]["published_at"] is None
    assert items[0]["date_missing"] is True


def test_html_scraper_empty_on_http_error():
    scraper = HtmlScraper(_source())
    fr = FetchResult(body=b"", http_status=404, url="test", from_fixture=True)
    assert scraper.parse(fr) == []


def test_html_scraper_empty_on_empty_body():
    scraper = HtmlScraper(_source())
    fr = FetchResult(body=b"", http_status=200, url="test", from_fixture=True)
    assert scraper.parse(fr) == []


def test_html_scraper_no_matches_returns_empty():
    """JS-rendered SPA case: selectors hit nothing — returns [] cleanly."""
    scraper = HtmlScraper(_source())
    fr = FetchResult(
        body=b"<html><body><div id=root></div></body></html>",
        http_status=200,
        url="test",
        from_fixture=True,
    )
    assert scraper.parse(fr) == []
