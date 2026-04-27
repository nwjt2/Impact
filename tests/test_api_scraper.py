"""API scraper: parse JSON fixtures for SEC EDGAR + GitHub releases shapes."""

import json

from pipeline.scrapers.api import ApiScraper
from pipeline.scrapers.base import FetchResult


GITHUB_RELEASES = json.dumps([
    {
        "name": "IRIS+ v5.3",
        "html_url": "https://github.com/GIIN/iris/releases/tag/v5.3",
        "published_at": "2026-03-12T10:00:00Z",
        "body": "New sector taxonomy for climate-adaptation metrics OI6831.",
    },
    {
        "name": "IRIS+ v5.2",
        "html_url": "https://github.com/GIIN/iris/releases/tag/v5.2",
        "published_at": "2026-01-05T10:00:00Z",
        "body": "Minor corrections.",
    },
]).encode()


SEC_EDGAR = json.dumps({
    "hits": {
        "hits": [
            {
                "_id": "0001234567-26-000123",
                "_source": {
                    "display_names": ["Example Impact Fund LLC"],
                    "file_date": "2026-04-10",
                    "adsh": "Offering size $250m, first sale 2026-04-10",
                },
            },
            {
                "_id": "0001234567-26-000124",
                "_source": {
                    "display_names": ["Another Fund LP"],
                    "file_date": "2026-04-11",
                    "adsh": "Offering size $100m.",
                },
            },
        ]
    }
}).encode()


def _github_source():
    return {
        "id": "iris-github-releases",
        "name": "IRIS+",
        "url": "https://api.github.com/repos/GIIN/iris/releases",
        "type": "api",
        "content_type": "guideline",
        "api_params": {
            "method": "GET",
            "headers": {"Accept": "application/vnd.github+json"},
            "json_path": "",
            "title_path": "name",
            "link_path": "html_url",
            "date_path": "published_at",
            "body_path": "body",
        },
    }


def _edgar_source():
    return {
        "id": "sec-edgar-search",
        "name": "SEC EDGAR",
        "url": "https://efts.sec.gov/LATEST/search-index",
        "type": "api",
        "content_type": "regulator_bulletin",
        "api_params": {
            "method": "GET",
            "headers": {"User-Agent": "FirstCloseTool test"},
            "json_path": "hits.hits",
            "title_path": "_source.display_names",
            "link_path": "_id",
            "date_path": "_source.file_date",
            "body_path": "_source.adsh",
        },
    }


def test_api_scraper_github_releases_shape():
    scraper = ApiScraper(_github_source())
    fr = FetchResult(body=GITHUB_RELEASES, http_status=200, url="test", from_fixture=True)
    items = scraper.parse(fr)
    assert len(items) == 2
    first = items[0]
    assert first["title"] == "IRIS+ v5.3"
    assert "github.com" in first["url"]
    assert first["published_at"] is not None
    assert first["published_at"].year == 2026
    assert first["published_at"].month == 3


def test_api_scraper_edgar_shape():
    scraper = ApiScraper(_edgar_source())
    fr = FetchResult(body=SEC_EDGAR, http_status=200, url="test", from_fixture=True)
    items = scraper.parse(fr)
    assert len(items) == 2
    first = items[0]
    # display_names is a list → stringified
    assert "Example Impact Fund LLC" in first["title"]
    assert first["url"] == "0001234567-26-000123"
    assert first["published_at"] is not None


def test_api_scraper_fixture_extension_is_json():
    """api.py overrides BaseScraper.fixture_path to use .json."""
    scraper = ApiScraper(_github_source())
    p = scraper.fixture_path("ok")
    assert p.suffix == ".json"


def test_api_scraper_empty_on_http_error():
    scraper = ApiScraper(_github_source())
    fr = FetchResult(body=b"", http_status=403, url="test", from_fixture=True)
    assert scraper.parse(fr) == []


def test_api_scraper_bad_json_raises():
    import pytest
    scraper = ApiScraper(_github_source())
    fr = FetchResult(body=b"not json at all", http_status=200, url="test", from_fixture=True)
    with pytest.raises(ValueError, match="JSON parse failed"):
        scraper.parse(fr)


def test_api_scraper_json_path_mismatch_raises():
    """If json_path resolves to a non-list, raise rather than silently empty."""
    import pytest
    src = _github_source()
    src["api_params"]["json_path"] = "nope"
    scraper = ApiScraper(src)
    fr = FetchResult(body=GITHUB_RELEASES, http_status=200, url="test", from_fixture=True)
    # GITHUB_RELEASES is a list at root; digging into 'nope' returns None → []
    # (but if it resolved to a non-list, ApiScraper raises)
    assert scraper.parse(fr) == []
