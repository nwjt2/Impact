"""fund_portfolio_scraper_MercyCorpsVentures.py

Scrapes Mercy Corps Ventures Fund 1 (Evergreen Fund) portfolio page.
Source: https://www.mercycorpsventures.com/fund-1-evergreen-fund

Notes for next operator (also see network/docs/discovery_learnings.md):
- Squarespace site. The portfolio is a logo grid built from `image-slide-anchor`
  anchors. The company name is NOT in the HTML — only the external website URL,
  a numeric image filename, and an empty aria-label.
- This scraper extracts (URL -> derived name + slug) for each anchor. Operator
  review is required to clean company names; that's the right pattern per
  advice-doc lesson 32.
- The page promises "61 portfolio companies" but only ~44 unique anchors
  appear in the static HTML. The rest may be exits/unlinked. We only emit
  what we can verify.
"""
from __future__ import annotations

import re
import sys
import urllib.parse
from datetime import date
from pathlib import Path

import httpx
from selectolax.parser import HTMLParser

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "fund_portfolio_scraper_MercyCorpsVentures"
SOURCE_URL = "https://www.mercycorpsventures.com/fund-1-evergreen-fund"
FUND_SLUG = "mercy-corps-ventures"
INGO_SLUG = "mercy-corps"

OUTPUT_HEADERS = [
    "Fund Slug",
    "INGO Slug",
    "Company Name",
    "Company Slug",
    "Company Website",
    "Round",
    "Round Date",
    "Lead",
    "Source URL",
    "Source Date",
    "Scraping Method Used",
]

# Hosts to ignore (CDN / framework chrome / social-only links that aren't portcos)
_IGNORE_HOST_FRAGMENTS = (
    "squarespace",
    "gstatic",
    "definitions.sqspcdn",
    "jsdelivr",
    "fonts.google",
    "office.com/Pages",  # operator form, not portco
)


def _derive_name_from_url(url: str) -> str:
    """Best-effort name from URL. Operator review will refine.

    LinkedIn company URLs: pull the company slug.
    Plain domains: take SLD, kebab-split, title-case.
    """
    parsed = urllib.parse.urlparse(url)
    host = (parsed.netloc or "").lower().removeprefix("www.")
    path = parsed.path.strip("/")

    if "linkedin.com/company" in url.lower():
        parts = path.split("/")
        if len(parts) >= 2 and parts[0] == "company":
            slug = parts[1]
            return slug.replace("-", " ").title()
        return host

    sld = host.split(".")[0]
    return sld.replace("-", " ").title()


def scrape(run_number: int, output_dir: Path | str) -> int:
    """Fetch the fund's portfolio page and write portcos to a per-fund CSV."""
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    html = r.text

    tree = HTMLParser(html)

    seen_urls: set[str] = set()
    rows: list[dict] = []
    today = date.today().isoformat()

    for a in tree.css("a.image-slide-anchor"):
        href = a.attributes.get("href", "")
        if not href or not href.startswith(("http://", "https://")):
            continue
        if any(frag in href.lower() for frag in _IGNORE_HOST_FRAGMENTS):
            continue
        normalized = href.rstrip("/")
        if normalized in seen_urls:
            continue
        seen_urls.add(normalized)

        name = _derive_name_from_url(href)
        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "Company Name": name,
                "Company Slug": slugify(name),
                "Company Website": href,
                "Round": "unknown",
                "Round Date": "",
                "Lead": "unknown",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    out_path = Path(output_dir) / f"run_{run_number}" / f"{FUND_SLUG}.csv"
    write_rows(out_path, OUTPUT_HEADERS, rows)
    return len(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=1, help="Run number (default 1)")
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "network" / "fund_portfolio_scraping" / "individual_fund_portfolios"),
    )
    args = parser.parse_args()

    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{FUND_SLUG}.csv")


if __name__ == "__main__":
    main()
