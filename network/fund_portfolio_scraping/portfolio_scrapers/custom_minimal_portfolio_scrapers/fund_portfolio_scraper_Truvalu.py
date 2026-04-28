"""fund_portfolio_scraper_Truvalu.py

Scrapes Truvalu Group's portfolio pages.
Source: https://truvalu-group.com/portfolio/  (paginated /page/2/, /page/3/)

Truvalu (originally founded 2015 by ICCO Cooperation; ICCO merged into
Cordaid in Jan 2021) maps to catalogue fund slug `truvalu-business-booster-fund`
under INGO `cordaid`. The site is a WordPress build with each portfolio
entry as a custom post type at `/portfolio/<slug>/`. The archive page anchors
each entry's display name as the link text.

Pagination: pages 1..N, stopping when a /page/<n>/ returns 404 (current is 3).

Per advice doc lesson 11: bespoke scraper for one bespoke page set.
"""
from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

import httpx
from selectolax.parser import HTMLParser

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "fund_portfolio_scraper_Truvalu"
SOURCE_URL = "https://truvalu-group.com/portfolio/"
FUND_SLUG = "truvalu-business-booster-fund"
INGO_SLUG = "cordaid"

# Non-portco hrefs that share the /portfolio/ prefix on the archive layout.
NON_PORTCO_PATHS = {"feed", "page"}

# Page list — Truvalu currently has 3 archive pages.
MAX_PAGES = 8

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

_PORTFOLIO_HREF_RE = re.compile(
    r"^https?://truvalu-group\.com/portfolio/([a-z0-9][a-z0-9-]*)/?$",
    re.IGNORECASE,
)


def _name_from_slug(slug: str) -> str:
    return " ".join(part.capitalize() for part in slug.replace("_", "-").split("-"))


def _fetch_page(page: int) -> str | None:
    url = SOURCE_URL if page == 1 else f"https://truvalu-group.com/portfolio/page/{page}/"
    r = httpx.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.content.decode("utf-8", errors="replace")


def scrape(run_number: int, output_dir: Path | str) -> int:
    today = date.today().isoformat()
    rows: list[dict] = []
    seen: set[str] = set()

    for page in range(1, MAX_PAGES + 1):
        html = _fetch_page(page)
        if html is None:
            break
        tree = HTMLParser(html)
        page_new = 0
        for a in tree.css("a"):
            href = a.attributes.get("href") or ""
            m = _PORTFOLIO_HREF_RE.match(href)
            if not m:
                continue
            slug = m.group(1).lower()
            if slug in NON_PORTCO_PATHS or slug in seen:
                continue
            text = (a.text() or "").strip()
            if not text:
                # The Elementor build emits two anchor tags per card — an
                # image-wrapper anchor with empty text and a title anchor.
                # Skip the empty one; the title one will be picked up.
                continue
            seen.add(slug)
            page_new += 1
            rows.append(
                {
                    "Fund Slug": FUND_SLUG,
                    "INGO Slug": INGO_SLUG,
                    "Company Name": text,
                    "Company Slug": slugify(text) or slug,
                    "Company Website": f"https://truvalu-group.com/portfolio/{slug}/",
                    "Round": "unknown",
                    "Round Date": "",
                    "Lead": "unknown",
                    "Source URL": SOURCE_URL,
                    "Source Date": today,
                    "Scraping Method Used": SCRAPER_NAME,
                }
            )
        if page > 1 and page_new == 0:
            break

    if not rows:
        raise RuntimeError(
            f"{SCRAPER_NAME}: zero portfolio anchors found at {SOURCE_URL} "
            f"(selectors may have broken)"
        )

    out_path = Path(output_dir) / f"run_{run_number}" / f"{FUND_SLUG}.csv"
    write_rows(out_path, OUTPUT_HEADERS, rows)
    return len(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument(
        "--output-dir",
        default=str(
            REPO_ROOT
            / "network"
            / "fund_portfolio_scraping"
            / "individual_fund_portfolios"
        ),
    )
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{FUND_SLUG}.csv")


if __name__ == "__main__":
    main()
