"""fund_portfolio_scraper_IFCAMC.py

Scrapes the IFC Asset Management Company portfolio across all 9 paginated
pages, attributed to ifc-aip (the IFC Accelerator Investment Platform —
the catalogue's IFC AMC entry).

Source: https://www.ifcamc.org/portfolio?page=0..8

Notes for next operator:
- Tailwind-styled Next.js-style server-rendered HTML. Each portfolio entry
  is an <article> with:
    - <h3>NAME</h3>
    - GEOGRAPHY/INDUSTRY spans
    - An outbound link in a commented-out <a href="..."> wrapper around
      the article body. We extract the URL from the comment with regex.
- 9 pages × ~16 articles ≈ 140+ entries. Mix of operating companies AND
  PE/VC funds (Adenia Capital IV, Actera Partners III, Multiples Private
  Equity Fund III, etc.). Per the catalogue convention these all land as
  Company Slug entries — the network treats them uniformly as "investments
  by ifc-aip", which matches IFC AMC's actual investment behaviour (a
  fund-of-funds with direct co-investments).
- This is an UMBRELLA scrape attributed to `ifc-aip`. The IFC AMC portfolio
  spans many AMC-managed vehicles (ALAC Fund, Russian Bank Cap Fund, Catalyst
  Fund, AMP, ASF, etc.); per-vehicle attribution is a future enrichment.
"""
from __future__ import annotations

import re
import sys
import time
from datetime import date
from pathlib import Path

import httpx
from selectolax.parser import HTMLParser

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "fund_portfolio_scraper_IFCAMC"
SOURCE_URL = "https://www.ifcamc.org/portfolio"
FUND_SLUG = "ifc-aip"
INGO_SLUG = ""  # non-INGO

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

# Pages 0-8 inclusive (9 pages).
PAGE_RANGE = range(0, 9)

# Per-article block extracts:
#   - optional commented-out outbound href:  <!-- <a href="URL" target="_blank"> -->
#   - <h3 ...>NAME</h3>
#   - GEOGRAPHY <span>...</span><span>VALUE</span>
#   - INDUSTRY <span>...</span><span>VALUE</span>
_ARTICLE_RE = re.compile(
    r'<article[^>]*>.*?'
    r'(?:<!-- <a href="([^"]*)" [^>]*> -->)?\s*'
    r'<h3[^>]*>([^<]+)</h3>'
    r'.*?GEOGRAPHY[^>]*>\s*<span[^>]*>([^<]+)</span>'
    r'.*?INDUSTRY[^>]*>\s*<span[^>]*>([^<]+)</span>'
    r'.*?</article>',
    re.S,
)


def _clean(s: str) -> str:
    """Strip whitespace, collapse internal whitespace, decode common entities."""
    s = s.replace("&amp;", "&").replace("&nbsp;", " ").replace("&#039;", "'")
    return re.sub(r"\s+", " ", s).strip()


def scrape(run_number: int, output_dir: Path | str) -> int:
    today = date.today().isoformat()
    rows: list[dict] = []
    seen_names: set[str] = set()

    with httpx.Client(
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        for page in PAGE_RANGE:
            url = f"{SOURCE_URL}?page={page}"
            r = client.get(url)
            r.raise_for_status()
            html = r.content.decode("utf-8", errors="replace")

            page_count = 0
            for m in _ARTICLE_RE.finditer(html):
                href, name, geo, industry = m.groups()
                name = _clean(name)
                if not name:
                    continue
                if name in seen_names:
                    continue
                seen_names.add(name)
                page_count += 1

                website = (href or "").strip()
                rows.append(
                    {
                        "Fund Slug": FUND_SLUG,
                        "INGO Slug": INGO_SLUG,
                        "Company Name": name,
                        "Company Slug": slugify(name),
                        "Company Website": website,
                        "Round": "unknown",
                        "Round Date": "",
                        "Lead": "unknown",
                        "Source URL": url,
                        "Source Date": today,
                        "Scraping Method Used": SCRAPER_NAME,
                    }
                )

            if page_count == 0:
                # Page 0-7 should have ~16 each; page 8 may be a partial.
                # Zero is suspicious unless we've gone past the last page.
                if page < 8:
                    raise RuntimeError(
                        f"{SCRAPER_NAME}: zero entries on page {page} ({url}) — "
                        f"page structure may have changed."
                    )

            # Politeness sleep between pagination requests.
            time.sleep(1.0)

    if len(rows) < 100:
        raise RuntimeError(
            f"{SCRAPER_NAME}: only {len(rows)} total portfolio entries scraped "
            f"(expected 130+) — paging may have broken."
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
        default=str(REPO_ROOT / "network" / "fund_portfolio_scraping" / "individual_fund_portfolios"),
    )
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{FUND_SLUG}.csv")


if __name__ == "__main__":
    main()
