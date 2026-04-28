"""fund_lp_scraper_OxfamSEIIF.py

Scrapes the Small Enterprise Impact Investing Fund (SEIIF) named launch
partners from Third Sector's October 2012 fund-launch press article.

Source: https://www.thirdsector.co.uk/oxfam-launches-fund-impact-investments-developing-world/finance/article/1154243

Background: SEIIF was launched July 2012 by Oxfam, the City of London
Corporation (which committed a $500k cornerstone), and Symbiotics (Geneva-
based microfinance investment company) as fund manager. The fund's first
investment (October 2012) was a $1m loan to Xac Leasing in Mongolia — that's
a portco, not an LP, so it's excluded from this LP scraper.

The catalogue notes also reference Oxfam's "private and institutional
investors" but no further LP names are disclosed in the article body.

Per advice doc lesson 11: bespoke scraper for one bespoke press article.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "fund_lp_scraper_OxfamSEIIF"
SOURCE_URL = (
    "https://www.thirdsector.co.uk/"
    "oxfam-launches-fund-impact-investments-developing-world/finance/article/1154243"
)
FUND_SLUG = "oxfam-seiif"
INGO_SLUG = "oxfam"
COMMITMENT_YEAR = "2012"

# (canonical name, substring needle to verify in fetched HTML)
SEIIF_LPS: list[tuple[str, str]] = [
    ("Oxfam", "Oxfam"),
    ("City of London Corporation", "City of London Corporation"),
    ("Symbiotics", "Symbiotics"),
]

OUTPUT_HEADERS = [
    "Fund Slug",
    "INGO Slug",
    "LP Name",
    "LP Slug",
    "Commitment Year",
    "Source URL",
    "Source Date",
    "Confidence",
    "Scraping Method Used",
]


def scrape(run_number: int, output_dir: Path | str) -> int:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    html = r.content.decode("utf-8", errors="replace")

    rows: list[dict] = []
    today = date.today().isoformat()
    missing: list[str] = []

    for canonical, needle in SEIIF_LPS:
        if needle not in html:
            missing.append(canonical)
            continue
        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "LP Name": canonical,
                "LP Slug": slugify(canonical),
                "Commitment Year": COMMITMENT_YEAR,
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Confidence": "confirmed",
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if missing:
        raise RuntimeError(
            f"{SCRAPER_NAME}: {len(missing)} expected LP(s) not found in source "
            f"(page may have been edited): {missing}"
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
        default=str(REPO_ROOT / "network" / "fund_lp_scraping" / "individual_fund_lps"),
    )
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{FUND_SLUG}.csv")


if __name__ == "__main__":
    main()
