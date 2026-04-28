"""fund_lp_scraper_WaterEquityGAF4.py

Scrapes LP roster of WaterEquity Global Access Fund IV from the Newswire
final-close press release.

Source: https://www.newswire.com/news/waterequity-announces-final-close-of-150-million-global-access-fund-iv-22128210

Approach: hand-curated list of LP names, each verified by substring match.
WACI4's $150m structure is corporate-LP-led: Starbucks is the anchor, joined
by Ecolab, Gap Inc., Reckitt and DuPont — all members of the Water Resilience
Coalition. The press release mentions debt capital alongside; equity LPs
named here are limited to the public roster in this single primary source.
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

SCRAPER_NAME = "fund_lp_scraper_WaterEquityGAF4"
SOURCE_URL = (
    "https://www.newswire.com/news/"
    "waterequity-announces-final-close-of-150-million-global-access-fund-iv-22128210"
)
FUND_SLUG = "waterequity-waci4"
INGO_SLUG = "water-org"
COMMITMENT_YEAR = "2023"  # final close July 2023

# (canonical LP name, substring to verify in source HTML)
WACI4_LPs: list[tuple[str, str]] = [
    ("Starbucks", "Starbucks"),
    ("Ecolab", "Ecolab"),
    ("Gap Inc.", "Gap Inc"),
    ("Reckitt", "Reckitt"),
    ("DuPont", "DuPont"),
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

    for canonical, needle in WACI4_LPs:
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
