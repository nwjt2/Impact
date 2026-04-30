"""fund_lp_scraper_AlignedClimateFund2.py

Scrapes LP roster of Aligned Climate Fund 2 LP (ACF2) from the Rockefeller
Foundation announcement of the joint Schmidt / Nathan Cummings / Rockefeller
LP commitment.

Source: https://www.rockefellerfoundation.org/news/nathan-cummings-rockefeller-and-schmidt-family-foundations-invest-in-aligned-climate-capitals-fund-2/

Per advice doc lesson 11: one custom scraper per fund. This is bespoke to
the Rockefeller press-release prose layout.
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

SCRAPER_NAME = "fund_lp_scraper_AlignedClimateFund2"
SOURCE_URL = (
    "https://www.rockefellerfoundation.org/news/"
    "nathan-cummings-rockefeller-and-schmidt-family-foundations-invest-"
    "in-aligned-climate-capitals-fund-2/"
)
FUND_SLUG = "aligned-climate-fund-2"
INGO_SLUG = ""
COMMITMENT_YEAR = "2024"

ACF2_LPs: list[tuple[str, str]] = [
    ("Schmidt Family Foundation", "Schmidt Family Foundation"),
    ("Nathan Cummings Foundation", "Nathan Cummings Foundation"),
    ("Rockefeller Foundation", "Rockefeller Foundation"),
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

    for canonical, needle in ACF2_LPs:
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
