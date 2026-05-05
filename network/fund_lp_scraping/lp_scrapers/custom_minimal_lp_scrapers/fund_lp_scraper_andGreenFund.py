"""fund_lp_scraper_andGreenFund.py

Scrapes LP roster of the &Green Fund from the fund's own about-us page,
which prose-discloses the fund's anchor LPs and capital providers in
narrative form (not a logo wall).

Source: https://www.andgreen.fund/about-us/

Per advice doc lesson 11: one custom scraper per fund. This one is bespoke
to the andgreen.fund about page's prose layout — it does not generalise.

Excluded by editorial discipline:
- IDH (incorporator/sponsor — already captured as INGO Slug, not an LP)
- GEF/UNEP ("added its name as a promotor" — language too soft to assert
  a capital commitment)

Each LP below has a verbatim capital commitment quoted from the source page.
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

SCRAPER_NAME = "fund_lp_scraper_andGreenFund"
SOURCE_URL = "https://www.andgreen.fund/about-us/"
FUND_SLUG = "andgreen-fund"
INGO_SLUG = "idh-stichting-idh-sustainable-trade-initiative"

# Each tuple: (canonical LP name, substring to verify in source, commitment year)
LPS: list[tuple[str, str, str]] = [
    (
        "Norwegian International Climate and Forest Initiative",
        "NICFI, committed nearly",
        "2017",
    ),
    (
        "FMO",
        "long-term financing from FMO",
        "2021",
    ),
    (
        "UK Mobilising Finance for Forests",
        "Mobilising Finance for Forests",
        "2021",
    ),
    (
        "Unilever",
        "Unilever Group was",
        "2019",
    ),
    (
        "Green Climate Fund",
        "GCF Board approved",
        "2023",
    ),
    (
        "Central African Forest Initiative",
        "Central African Forest Initiative",
        "2024",
    ),
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

    for canonical, needle, year in LPS:
        if needle not in html:
            missing.append(canonical)
            continue
        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "LP Name": canonical,
                "LP Slug": slugify(canonical),
                "Commitment Year": year,
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
