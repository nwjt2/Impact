"""fund_lp_scraper_AltheliaSOF.py

Scrapes LP roster of the Althelia Sustainable Ocean Fund (formerly known as
Sustainable Ocean Fund) from ImpactAlpha's first-close coverage, which names
the David and Lucile Packard Foundation alongside EIB, AXA, FMO, and IDB.

Source: https://impactalpha.com/sustainable-oceans-fund-raises-37-5-million-for-life-under-water/

Verbatim attribution: "the European Investment Bank, the global insurance
company AXA and the Dutch development bank FMO. The Inter-American
Development Bank added $4 million and the David and Lucile Packard
Foundation made a $1 million program-related investment."

The Packard commitment is also independently disclosed in Packard's own
"Mission Investing Program Examples (2020)" PDF, but PDF byte-search is
unreliable so the trade-press URL is used as the scraper substrate.

Per advice doc lesson 11: one custom scraper per fund.
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

SCRAPER_NAME = "fund_lp_scraper_AltheliaSOF"
SOURCE_URL = (
    "https://impactalpha.com/"
    "sustainable-oceans-fund-raises-37-5-million-for-life-under-water/"
)
FUND_SLUG = "althelia-sustainable-ocean-fund"
INGO_SLUG = ""
COMMITMENT_YEAR = "2018"  # first close June 2018

ALTHELIA_SOF_LPs: list[tuple[str, str]] = [
    ("European Investment Bank", "European Investment Bank"),
    ("AXA", "insurance company AXA"),
    ("FMO", "Dutch development bank FMO"),
    ("Inter-American Development Bank", "Inter-American Development Bank"),
    ("David and Lucile Packard Foundation", "David and Lucile Packard Foundation"),
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

    for canonical, needle in ALTHELIA_SOF_LPs:
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
