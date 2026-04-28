"""fund_lp_scraper_WCIF3.py

Scrapes LP roster of WaterCredit Investment Fund 3 (WCIF3) from
WaterEquity's first-close press release on PR Newswire.

Source: https://www.prnewswire.com/news-releases/waterequity-announces-the-first-closing-of-its-us-50-million-impact-investment-fund-300716450.html

Same hand-curated-with-verification pattern as fund_lp_scraper_KRIF and
fund_lp_scraper_WaterEquityGAF4. WCIF3's first-close press release names
seven investors at $33m first close (Sept 2018); the fund went on to a
$50m final close in March 2019, but per-LP names beyond this initial
roster are not consistently in primary sources.

Per advice doc lesson 11: one custom scraper per fund. This is bespoke to
this PR Newswire release.
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

SCRAPER_NAME = "fund_lp_scraper_WCIF3"
SOURCE_URL = (
    "https://www.prnewswire.com/news-releases/"
    "waterequity-announces-the-first-closing-of-its-us-50-million-"
    "impact-investment-fund-300716450.html"
)
FUND_SLUG = "watercredit-investment-fund-3"
INGO_SLUG = "water-org"
COMMITMENT_YEAR = "2018"  # first close 20 Sept 2018

# (canonical LP name, substring to verify in source HTML)
WCIF3_LPs: list[tuple[str, str]] = [
    ("Bank of America", "Bank of America"),
    ("Overseas Private Investment Corporation (OPIC)", "Overseas Private Investment Corporation"),
    ("Ceniarth LLC", "Ceniarth"),
    ("Niagara Bottling", "Niagara Bottling"),
    ("Conrad N. Hilton Foundation", "Conrad N. Hilton"),
    ("Skoll Foundation", "Skoll"),
    ("Osprey Foundation", "Osprey"),
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

    for canonical, needle in WCIF3_LPs:
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
