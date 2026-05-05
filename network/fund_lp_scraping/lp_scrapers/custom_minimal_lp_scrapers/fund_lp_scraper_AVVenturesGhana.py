"""fund_lp_scraper_AVVenturesGhana.py

Scrapes the LP roster of AV Ventures Ghana (AVVG) from the AV Ventures
fund-management site. AVVG is described on av-ventures.com/our_funds/ as
"an evergreen (permanent capital) blended finance vehicle, funded with
concessionary capital from the U.S. Department of Agriculture (USDA) and
a long-term loan from the US Development Finance Corporation (USDFC,
previously OPIC)."

Source: https://av-ventures.com/our_funds/

DFC's commitment is corroborated by the public DFC project record at
https://ewsdata.rightsindevelopment.org/projects/2018-avventuresgha-av-ventures-ghana-llc/
(2018, $2m senior loan). USDA's commitment came via the Ghana Poultry
Program (2016, ~$1.8m concessionary capital).

Per advice doc lesson 11: one custom scraper per fund.

Excluded by editorial discipline:
- ACDI/VOCA itself — INGO sponsor, captured via INGO Slug rather than an
  LP edge (consistent with KRIF and AV Frontiers CAIF precedent).
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

SCRAPER_NAME = "fund_lp_scraper_AVVenturesGhana"
SOURCE_URL = "https://av-ventures.com/our_funds/"
FUND_SLUG = "av-ventures-ghana"
INGO_SLUG = "acdi-voca"

# Each tuple: (canonical name to write, substring to verify in source, commitment year)
LPs: list[tuple[str, str, str]] = [
    (
        "U.S. Department of Agriculture",
        "U.S. Department of Agriculture (USDA)",
        "2016",
    ),
    (
        "U.S. International Development Finance Corporation",
        "US Development Finance Corporation (USDFC, previously OPIC)",
        "2018",
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

    for canonical, needle, year in LPs:
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
