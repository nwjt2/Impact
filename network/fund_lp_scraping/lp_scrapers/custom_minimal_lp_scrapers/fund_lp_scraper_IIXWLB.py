"""fund_lp_scraper_IIXWLB.py

Scrapes the named investors / ecosystem partners of the IIX Women's
Livelihood Bond (WLB) Series from the IIX-published WLB6 launch press
release. WLB6 ($100m, December 2023) is the largest issuance in the series
and its press release names the most LPs of any single tranche.

Source: https://wlb.iixglobal.com/iixs-womens-livelihood-bond-6-breaks-new-ground/

Approach: hand-curated (canonical_name, needle) tuples — same prose-pattern
as KRIF / FEFISOL II / WCIF3. The PR explicitly lists ecosystem partners:
"Partners include the Swedish International Development Cooperation Agency
(Sida), Australian Department of Foreign Affairs and Trade (Australian DFAT),
Shearman & Sterling, Clifford Chance, Cyril Amarchand Mangaldas, Standard
Chartered Bank, ANZ, Global Affairs Canada, Nuveen, APG, PayPal, MetLife,
Uniting Financial Services (UFS), Pathfinder, Alvarium, Grieg Foundation
and Impax Asset Management. The U.S. International Development Finance
Corporation (DFC) will provide a loan to an affiliate of the issuer."

We INCLUDE: institutional investors, DFIs, foundations, asset managers,
corporate LPs (15 entities).
We EXCLUDE: arrangers/lead underwriters that play a structuring role rather
than holding LP positions (Shearman & Sterling and Clifford Chance and
Cyril Amarchand Mangaldas are law firms — advisors, not investors).

The catalogue's `iix-wlb-series` slug represents the bond series as a single
fund vehicle (WLB1-WLB7); attributing all LPs to the series-level slug is
the same umbrella treatment as the WaterEquity GAF series.
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

SCRAPER_NAME = "fund_lp_scraper_IIXWLB"
SOURCE_URL = (
    "https://wlb.iixglobal.com/"
    "iixs-womens-livelihood-bond-6-breaks-new-ground/"
)
FUND_SLUG = "iix-wlb-series"
INGO_SLUG = ""  # non-INGO
COMMITMENT_YEAR = "2023"  # WLB6 priced December 2023

# (canonical LP name, substring needle to verify in source HTML)
WLB6_LPS: list[tuple[str, str]] = [
    ("Swedish International Development Cooperation Agency (Sida)", "Sida"),
    ("Australian Department of Foreign Affairs and Trade (DFAT)", "Australian DFAT"),
    ("Standard Chartered Bank", "Standard Chartered"),
    ("ANZ", "ANZ"),
    ("Global Affairs Canada", "Global Affairs Canada"),
    ("Nuveen", "Nuveen"),
    ("APG", "APG"),
    ("PayPal", "PayPal"),
    ("MetLife", "MetLife"),
    ("Uniting Financial Services (UFS)", "Uniting Financial Services"),
    ("Pathfinder", "Pathfinder"),
    ("Alvarium", "Alvarium"),
    ("Grieg Foundation", "Grieg Foundation"),
    ("Impax Asset Management", "Impax Asset Management"),
    ("U.S. International Development Finance Corporation (DFC)", "U.S. International Development Finance Corporation"),
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

    for canonical, needle in WLB6_LPS:
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
