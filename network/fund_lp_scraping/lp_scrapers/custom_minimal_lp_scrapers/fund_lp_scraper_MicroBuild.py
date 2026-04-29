"""fund_lp_scraper_MicroBuild.py

Scrapes the named equity-LP / co-owner roster of Habitat for Humanity's
MicroBuild Fund from Habitat's own program page.

Source: https://www.habitat.org/our-work/terwilliger-center-innovation-in-shelter/microbuild

Verbatim sentence:
"Triple Jump, Omidyar Network and MetLife Foundation serve as partners
and co-owners of the fund."

Structure: $10M equity tranche held by Habitat for Humanity International
(majority, 51%), Triple Jump (fund manager, partly owned by Oxfam
Netherlands), Omidyar Network (PRI), and MetLife Foundation, leveraged
~10x by $90M of OPIC (now DFC) debt at fund launch in 2012. The three
named "partners and co-owners" each hold an equity stake in the fund
vehicle — that is the LP-edge relationship the scraper records.

Per advice doc lesson 11: one custom scraper per fund. This file is
bespoke to the Habitat MicroBuild prose layout — it does not generalise.
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

SCRAPER_NAME = "fund_lp_scraper_MicroBuild"
SOURCE_URL = (
    "https://www.habitat.org/our-work/"
    "terwilliger-center-innovation-in-shelter/microbuild"
)
FUND_SLUG = "microbuild-fund"
INGO_SLUG = "habitat-for-humanity"
COMMITMENT_YEAR = "2012"  # fund launch / equity tranche assembled at inception

# (canonical LP name, substring needle to verify in source HTML)
MICROBUILD_LPS: list[tuple[str, str]] = [
    ("Triple Jump", "Triple Jump"),
    ("Omidyar Network", "Omidyar Network"),
    ("MetLife Foundation", "MetLife Foundation"),
]

# Verification needles — the page must still describe the co-ownership
# relationship, otherwise the scraper has lost its provenance.
EXTRA_VERIFICATION_NEEDLES: list[str] = [
    "partners and co-owners",
    "MicroBuild",
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

    missing_context = [n for n in EXTRA_VERIFICATION_NEEDLES if n not in html]
    if missing_context:
        raise RuntimeError(
            f"{SCRAPER_NAME}: source page no longer describes the MicroBuild "
            f"co-ownership relationship — missing: {missing_context}"
        )

    for canonical, needle in MICROBUILD_LPS:
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
