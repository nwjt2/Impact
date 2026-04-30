"""fund_lp_scraper_LeapFrogIII.py

Scrapes named LPs of LeapFrog Investments' Emerging Consumer Fund III
($743M, vintage 2017-2019).

Three sources are stitched together — none alone names the full LP roster:

1. Rockefeller Foundation Zero Gap Fund portfolio summary (24 July 2025) —
   names Rockefeller Foundation as catalytic-capital LP via Zero Gap Fund.
   https://www.rockefellerfoundation.org/news/zero-gap-fund-mobilizes-1-05b-in-private-capital-to-advance-un-sustainable-development-goals/

2. Impact Investing Institute case study (impactinvest.org.uk) —
   discloses the 50-LP roster grouped by category. Verbatim:
   "The fund is backed by 50 limited partners (LPs) including insurers
   (including Prudential Financial and AIG), other institutional investors
   (including Morgan Stanley and Nuveen), DFI/MDBs (including CDC, IFC and
   EIB) and many foundations/endowments (including Ford Foundation and
   Rockefeller Foundation)."
   https://www.impactinvest.org.uk/case-study/leapfrog-investments-leapfrog-emerging-consumer-fund-iii-lp-2/

3. The Russell Family Foundation impact-investments portfolio disclosure —
   names "Leapfrog Emerging Consumer Fund III" as a TRFF holding
   (accessed via Global Impact Access Partnership wrapper).
   https://trff.org/impact-investments/browse-investments/

Each LP row carries the specific source URL it was sourced from, so the
provenance of each edge is preserved.

Per advice doc lesson 11: one custom scraper per fund. This file is
bespoke to the three cited sources — it does not generalise.
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

SCRAPER_NAME = "fund_lp_scraper_LeapFrogIII"
FUND_SLUG = "leapfrog-emerging-consumer-iii"
INGO_SLUG = ""  # non-INGO comparable (manager: LeapFrog Investments)

ROCKEFELLER_URL = (
    "https://www.rockefellerfoundation.org/news/"
    "zero-gap-fund-mobilizes-1-05b-in-private-capital-"
    "to-advance-un-sustainable-development-goals/"
)
III_URL = (
    "https://www.impactinvest.org.uk/case-study/"
    "leapfrog-investments-leapfrog-emerging-consumer-fund-iii-lp-2/"
)
TRFF_URL = "https://trff.org/impact-investments/browse-investments/"

# Each entry: (source_url, commitment_year, [(canonical, needle), ...],
#              [extra verification needles to confirm provenance])
SOURCES: list[tuple[str, str, list[tuple[str, str]], list[str]]] = [
    (
        ROCKEFELLER_URL,
        "",  # Zero Gap Fund launched 2019; specific commitment year not stated
        [
            ("Rockefeller Foundation", "Rockefeller Foundation"),
        ],
        ["LeapFrog", "Emerging Consumer Fund III", "Zero Gap"],
    ),
    (
        III_URL,
        "2017",  # ECF III vintage / first close 2017; final close May 2019
        [
            ("Nuveen", "Nuveen"),
            ("Prudential Financial", "Prudential Financial"),
        ],
        ["LeapFrog", "Emerging Consumer Fund III", "limited partners"],
    ),
    (
        TRFF_URL,
        "",  # TRFF disclosure does not state commitment year; portfolio reflects current holdings
        [
            ("The Russell Family Foundation", "Leapfrog Emerging Consumer Fund III"),
        ],
        ["Leapfrog Emerging Consumer Fund III"],
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
    rows: list[dict] = []
    today = date.today().isoformat()
    missing_overall: list[str] = []

    for source_url, year, lps, extra_needles in SOURCES:
        r = httpx.get(
            source_url,
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
            follow_redirects=True,
        )
        r.raise_for_status()
        html = r.content.decode("utf-8", errors="replace")

        missing_context = [n for n in extra_needles if n not in html]
        if missing_context:
            raise RuntimeError(
                f"{SCRAPER_NAME}: source {source_url} no longer describes the "
                f"LeapFrog ECF III LP relationship — missing: {missing_context}"
            )

        for canonical, needle in lps:
            if needle not in html:
                missing_overall.append(f"{canonical} ({source_url})")
                continue
            rows.append(
                {
                    "Fund Slug": FUND_SLUG,
                    "INGO Slug": INGO_SLUG,
                    "LP Name": canonical,
                    "LP Slug": slugify(canonical),
                    "Commitment Year": year,
                    "Source URL": source_url,
                    "Source Date": today,
                    "Confidence": "confirmed",
                    "Scraping Method Used": SCRAPER_NAME,
                }
            )

    if missing_overall:
        raise RuntimeError(
            f"{SCRAPER_NAME}: {len(missing_overall)} expected LP(s) not found "
            f"in source (page may have been edited): {missing_overall}"
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
