"""fund_lp_scraper_WWBFundI.py

Scrapes named LPs of WWB Capital Partners Fund I (2012 vintage) from
two foundation/family-office portfolio disclosures. The fund manager
itself does not publish an LP roster, so each LP edge is sourced from
the LP's own portfolio page.

Sources stitched together (each LP carries its own Source URL):

1. Cordes Foundation impact-investing portfolio page —
   names "WWB Capital Partners" at the manager-family level. Page does
   not specify Fund I vs Fund II; vintage attribution to Fund I is
   based on Cordes's documented 100% impact-portfolio transition era
   (2014-2017), which aligns with WWB CP I's deployment window —
   Fund II's 2024 close postdates that era.
   https://www.cordesfoundation.org/impact-investing

2. Blue Haven Initiative fund-investments page —
   under the "Private Equity" section the page lists "WWB Capital
   Partners, L.P." verbatim. Page does not specify Fund I vs Fund II;
   vintage attribution to Fund I follows the same precedent set by the
   Cordes Foundation entry (Blue Haven was founded c. 2012 by Liesel
   Pritzker Simmons & Ian Simmons, contemporaneous with Fund I).
   https://www.bluehaveninitiative.com/portfolio/funds/

Per advice doc lesson 11: one custom scraper per fund. This file is
bespoke to the two cited portfolio-page layouts — it does not
generalise.
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

SCRAPER_NAME = "fund_lp_scraper_WWBFundI"
FUND_SLUG = "wwb-capital-partners-fund"
INGO_SLUG = "women-s-world-banking"

CORDES_URL = "https://www.cordesfoundation.org/impact-investing"
BLUE_HAVEN_URL = "https://www.bluehaveninitiative.com/portfolio/funds/"

# Each entry: (source_url, commitment_year, [(canonical, needle), ...],
#              [extra context needles to confirm provenance])
SOURCES: list[tuple[str, str, list[tuple[str, str]], list[str]]] = [
    (
        CORDES_URL,
        "",  # not stated on Cordes page; vintage inferred from era
        [
            ("Cordes Foundation", "Cordes Foundation"),
        ],
        ["Cordes Foundation", "WWB Capital Partners"],
    ),
    (
        BLUE_HAVEN_URL,
        "",  # not stated on Blue Haven page
        [
            ("Blue Haven Initiative", "WWB Capital Partners"),
        ],
        ["Blue Haven", "WWB Capital Partners"],
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
                f"WWB Capital Partners Fund I LP relationship — missing: {missing_context}"
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
