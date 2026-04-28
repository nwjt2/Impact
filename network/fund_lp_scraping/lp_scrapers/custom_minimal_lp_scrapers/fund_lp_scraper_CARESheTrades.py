"""fund_lp_scraper_CARESheTrades.py

Scrapes the CARE-SheTrades Impact Fund's named sponsors / capital partners
from Bamboo Capital Partners' 22 April 2020 press release announcing ITC's
formal entry into the fund.

Source: https://bamboocp.com/the-international-trade-centre-care-enterprises-and-bamboo-capital-partners-join-forces-to-boost-gender-equality-with-the-care-shetrades-impact-fund/

The press release names the three founding/anchor entities:
- The International Trade Centre (ITC) — joint UN/WTO agency, joined April 2020
- CARE Enterprises — CARE International's social enterprise affiliate, co-launch
- Bamboo Capital Partners — licensed fund manager (Luxembourg)

Per Kampani-style sponsor scrape precedent (5+ named sponsors -> LP edges),
we treat these three as confirmed LP-style ties to the fund. Catalogue notes
record CARE seeded a $10m first-loss tranche, but that detail is from CARE
documentation, not this press release — we don't add it here (honesty
discipline: every row's source must support it).

Per advice doc lesson 11: bespoke scraper for one bespoke press release.
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

SCRAPER_NAME = "fund_lp_scraper_CARESheTrades"
SOURCE_URL = (
    "https://bamboocp.com/"
    "the-international-trade-centre-care-enterprises-and-bamboo-capital-partners-"
    "join-forces-to-boost-gender-equality-with-the-care-shetrades-impact-fund/"
)
FUND_SLUG = "care-shetrades"
INGO_SLUG = "care-international"
COMMITMENT_YEAR = "2020"  # ITC formally joined April 2020 (fund launched June 2018)

# (canonical name, substring needle to verify in fetched HTML)
SHETRADES_LPS: list[tuple[str, str]] = [
    ("International Trade Centre", "International Trade Centre"),
    ("CARE Enterprises", "CARE Enterprises"),
    ("Bamboo Capital Partners", "Bamboo Capital Partners"),
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

    for canonical, needle in SHETRADES_LPS:
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
