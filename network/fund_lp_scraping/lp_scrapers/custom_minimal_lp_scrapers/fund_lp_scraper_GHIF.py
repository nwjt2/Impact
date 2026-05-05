"""fund_lp_scraper_GHIF.py

Scrapes LP roster of the Global Health Investment Fund (GHIF) from the
Global Health Investment Corporation (GHIC) homepage logo grid.

Source: https://ghicfunds.org/

Approach: hand-curated list of LP names sourced from the GHIC homepage's
logo-grid <img alt="..."> attributes (which disambiguate "The Pfizer
Foundation" from Pfizer Inc.) and the /about/ prose ("Other GHIF
stakeholders and investors include..."). The scraper VERIFIES each
needle appears in the fetched HTML — if any are missing the page may
have been edited / replaced and the scraper exits non-zero so the
operator gets paged.

Per advice doc lesson 11: one custom scraper per fund. This one is
bespoke to the GHIC homepage layout — it does not generalise.
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

SCRAPER_NAME = "fund_lp_scraper_GHIF"
SOURCE_URL = "https://ghicfunds.org/"
FUND_SLUG = "global-health-investment-fund"
INGO_SLUG = ""  # GHIF has no parent INGO; sponsored by Gates Foundation
COMMITMENT_YEAR = "2013"  # final close December 2013

# Each tuple: (canonical name to write, substring to verify in raw HTML).
# Needles target the homepage <img alt="..."> grid; "The Pfizer Foundation"
# alt text is the primary disambiguation against Pfizer Inc.
GHIF_LPs: list[tuple[str, str]] = [
    ("Bill & Melinda Gates Foundation", "Bill & Melinda Gates Foundation"),
    ("Grand Challenges Canada", "Grand Challenges Canada"),
    ("JP Morgan Chase", "JP Morgan Chase"),
    ("KfW Development Bank", "KFW - Bank aus Verantwortung"),
    (
        "German Federal Ministry for Economic Cooperation and Development (BMZ)",
        "Federal Ministry for Economic Cooperation and Development",
    ),
    ("International Finance Corporation (IFC)", 'alt="IFC"'),
    ("Children's Investment Fund Foundation", "Children's Investment Fund Foundation"),
    ("AXA Investment Managers", "Axa Investment managers"),
    ("Storebrand", 'alt="Storebrand"'),
    ("Merck & Co.", 'alt="Merck"'),
    ("The Pfizer Foundation", "The Pfizer Foundation"),
    ("GlaxoSmithKline (GSK)", "Glaxo Smith Kline"),
    ("Sida (Swedish International Development Agency)", 'alt="Sida"'),
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

    for canonical, needle in GHIF_LPs:
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
