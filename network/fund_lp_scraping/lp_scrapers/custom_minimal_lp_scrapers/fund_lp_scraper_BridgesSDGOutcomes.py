"""fund_lp_scraper_BridgesSDGOutcomes.py

Scrapes the named LPs / outcome funders / first-loss capital providers of
the Bridges Outcomes Partnerships SDG Outcomes Fund from UBS's final-close
press release.

Source: https://www.ubs.com/global/en/media/display-page-ndp/en-20250703-sdg-outcomes-fund.html

Approach: hand-curated list of LP names sourced from this specific UBS press
release (3 July 2025). Same prose-pattern as KRIF / WLB6 / FEFISOL II — each
(canonical_name, needle) tuple verifies the LP is actually mentioned in the
fetched HTML before emitting a row. If any needle is missing the page may
have been edited; the scraper exits non-zero so the operator gets paged.

Why this fund matters: UBS Optimus Foundation provided first-loss capital,
which surfaces UBS Optimus on the network view as a foundation-type investor
(previously absent from the rendered network despite being profiled on the
foundations page).

Per advice doc lesson 11: one custom scraper per fund. This is bespoke to
the UBS press-release prose layout — it does not generalise.
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

SCRAPER_NAME = "fund_lp_scraper_BridgesSDGOutcomes"
SOURCE_URL = (
    "https://www.ubs.com/global/en/media/display-page-ndp/"
    "en-20250703-sdg-outcomes-fund.html"
)
FUND_SLUG = "bridges-sdg-outcomes-fund"
INGO_SLUG = ""  # non-INGO comparable (manager: Bridges Outcomes Partnerships)
COMMITMENT_YEAR = "2025"  # final close 3 July 2025

# (canonical LP name, substring needle to verify in source HTML)
SDG_OUTCOMES_LPS: list[tuple[str, str]] = [
    ("UBS Optimus Foundation", "UBS Optimus Foundation"),
    ("European Investment Bank", "European Investment Bank"),
    ("U.S. International Development Finance Corporation", "Development Finance Corporation"),
    ("British International Investment", "British International Investment"),
    ("Legatum", "Legatum"),
    ("Tsao Family Office", "Tsao"),
    ("Ferd", "Ferd"),
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

    for canonical, needle in SDG_OUTCOMES_LPS:
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
