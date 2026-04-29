"""fund_lp_scraper_LeapFrogIII.py

Scrapes the Rockefeller Foundation LP role in LeapFrog Investments'
Emerging Consumer Fund III, from the Rockefeller Foundation's Zero
Gap Fund portfolio summary (24 July 2025).

Source: https://www.rockefellerfoundation.org/news/zero-gap-fund-mobilizes-1-05b-in-private-capital-to-advance-un-sustainable-development-goals/

The page describes Zero Gap Fund (Rockefeller Foundation's catalytic-
capital vehicle) and its portfolio of 14 funds / mechanisms. LeapFrog
Fund III is named explicitly: "LeapFrog's Emerging Consumer Fund III
(Fund III) is a growth equity fund serving low-income consumers in
Asia and Africa..." Rockefeller's $3M commitment took the form of an
insurance deductible backstopping a broader policy benefiting Fund
III equity investors. The scraper records Rockefeller Foundation as
the LP / catalytic capital provider; commitment year cannot be
pinned to a specific year from this source (Zero Gap Fund itself
launched 2019; the disclosure window covers data through Dec 2024).

Per advice doc lesson 11: one custom scraper per fund. This is
bespoke to the Rockefeller Foundation prose layout — it does not
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

SCRAPER_NAME = "fund_lp_scraper_LeapFrogIII"
SOURCE_URL = (
    "https://www.rockefellerfoundation.org/news/"
    "zero-gap-fund-mobilizes-1-05b-in-private-capital-"
    "to-advance-un-sustainable-development-goals/"
)
FUND_SLUG = "leapfrog-emerging-consumer-iii"
INGO_SLUG = ""  # non-INGO comparable (manager: LeapFrog Investments)
COMMITMENT_YEAR = ""  # not stated; Zero Gap Fund launched 2019

# (canonical LP name, substring needle to verify in source HTML)
# We require BOTH "LeapFrog" and "Rockefeller Foundation" to appear so the
# scraper fails loudly if the page is rewritten and either name vanishes.
LEAPFROG_III_LPS: list[tuple[str, str]] = [
    ("Rockefeller Foundation", "Rockefeller Foundation"),
]

# Additional verification needles — the page must still describe the
# Zero-Gap-Fund-into-LeapFrog-Fund-III relationship, otherwise the
# scraper has lost its provenance and exits non-zero.
EXTRA_VERIFICATION_NEEDLES: list[str] = [
    "LeapFrog",
    "Emerging Consumer Fund III",
    "Zero Gap",
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
            f"{SCRAPER_NAME}: source page no longer describes the Zero-Gap-into-"
            f"LeapFrog-Fund-III relationship — missing: {missing_context}"
        )

    for canonical, needle in LEAPFROG_III_LPS:
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
