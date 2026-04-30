"""fund_lp_scraper_MeloyFund.py

Scrapes named LP roster of the Meloy Fund I, LP — a first-of-its-kind
impact investment fund (debt + equity) for sustainable small-scale
coastal fisheries in Indonesia and the Philippines, US$22M total close.

Fund GP is a wholly-owned subsidiary of Rare (US-based global
conservation INGO); fund manager is Deliberate Capital (Rare's GP arm).

Source: PR Newswire / PRWeb first-close press release (4 August 2017) —
https://www.prweb.com/releases/impact_investment_fund_marks_first_close_of_10_million_for_sustainable_coastal_fisheries/prweb14562946.htm

Verbatim attributions on the source page:
- "Lukas Walton, who will have a significant interest in the Meloy Fund
  via the Lukas Walton Fund of the Walton Family Foundation"
- "US$6 million anticipated from the Global Environmental Facility (GEF)
  as anchor investor"

Per advice doc lesson 11: one custom scraper per fund. This is bespoke
to the PRWeb prose layout.
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

SCRAPER_NAME = "fund_lp_scraper_MeloyFund"
SOURCE_URL = (
    "https://www.prweb.com/releases/"
    "impact_investment_fund_marks_first_close_of_10_million_for_sustainable_coastal_fisheries/"
    "prweb14562946.htm"
)
FUND_SLUG = "meloy-fund-i"
INGO_SLUG = ""
COMMITMENT_YEAR = "2017"

MELOY_LPs: list[tuple[str, str]] = [
    ("Walton Family Foundation", "Lukas Walton Fund of the Walton Family Foundation"),
    ("Global Environment Facility", "Global Environmental Facility (GEF) as anchor investor"),
]

CONTEXT_NEEDLES = [
    "Meloy Fund I, LP",
    "Rare, a global conservation organization, is the Managing Member",
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

    missing_context = [n for n in CONTEXT_NEEDLES if n not in html]
    if missing_context:
        raise RuntimeError(
            f"{SCRAPER_NAME}: source no longer describes the Meloy Fund "
            f"first-close announcement — missing context: {missing_context}"
        )

    rows: list[dict] = []
    today = date.today().isoformat()
    missing: list[str] = []

    for canonical, needle in MELOY_LPs:
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
