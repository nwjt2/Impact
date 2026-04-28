"""fund_lp_scraper_KRIF.py

Scrapes LP roster of the Kiva Refugee Investment Fund (KRIF) from Kiva
Capital Management's final-close press release on PR Newswire.

Source: https://www.prnewswire.com/news-releases/kiva-capital-management-announces-final-close-of-kiva-refugee-investment-fund-to-scale-lending-to-fragile-communities-globally-301264464.html

Approach: hand-curated list of LP names sourced from this specific press
release. The scraper VERIFIES each name appears in the fetched HTML — if any
are missing the page may have been edited / replaced and the scraper exits
non-zero so the operator gets paged.

Per advice doc lesson 11: one custom scraper per fund. This one is bespoke
to the KRIF press release's prose layout — it does not generalise.
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

SCRAPER_NAME = "fund_lp_scraper_KRIF"
SOURCE_URL = (
    "https://www.prnewswire.com/news-releases/"
    "kiva-capital-management-announces-final-close-of-kiva-refugee-"
    "investment-fund-to-scale-lending-to-fragile-communities-globally-301264464.html"
)
FUND_SLUG = "kiva-refugee-investment-fund"
INGO_SLUG = "kiva-org"
COMMITMENT_YEAR = "2021"  # final close 7 April 2021

# Each tuple: (canonical name to write, substring to verify in source)
KRIF_LPs: list[tuple[str, str]] = [
    ("U.S. International Development Finance Corporation", "U.S. Development Finance Corporation"),
    ("Soros Economic Development Fund", "Soros Economic Development Fund"),
    ("Sobrato Philanthropies", "Sobrato Philanthropies"),
    ("ImpactAssets", "ImpactAssets"),
    ("Ceniarth", "Ceniarth"),
    ("The Missionary Sisters of the Sacred Heart of Jesus", "Missionary Sisters of the Sacred Heart of Jesus"),
    ("Tiedemann Advisors", "Tiedemann Advisors"),
    ("The Shapiro Family Foundation", "Shapiro Family Foundation"),
    ("The Dunn Family Charitable Foundation", "Dunn Family Charitable Foundation"),
    ("The Fairmount Foundation", "Fairmount Foundation"),
    ("Mercy Investment Services", "Mercy Investment Services"),
    ("Vanguard Charitable", "Vanguard Charitable"),
    ("CapShift", "CapShift"),
    ("The JMB Charitable Fund", "JMB Charitable Fund"),
    ("The Todd and Anne McCormack Fund", "Todd and Anne McCormack Fund"),
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

    for canonical, needle in KRIF_LPs:
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
        # Hard failure per lesson 4 — don't silently emit a partial roster.
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
