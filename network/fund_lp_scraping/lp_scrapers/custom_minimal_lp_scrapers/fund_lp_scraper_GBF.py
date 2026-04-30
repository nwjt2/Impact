"""fund_lp_scraper_GBF.py

Scrapes LP roster of the Grassroots Business Fund (GBF) from primary
foundation disclosures.

KL Felicitas Foundation source:
https://charlykleissner.com/deep-impact/kl-felicitas-foundation/

That page is authored by KLF co-founder Charly Kleissner and names GBF
verbatim as a KLF investment alongside Global Partnerships and Acumen
Capital Markets I.

Per advice doc lesson 11: one custom scraper per fund. This one is bespoke
to the foundation pages it pulls from; extend the LP_SOURCES list with
(canonical_name, source_url, needle, commitment_year) tuples as new
primary-source disclosures surface.
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

SCRAPER_NAME = "fund_lp_scraper_GBF"
FUND_SLUG = "grassroots-business-fund"
INGO_SLUG = ""  # GBF is a non-INGO peer fund

# Each tuple: (canonical LP name, source URL, substring to verify in source, commitment year)
LP_SOURCES: list[tuple[str, str, str, str]] = [
    (
        "KL Felicitas Foundation",
        "https://charlykleissner.com/deep-impact/kl-felicitas-foundation/",
        "Grassroots Business Fund",
        "",  # KLF disclosure references the 2006-2017 portfolio era; exact year not stated
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
    missing: list[tuple[str, str]] = []

    for canonical, source_url, needle, year in LP_SOURCES:
        r = httpx.get(
            source_url,
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
            follow_redirects=True,
        )
        r.raise_for_status()
        html = r.content.decode("utf-8", errors="replace")
        if needle not in html:
            missing.append((canonical, source_url))
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
