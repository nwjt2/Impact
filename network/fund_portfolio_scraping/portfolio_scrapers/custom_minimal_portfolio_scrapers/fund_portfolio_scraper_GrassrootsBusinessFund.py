"""fund_portfolio_scraper_GrassrootsBusinessFund.py

Scrapes Grassroots Business Fund's case-study portfolio from the /impact page.

Source: https://www.gbfund.org/impact

Notes for next operator:
- Squarespace site, fully static HTML. The /impact page contains a "Case
  Studies" section with each portfolio company as an <h4> heading inside a
  <div> container. As of 2026-04-29 there are 7 case-study portcos:
  Wamu, Villa Andina, SOKO, Gone Rural, Chemicaland, Jaipur Rugs, Zana Africa.
- These are highlighted/anonymized case-study companies, not GBF's full
  active investment list (the catalogue notes ~21 active investments). The
  case-study list is the public-disclosure subset; per honesty discipline
  we emit only what the page actually shows. Future enrichment may add the
  remainder if a fuller listing becomes public.
- We hand-curate a verification list (canonical_name, needle) so a page
  edit that drops a portco fails loudly. New <h4>s the operator hasn't
  whitelisted are surfaced as a warning but don't break the scrape.
"""
from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

import httpx
from selectolax.parser import HTMLParser

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "fund_portfolio_scraper_GrassrootsBusinessFund"
SOURCE_URL = "https://www.gbfund.org/impact"
FUND_SLUG = "grassroots-business-fund"
INGO_SLUG = ""  # non-INGO

OUTPUT_HEADERS = [
    "Fund Slug",
    "INGO Slug",
    "Company Name",
    "Company Slug",
    "Company Website",
    "Round",
    "Round Date",
    "Lead",
    "Source URL",
    "Source Date",
    "Scraping Method Used",
]

# Curated whitelist: (canonical name, h4 text needle).
# If the page drops a portco the scraper exits non-zero (Lesson 17 / KRIF
# pattern). New <h4>s that don't match any needle are logged but skipped.
GBF_PORTCOS: list[tuple[str, str]] = [
    ("Wamu", "Wamu"),
    ("Villa Andina", "Villa Andina"),
    ("SOKO", "SOKO"),
    ("Gone Rural", "Gone Rural"),
    ("Chemicaland", "Chemicaland"),
    ("Jaipur Rugs", "Jaipur Rugs"),
    ("Zana Africa", "Zana Africa"),
]


def scrape(run_number: int, output_dir: Path | str) -> int:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    tree = HTMLParser(r.content.decode("utf-8", errors="replace"))

    # Collect short <h4> texts that look like portco names.
    h4_texts: set[str] = set()
    for h in tree.css("h4"):
        t = (h.text() or "").strip()
        # Skip the "By supporting..." marketing-copy h4 plus any oversized
        # paragraph-style h4s.
        if 2 <= len(t) <= 50 and not t.startswith("By "):
            h4_texts.add(t)

    today = date.today().isoformat()
    rows: list[dict] = []
    missing: list[str] = []

    for canonical, needle in GBF_PORTCOS:
        if not any(needle == t or needle in t for t in h4_texts):
            missing.append(canonical)
            continue
        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "Company Name": canonical,
                "Company Slug": slugify(canonical),
                "Company Website": "",
                "Round": "unknown",
                "Round Date": "",
                "Lead": "unknown",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if missing:
        raise RuntimeError(
            f"{SCRAPER_NAME}: {len(missing)} expected portco(s) not found in source "
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
        default=str(REPO_ROOT / "network" / "fund_portfolio_scraping" / "individual_fund_portfolios"),
    )
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{FUND_SLUG}.csv")


if __name__ == "__main__":
    main()
