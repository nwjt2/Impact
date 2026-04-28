"""portco_investor_scraper_OneAcreFund.py

Scrapes One Acre Fund's grant funders from their public accolades page.
Source: https://oneacrefund.org/about-us/accolades/grants

OAF is a Kiva field partner in our catalogue (slug 'one-acre-fund'). It's
also a 501(c)(3) so its "investors" are GRANT funders, not equity LPs.
The advice doc treats both as one canonical investor record per organization.

Page structure: Drupal accordion. Each funder is an `.c-accordion__item`
with a `.c-accordion__button-heading` containing the funder name.
"""
from __future__ import annotations

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

SCRAPER_NAME = "portco_investor_scraper_OneAcreFund"
SOURCE_URL = "https://oneacrefund.org/about-us/accolades/grants"
COMPANY_SLUG = "one-acre-fund"

OUTPUT_HEADERS = [
    "Company Slug",
    "Investor Name",
    "Investor Slug",
    "Round",
    "Round Date",
    "Lead",
    "Source URL",
    "Source Date",
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
    tree = HTMLParser(html)

    seen_slugs: set[str] = set()
    rows: list[dict] = []
    today = date.today().isoformat()

    for el in tree.css("span.c-accordion__button-heading"):
        name = el.text(strip=True)
        if not name:
            continue
        # Drop any trailing whitespace or formatting artifacts
        name = " ".join(name.split())
        slug = slugify(name)
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        rows.append(
            {
                "Company Slug": COMPANY_SLUG,
                "Investor Name": name,
                "Investor Slug": slug,
                "Round": "grant",
                "Round Date": "",
                "Lead": "unknown",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    out_path = Path(output_dir) / f"run_{run_number}" / f"{COMPANY_SLUG}.csv"
    write_rows(out_path, OUTPUT_HEADERS, rows)
    return len(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "network" / "portco_investor_scraping" / "individual_portco_investors"),
    )
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{COMPANY_SLUG}.csv")


if __name__ == "__main__":
    main()
