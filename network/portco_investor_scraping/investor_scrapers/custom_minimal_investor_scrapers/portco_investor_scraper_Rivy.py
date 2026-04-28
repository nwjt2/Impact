"""portco_investor_scraper_Rivy.py

Scrapes Rivy's (formerly Payhippo) "Our partners" section.
Source: https://payhippo.ng/

Rivy is an MCV portco. Catalogue slug is `payhippo` (we kept the original
slug when cleaning the company name to "Rivy" after the rebrand).
Webflow site; partners are `<img class="partner-logo">` inside
`.partners-bottom_container` (or similar). Slider duplicates entries.
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

SCRAPER_NAME = "portco_investor_scraper_Rivy"
SOURCE_URL = "https://payhippo.ng/"
COMPANY_SLUG = "payhippo"  # catalogue slug; company is now branded Rivy

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

    seen: set[str] = set()
    rows: list[dict] = []
    today = date.today().isoformat()

    for img in tree.css("img.partner-logo"):
        alt = (img.attributes.get("alt") or "").strip()
        if not alt:
            continue
        name = re.sub(r"\s+Logo\s*$", "", alt, flags=re.IGNORECASE).strip()
        slug = slugify(name)
        if slug in seen:
            continue
        seen.add(slug)
        rows.append(
            {
                "Company Slug": COMPANY_SLUG,
                "Investor Name": name,
                "Investor Slug": slug,
                "Round": "unknown",
                "Round Date": "",
                "Lead": "unknown",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if not rows:
        raise RuntimeError(f"{SCRAPER_NAME}: 0 partners extracted — selector may have changed")

    out_path = Path(output_dir) / f"run_{run_number}" / f"{COMPANY_SLUG}.csv"
    write_rows(out_path, OUTPUT_HEADERS, rows)
    return len(rows)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "network" / "portco_investor_scraping" / "individual_portco_investors"))
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{COMPANY_SLUG}.csv")


if __name__ == "__main__":
    main()
