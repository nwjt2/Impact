"""portco_investor_scraper_Epoch.py

Scrapes Epoch's "Backed by" section.
Source: https://epoch.blue/about

Epoch is an MCV portco (slug 'epoch'). Webflow site, "Backed by" heading
followed by an `.investor-marquee` div containing logo `<img>` tags with
populated `alt` attributes.
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

SCRAPER_NAME = "portco_investor_scraper_Epoch"
SOURCE_URL = "https://epoch.blue/about"
COMPANY_SLUG = "epoch"

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

# MCV scraper itself emits FUND_SLUG=mercy-corps-ventures with name "MCV".
# When Epoch lists "MCV" as a backer, normalise to the catalogue's slug so
# the cross-reference works.
_NAME_FIXUPS = {
    "MCV": "Mercy Corps Ventures",
}


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

    # Each backer logo lives inside .investor-marquee; sometimes the slider
    # repeats the same logo for animation. Dedup by slug.
    for marquee in tree.css(".investor-marquee"):
        for img in marquee.css("img"):
            alt = (img.attributes.get("alt") or "").strip()
            if not alt:
                continue
            name = re.sub(r"\s+logo\s*$", "", alt, flags=re.IGNORECASE).strip()
            name = _NAME_FIXUPS.get(name, name)
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
        raise RuntimeError(f"{SCRAPER_NAME}: 0 backers extracted — selector may have changed")

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
