"""portco_investor_scraper_Igloo.py

Scrapes Igloo's "Backed by a reputable global investor base" section.
Source: https://iglooinsure.com/about

Igloo is a portco of WWB Capital Partners Fund II (catalogue slug 'igloo').
Bricks-builder WordPress site; investor logos live inside
`<div id="backed-by-vcs" class="vcs__container">`. Each logo is an `<img>`
with a usable `alt` attribute.
"""
from __future__ import annotations

import html
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

SCRAPER_NAME = "portco_investor_scraper_Igloo"
SOURCE_URL = "https://iglooinsure.com/about"
COMPANY_SLUG = "igloo"

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

# Title-case fixups for known investors whose alt text is lowercased.
_NAME_FIXUPS = {
    "aca investments": "ACA Investments",
}


def _clean_name(alt: str) -> str:
    name = html.unescape(alt).strip()
    name = re.sub(r"\s+logo\s*$", "", name, flags=re.IGNORECASE)
    if name.lower() in _NAME_FIXUPS:
        return _NAME_FIXUPS[name.lower()]
    # Title-case for all-lowercase alts
    if name.islower():
        name = " ".join(p.capitalize() for p in name.split())
    return name


def scrape(run_number: int, output_dir: Path | str) -> int:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    tree = HTMLParser(r.content.decode("utf-8", errors="replace"))

    container = tree.css_first("#backed-by-vcs")
    if container is None:
        raise RuntimeError(f"{SCRAPER_NAME}: #backed-by-vcs not found — page structure changed")

    seen: set[str] = set()
    rows: list[dict] = []
    today = date.today().isoformat()

    for img in container.css("img[alt]"):
        alt = (img.attributes.get("alt") or "").strip()
        if not alt:
            continue
        name = _clean_name(alt)
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
        raise RuntimeError(f"{SCRAPER_NAME}: 0 investors extracted — selector may have changed")

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
