"""portco_investor_scraper_SatellitesOnFire.py

Scrapes the investor block on Satellites On Fire's homepage.
Source: https://www.satellitesonfire.com/

Satellites On Fire is an MCV portco (catalogue slug 'satellitesonfire').
Wix site. The homepage embeds gallery items as JSON inside the page's
inline scripts. Each item has a `title` (entity name) and a `description`
field that distinguishes investors ("Investors" / "Inversores") from
program partners, awards, and accelerators. We filter to items whose
description literally calls them an investor.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import date
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "portco_investor_scraper_SatellitesOnFire"
SOURCE_URL = "https://www.satellitesonfire.com/"
COMPANY_SLUG = "satellitesonfire"

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

# Items in the gallery whose description naming-convention marks them as
# investors. Site is bilingual (EN/ES). Anything else (programs, awards,
# universities, AWS Activate, etc.) is dropped.
_INVESTOR_DESCRIPTIONS = {"investors", "inversores"}

# Free-text descriptions on a few investor items don't use the canonical
# tag. These are explicit allowlist matches keyed by `title` -> reason
# the item is being kept (substring of the description that confirms it).
_TITLE_ALLOWLIST = {
    "Draper Cygnus": "is one of the most recognized Deep Tech funds",
    "Mercy Corps": "provided us with investment",
}

_GALLERY_ITEM_RE = re.compile(
    r'\{"itemId":"[^"]+","isSecure":[^,]+,"createdDate":"[^"]+","orderIndex":\d+,'
    r'"metaData":\{"description":"(?P<desc>[^"]*)","title":"(?P<title>[^"]+)"',
)


def _extract_items(html: str) -> list[tuple[str, str]]:
    return [(m.group("title"), m.group("desc")) for m in _GALLERY_ITEM_RE.finditer(html)]


def _is_investor(title: str, description: str) -> bool:
    if description.strip().lower() in _INVESTOR_DESCRIPTIONS:
        return True
    if title in _TITLE_ALLOWLIST and _TITLE_ALLOWLIST[title] in description:
        return True
    return False


def scrape(run_number: int, output_dir: Path | str) -> int:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    html = r.content.decode("utf-8", errors="replace")

    items = _extract_items(html)
    if not items:
        raise RuntimeError(f"{SCRAPER_NAME}: no gallery items parsed — page structure changed")

    seen: set[str] = set()
    rows: list[dict] = []
    today = date.today().isoformat()

    for title, desc in items:
        if not _is_investor(title, desc):
            continue
        name = json.loads(f'"{title}"')  # decode any \u escapes safely
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
        raise RuntimeError(f"{SCRAPER_NAME}: 0 investors after filtering — description tags may have changed")

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
