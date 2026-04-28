"""portco_investor_scraper_Floodbase.py

Scrapes Floodbase's "Our Investors" section.
Source: https://www.floodbase.com/about

Floodbase is an MCV portco (slug 'floodbase'). Webflow site. Each investor
is an `<img>` inside a `.investors-logo_container` div. Most have an `alt`
attribute we can use directly; one has an empty alt and we derive the name
from the image filename.
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

SCRAPER_NAME = "portco_investor_scraper_Floodbase"
SOURCE_URL = "https://www.floodbase.com/about"
COMPANY_SLUG = "floodbase"

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


def _name_from_alt_or_filename(img_node) -> str | None:
    alt = (img_node.attributes.get("alt") or "").strip()
    if alt:
        # Strip noisy " logo" / " svg logo" suffixes
        return re.sub(r"\s+(svg\s+)?logo\s*$", "", alt, flags=re.IGNORECASE).strip()
    src = img_node.attributes.get("src") or ""
    if not src:
        return None
    # filename like ".../62bf60b949cc0f2cb8a3b759_collab-fund-light.svg"
    fname = src.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    # Drop the hex prefix if present
    fname = re.sub(r"^[0-9a-f]{12,}_", "", fname, flags=re.IGNORECASE)
    # Drop trailing variants like "-light", "-dark", "%201"
    fname = re.sub(r"(-light|-dark|%20\d+)$", "", fname, flags=re.IGNORECASE)
    # Convert to title case
    return " ".join(part.capitalize() for part in fname.replace("_", "-").split("-")).strip()


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

    # Each investor is an img inside any element matching .investors-logo_container
    for container in tree.css(".investors-logo_container"):
        for img in container.css("img"):
            name = _name_from_alt_or_filename(img)
            if not name:
                continue
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
