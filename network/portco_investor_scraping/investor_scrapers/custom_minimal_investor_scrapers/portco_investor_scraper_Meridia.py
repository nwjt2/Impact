"""portco_investor_scraper_Meridia.py

Scrapes Meridia's "Key investors" section.
Source: https://www.meridia.land/team

Meridia is an MCV portco (slug 'meridia'). Webflow site; investors live in
`.investors_logo-inner` containers as `<img class="investor-logo">`. The
`alt` attributes are empty, so we derive names from the image filename
(same fallback used in the Floodbase scraper).
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

SCRAPER_NAME = "portco_investor_scraper_Meridia"
SOURCE_URL = "https://www.meridia.land/team"
COMPANY_SLUG = "meridia"

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

# Image filenames are slug-style; map known short-forms to canonical names.
_FILENAME_FIXUPS = {
    "ice": "ICE",
    "edaphon": "Edaphon",
    "regeneraion": "Regeneration",  # filename has a typo
    "cerulean": "Cerulean",
}


def _name_from_img(img_node) -> str | None:
    alt = (img_node.attributes.get("alt") or "").strip()
    if alt:
        return re.sub(r"\s+logo\s*$", "", alt, flags=re.IGNORECASE).strip()
    src = img_node.attributes.get("src") or ""
    if not src:
        return None
    fname = src.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    fname = re.sub(r"^[0-9a-f]{12,}_", "", fname, flags=re.IGNORECASE)
    fname = fname.lower()
    if fname in _FILENAME_FIXUPS:
        return _FILENAME_FIXUPS[fname]
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

    for img in tree.css("img.investor-logo"):
        name = _name_from_img(img)
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
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "network" / "portco_investor_scraping" / "individual_portco_investors"))
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{COMPANY_SLUG}.csv")


if __name__ == "__main__":
    main()
