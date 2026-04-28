"""portco_investor_scraper_Goldfinch.py

Scrapes Goldfinch's "Backed by" section.
Source: https://goldfinch.finance/

Goldfinch is an MCV portco (slug 'goldfinch'). Page is Next.js but the
"Backed by" section is in the initial server-rendered HTML — each backer is
an `<img alt="<Investor Name>">` inside a region following the heading.
"""
from __future__ import annotations

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

SCRAPER_NAME = "portco_investor_scraper_Goldfinch"
SOURCE_URL = "https://goldfinch.finance/"
COMPANY_SLUG = "goldfinch"

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

_HEADING = "Backed by"
_IMG_ALT_RE = re.compile(r'<img[^>]+alt="([^"]+)"', re.IGNORECASE)

# Source has typo "Andreesen Horowitz" — canonical is Andreessen Horowitz / a16z.
_NAME_FIXUPS = {
    "Andreesen Horowitz": "Andreessen Horowitz",
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

    idx = html.find(_HEADING)
    if idx == -1:
        raise RuntimeError(f"{SCRAPER_NAME}: '{_HEADING}' heading not found at {SOURCE_URL}")

    # The 6 real backers cluster in the first ~9kB after the heading.
    # Past that, a marketing section begins with images titled "Institutional
    # grade", "Decades of experience" etc. — those are NOT investors. Cap the
    # region to avoid them. If Goldfinch adds a 7th-8th backer, expand here.
    region = html[idx : idx + 9000]
    raw_alts = _IMG_ALT_RE.findall(region)

    # Skip alts that are obviously not investor names (e.g. icons, screenshots).
    seen: set[str] = set()
    rows: list[dict] = []
    today = date.today().isoformat()
    for alt in raw_alts:
        name = alt.strip()
        if not name:
            continue
        # Light hygiene: drop trailing 'logo' if present
        name = re.sub(r"\s+logo\s*$", "", name, flags=re.IGNORECASE).strip()
        # Apply known typo fixups
        name = _NAME_FIXUPS.get(name, name)
        slug = slugify(name)
        if slug in seen:
            continue
        # Filter out things that aren't investor names
        if len(name) < 3 or len(name) > 60:
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
        raise RuntimeError(f"{SCRAPER_NAME}: 0 backers extracted — page may have changed")

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
