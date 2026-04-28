"""fund_portfolio_scraper_AndGreen.py

Scrapes &Green Fund's portfolio archive page.
Source: https://www.andgreen.fund/portfolio/

Notes for next operator:
- WordPress (Uncode theme) site, clean static HTML.
- Each portfolio item is a custom post type at /portfolio/<slug>/.
- The archive page lists all items; portfolio anchors also appear in the
  site's mega-menu, but we lock to the archive page as the canonical source.
- The site has a few duplicate slugs that point at the same company under
  shorter and longer URLs (e.g. /portfolio/etg/ vs /portfolio/etg-deal-disclosure/,
  /portfolio/fs-agrisolutions/ vs /portfolio/fuelingsustainability/).
  Map shorter aliases to the canonical longer slug below.

&Green Fund is sponsored by IDH (the Sustainable Trade Initiative) and
NICFI (Norwegian govt). Catalogue INGO slug is the IDH variant.
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

SCRAPER_NAME = "fund_portfolio_scraper_AndGreen"
SOURCE_URL = "https://www.andgreen.fund/portfolio/"
FUND_SLUG = "andgreen-fund"
INGO_SLUG = "idh-stichting-idh-sustainable-trade-initiative"

# Slug aliases to skip — they redirect/duplicate the canonical entry.
ALIAS_SKIPS = {"etg", "fuelingsustainability"}
# Non-portfolio paths that share the /portfolio/ prefix.
NON_PORTCO_PATHS = {"feed", "page"}

# Hand-cleaned display names (strip ALL CAPS marketing styling for display).
# Source = the page itself; this is purely a casing/punctuation tidy from the
# anchor text we extract — not a fabrication.
DISPLAY_NAME_OVERRIDES = {
    "agropecuaria-roncador-ltda-rondacor": "Agropecuária Roncador Ltda (Roncador)",
    "etg-deal-disclosure": "ETC Group (ETG)",
    "fs-agrisolutions": "Fueling Sustainability (FS Agrisolutions)",
    "hacienda-san-jose-hsj": "Hacienda San José (HSJ)",
    "marfrig-global-foods-s-a-marfrig": "Marfrig Global Foods S.A. (Marfrig)",
    "mercon-bv": "Mercon B.V.",
    "phuc-sinh": "Phuc Sinh Corporation",
    "pt-dharma-satya-nusantara-tbk-dsng": "PT Dharma Satya Nusantara Tbk (DSNG)",
    "pt-hilton-duta-lestari-hdl": "PT Hilton Duta Lestari (HDL)",
    "pt-royal-lestari-utama-rlu": "PT Royal Lestari Utama (RLU)",
    "valency-international": "Valency International",
}

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

_PORTFOLIO_HREF_RE = re.compile(
    r"^https://www\.andgreen\.fund/portfolio/([a-z0-9_-]+)/?$",
    re.IGNORECASE,
)


def _name_from_slug(slug: str) -> str:
    """Fallback display-name derivation when a slug isn't in the override map."""
    return " ".join(part.capitalize() for part in slug.replace("_", "-").split("-"))


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

    for a in tree.css("a"):
        href = a.attributes.get("href") or ""
        m = _PORTFOLIO_HREF_RE.match(href)
        if not m:
            continue
        path_slug = m.group(1).lower()
        if path_slug in NON_PORTCO_PATHS or path_slug in ALIAS_SKIPS:
            continue
        if path_slug in seen:
            continue
        seen.add(path_slug)

        name = DISPLAY_NAME_OVERRIDES.get(path_slug) or _name_from_slug(path_slug)
        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "Company Name": name,
                "Company Slug": slugify(name),
                "Company Website": f"https://www.andgreen.fund/portfolio/{path_slug}/",
                "Round": "unknown",
                "Round Date": "",
                "Lead": "unknown",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if not rows:
        raise RuntimeError(
            f"{SCRAPER_NAME}: zero portfolio anchors found at {SOURCE_URL} "
            f"(selectors may have broken)"
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
