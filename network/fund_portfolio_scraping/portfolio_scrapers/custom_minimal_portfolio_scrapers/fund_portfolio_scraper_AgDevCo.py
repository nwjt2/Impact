"""fund_portfolio_scraper_AgDevCo.py

Scrapes AgDevCo's investment portfolio.
Source: https://www.agdevco.com/our-investments

Notes for next operator:
- WordPress site, clean static HTML. No JS rendering required.
- Each investee has a per-company detail page at /our-investments/<slug>/.
  The slug IS a kebab-cased company name, so we get clean names directly
  without operator review (much cleaner than MCV which derived from external
  domains).
- This scrapes the AgDevCo UMBRELLA portfolio. The fund vehicle in our
  catalogue is "AgDevCo Smallholder Development Unit" — strictly speaking
  the umbrella may include other vehicles. Per-fund attribution is a future
  enrichment.
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

SCRAPER_NAME = "fund_portfolio_scraper_AgDevCo"
SOURCE_URL = "https://www.agdevco.com/our-investments"
FUND_SLUG = "agdevco-smallholder-development-unit"
INGO_SLUG = ""  # AgDevCo is non-INGO (UK FCDO-backed)

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

# Match per-investee detail-page paths.
# Excludes the index path itself and any non-investee subpaths.
_INVESTEE_PATH_RE = re.compile(r"^/our-investments/([a-z0-9-]+)/?$")


def _name_from_slug(slug: str) -> str:
    """Slug like 'evergreen-avocado-limited' -> 'Evergreen Avocado Limited'."""
    return " ".join(part.capitalize() for part in slug.split("-"))


def scrape(run_number: int, output_dir: Path | str) -> int:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    tree = HTMLParser(r.text)

    seen_slugs: set[str] = set()
    rows: list[dict] = []
    today = date.today().isoformat()

    for a in tree.css("a"):
        href = a.attributes.get("href") or ""
        m = _INVESTEE_PATH_RE.match(href)
        if not m:
            continue
        path_slug = m.group(1)
        if path_slug in seen_slugs:
            continue
        seen_slugs.add(path_slug)

        company_name = _name_from_slug(path_slug)
        company_slug = slugify(company_name)
        detail_url = f"https://www.agdevco.com{href}"
        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "Company Name": company_name,
                "Company Slug": company_slug,
                "Company Website": detail_url,
                "Round": "unknown",
                "Round Date": "",
                "Lead": "unknown",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Scraping Method Used": SCRAPER_NAME,
            }
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
