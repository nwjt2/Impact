"""fund_portfolio_scraper_AcumenCapitalPartners.py

Scrapes Acumen's full published portfolio (160 entries) via WP REST API,
attributed to `acumen-capital-partners` — the umbrella fund row that
roll-ups all Acumen-managed direct investments (pre-fund Patient Capital
plus the Kawisafi / ARAF / ALEG / H2R vehicles).

Source: https://acumen.org/companies/
WP REST: https://acumen.org/wp-json/wp/v2/company

Why fund-portfolio rather than LP-portfolio: Acumen Capital Partners LLC is
the SEC-registered fund manager for the four Acumen INGO-Backed Funds, not
itself an outside investor. Treating it as a fund means companies in
Acumen's portfolio that overlap with other INGO funds (Kiva-RIF etc.) are
correctly counted as INGO-co-funded rather than externally co-invested,
matching the catalysts page framing.

Slug discipline: title is kebab-cased into Company Slug. For the handful
of companies that already exist in the catalogue under a different slug
(e.g. Acumen's feed has `soluna-energia-2`; catalogue has `soluna-energia`),
override to the catalogue slug so cross-references work.
"""
from __future__ import annotations

import html as html_lib
import json
import sys
from datetime import date
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "fund_portfolio_scraper_AcumenCapitalPartners"
FUND_SLUG = "acumen-capital-partners"
INGO_SLUG = "acumen"
SOURCE_URL = "https://acumen.org/companies/"
WPJSON_URL = "https://acumen.org/wp-json/wp/v2/company?per_page=100"

# Acumen's WP feed appends `-N` when a slug collides with another post type;
# normalise to the catalogue's canonical slugs.
_SLUG_OVERRIDES: dict[str, str] = {
    "soluna-energia-2": "soluna-energia",
    "pagatech": "paga",
    "coschool-2": "coschool",
    "easy-solar-2": "easy-solar",
}

# A few Acumen titles render with all-caps brand styling; normalise display
# names where the slug is correct but the title would look ugly in lists.
_NAME_OVERRIDES: dict[str, str] = {
    "burn-manufacturing": "Burn Manufacturing",
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


def _fetch_all_pages() -> list[dict]:
    items: list[dict] = []
    for page in range(1, 10):
        url = f"{WPJSON_URL}&page={page}"
        r = httpx.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
            follow_redirects=True,
        )
        r.raise_for_status()
        batch = json.loads(r.content.decode("utf-8", errors="replace"))
        if not batch:
            break
        items.extend(batch)
        total_pages = int(r.headers.get("X-WP-TotalPages") or 1)
        if page >= total_pages:
            break
    return items


def scrape(run_number: int, output_dir: Path | str) -> int:
    items = _fetch_all_pages()
    if not items:
        raise RuntimeError(f"{SCRAPER_NAME}: empty WP feed — Acumen may have moved the API")

    today = date.today().isoformat()
    rows: list[dict] = []
    seen: set[str] = set()

    for d in items:
        feed_slug = (d.get("slug") or "").strip()
        title = html_lib.unescape((d.get("title") or {}).get("rendered") or "").strip()
        if not feed_slug or not title:
            continue
        canonical_slug = _SLUG_OVERRIDES.get(feed_slug) or slugify(title)
        if canonical_slug in seen:
            continue
        seen.add(canonical_slug)
        canonical_name = _NAME_OVERRIDES.get(canonical_slug, title)
        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "Company Name": canonical_name,
                "Company Slug": canonical_slug,
                "Company Website": "",
                "Round": "unknown",
                "Round Date": "",
                "Lead": "unknown",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if len(rows) < 50:
        raise RuntimeError(
            f"{SCRAPER_NAME}: only {len(rows)} rows — Acumen typically lists 150+. "
            f"Feed structure may have changed."
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
