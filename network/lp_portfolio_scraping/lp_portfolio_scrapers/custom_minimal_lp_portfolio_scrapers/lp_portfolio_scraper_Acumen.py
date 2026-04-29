"""lp_portfolio_scraper_Acumen.py

LP-portfolio scraper for Acumen Capital Partners.

Source: https://acumen.org/companies/
WP REST: https://acumen.org/wp-json/wp/v2/company

Approach: Acumen exposes their full portfolio as a `company` post type via
the WordPress REST API. Each entry is a company Acumen has invested in
(across their direct funds plus successor vehicles like KawiSafi and ALEG).
Status taxonomy distinguishes Active (148) and Exited (149); we keep both.

LP attribution: emit rows under `acumen-capital-partners` (the existing
investor-catalogue slug for Acumen's investing arm, since the parent
Acumen entry sits in ingos.csv rather than investors.csv).

Slug discipline: title is kebab-cased into Investee Slug. For the handful
of companies that already exist in our catalogue under a different slug
(e.g. Acumen's feed has `soluna-energia-2`; catalogue has `soluna-energia`),
override to the catalogue slug so the cross-reference works without
creating a duplicate portfolio_companies entry.
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

SCRAPER_NAME = "lp_portfolio_scraper_Acumen"
LP_NAME = "Acumen Capital Partners"
LP_SLUG = "acumen-capital-partners"
SOURCE_URL = "https://acumen.org/companies/"
WPJSON_URL = "https://acumen.org/wp-json/wp/v2/company?per_page=100"

# Acumen's feed sometimes appends `-2`/`-N` when a slug collides with another
# WP post type (status pages, etc.). Map these to their catalogue-canonical
# slugs so cross-references work.
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
    "LP Slug",
    "Investee Name",
    "Investee Slug",
    "Investee Type",
    "Commitment Year",
    "Source URL",
    "Source Date",
    "Confidence",
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
                "LP Slug": LP_SLUG,
                "Investee Name": canonical_name,
                "Investee Slug": canonical_slug,
                "Investee Type": "company",
                "Commitment Year": "",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Confidence": "confirmed",
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if len(rows) < 50:
        raise RuntimeError(
            f"{SCRAPER_NAME}: only {len(rows)} rows — Acumen typically lists 150+. "
            f"Feed structure may have changed."
        )

    out_path = Path(output_dir) / f"run_{run_number}" / f"{LP_SLUG}.csv"
    write_rows(out_path, OUTPUT_HEADERS, rows)
    return len(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "network" / "lp_portfolio_scraping" / "individual_lp_portfolios"),
    )
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{LP_SLUG}.csv")


if __name__ == "__main__":
    main()
