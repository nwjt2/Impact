"""fund_portfolio_scraper_KivaUmbrella.py

Scrapes Kiva's umbrella Field Partners list.
Source: https://www.kiva.org/partners

Notes for next operator:
- Static HTML, server-rendered. Each partner is a `<article class="partnerCard">`
  with a `<h1 class="name">` containing the canonical partner name and a
  `<div class='country'>` with the country.
- Per-partner detail pages live at
  `/about/where-kiva-works/partners/<numeric-id>`.
- 225 partners on the umbrella page (as of 2026-04-28). Cleanest data source
  encountered so far.

Attribution caveat:
- This scrape is the Kiva UMBRELLA portfolio. The fund vehicle in our
  catalogue is `kiva-refugee-investment-fund` (KRIF, $33m, 2019). KRIF
  funds only a subset of these 225 partners — those serving refugee /
  fragile-context populations. The umbrella over-attributes; per-fund
  attribution should be refined in a future enrichment pass against Kiva's
  KRIF reports.
- For now we tag all rows with Fund Slug = kiva-refugee-investment-fund and
  note in `Notes` that this is umbrella-level attribution.
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

SCRAPER_NAME = "fund_portfolio_scraper_KivaUmbrella"
SOURCE_URL = "https://www.kiva.org/partners"
FUND_SLUG = "kiva-refugee-investment-fund"
INGO_SLUG = "kiva-org"

OUTPUT_HEADERS = [
    "Fund Slug",
    "INGO Slug",
    "Company Name",
    "Company Slug",
    "Company Website",
    "Country",
    "Round",
    "Round Date",
    "Lead",
    "Source URL",
    "Source Date",
    "Scraping Method Used",
    "Notes",
]

UMBRELLA_NOTE = "Kiva umbrella attribution; refine to KRIF subset in enrichment"


_NA_PREFIX_RE = re.compile(r"^\s*N/A,\s*direct to\s*", re.IGNORECASE)


def _normalise_name(raw: str) -> str:
    """Strip Kiva placeholder prefix and tidy whitespace.

    "N/A, direct to Motopack SAS" -> "Motopack SAS"
    """
    name = _NA_PREFIX_RE.sub("", raw).strip()
    return re.sub(r"\s+", " ", name)


def scrape(run_number: int, output_dir: Path | str) -> int:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    # Force UTF-8 decoding — Kiva sometimes serves headers that mismatch the
    # actual byte stream, producing � replacement chars in non-ASCII names.
    html = r.content.decode("utf-8", errors="replace")
    tree = HTMLParser(html)

    seen_slugs: set[str] = set()
    rows: list[dict] = []
    today = date.today().isoformat()

    for card in tree.css("article.partnerCard"):
        name_el = card.css_first("h1.name a")
        if not name_el:
            continue
        raw = name_el.text(strip=True)
        if not raw:
            continue
        name = _normalise_name(raw)
        if not name:
            continue
        href = name_el.attributes.get("href", "")
        detail_url = href if href.startswith("http") else f"https://www.kiva.org{href}"

        country_el = card.css_first("div.country")
        country = country_el.text(strip=True) if country_el else ""

        company_slug = slugify(name)
        if company_slug in seen_slugs:
            continue
        seen_slugs.add(company_slug)

        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "Company Name": name,
                "Company Slug": company_slug,
                "Company Website": detail_url,
                "Country": country,
                "Round": "unknown",
                "Round Date": "",
                "Lead": "unknown",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Scraping Method Used": SCRAPER_NAME,
                "Notes": UMBRELLA_NOTE,
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
