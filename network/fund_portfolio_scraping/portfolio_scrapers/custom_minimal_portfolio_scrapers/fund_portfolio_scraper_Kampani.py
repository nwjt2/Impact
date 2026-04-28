"""fund_portfolio_scraper_Kampani.py

Scrapes Kampani's portfolio page.
Source: https://www.kampani.org/portfolio

Notes for next operator:
- Webflow site, clean static HTML.
- Portfolio companies are listed as <h3> headings, grouped under three
  region <h2> banners ("Africa", "Latin America", "Asia"). The page also
  has unrelated <h2>s on /portfolio (testimonials, fund highlights), so we
  whitelist exactly those three region labels and only collect <h3> text
  while one of them is in scope.
- Anchors are present per-card but most have empty href, so anchor-walking
  isn't reliable — we lean on the <h2>/<h3> structure.
- Kampani is INGO-sponsored: 5 Belgian INGOs (Rikolto, Broederlijk Delen,
  Trias, Oxfam-Solidarité, Louvain Coopération) plus SIDI, Alterfin, BIO,
  King Baudouin Foundation, Boerenbond, KU Leuven, CLAC, Solidaridad. Per
  the catalogue the fund's INGO slug is the multi-INGO compound used on
  peer_funds.yml.
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

SCRAPER_NAME = "fund_portfolio_scraper_Kampani"
SOURCE_URL = "https://www.kampani.org/portfolio"
FUND_SLUG = "kampani"
INGO_SLUG = "rikolto-broederlijk-delen-trias-oxfam-solidarite-belgium-louvain-cooperation-sidi"

REGION_HEADERS = {"Africa", "Latin America", "Asia"}

# Hand-cleaned display names (preserve diacritics, expand obvious caps-only
# acronyms only when the page text confirms the long form).
DISPLAY_NAME_OVERRIDES = {
    "esop-vall-e": "Esop Vallée",  # accented form per page text
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

_HEADER_RE = re.compile(r"<h([2-3])[^>]*>(.*?)</h\1>", re.IGNORECASE | re.DOTALL)
_HTML_ENTITIES = {
    "&amp;": "&",
    "&#x27;": "'",
    "&#039;": "'",
    "&nbsp;": " ",
    "&quot;": '"',
}


def _clean_text(s: str) -> str:
    s = re.sub(r"<[^>]+>", "", s).strip()
    for k, v in _HTML_ENTITIES.items():
        s = s.replace(k, v)
    return s


def scrape(run_number: int, output_dir: Path | str) -> int:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    html = r.content.decode("utf-8", errors="replace")

    rows: list[dict] = []
    today = date.today().isoformat()
    region: str | None = None
    seen: set[str] = set()

    for m in _HEADER_RE.finditer(html):
        level = m.group(1)
        text = _clean_text(m.group(2))
        if not text:
            continue
        if level == "2":
            region = text if text in REGION_HEADERS else None
            continue
        if level == "3" and region:
            company_slug = slugify(text)
            display = DISPLAY_NAME_OVERRIDES.get(company_slug, text)
            display_slug = slugify(display)
            if display_slug in seen:
                continue
            seen.add(display_slug)
            rows.append(
                {
                    "Fund Slug": FUND_SLUG,
                    "INGO Slug": INGO_SLUG,
                    "Company Name": display,
                    "Company Slug": display_slug,
                    "Company Website": "",
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
            f"{SCRAPER_NAME}: zero portfolio companies extracted from {SOURCE_URL} "
            f"(region <h2>s or <h3>s may have moved)"
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
