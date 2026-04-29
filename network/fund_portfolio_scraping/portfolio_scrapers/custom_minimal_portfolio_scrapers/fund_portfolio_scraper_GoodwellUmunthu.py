"""fund_portfolio_scraper_GoodwellUmunthu.py

Scrapes Goodwell Investments' portfolio (umbrella) and attributes it to the
uMunthu II fund slug.

Source: https://goodwell.nl/portfolio/

Notes for next operator:
- WordPress site, fully static HTML. /portfolio/ lists 16 portfolio company
  detail-page anchors of shape /portfolio/<slug>/. Cleanest first-scraper
  candidate after AgDevCo.
- One slug is a placeholder ("portfolio-dummy-1") whose detail page <title>
  reads "Chicoa Fish Farm | Goodwell Investments". We resolve that name from
  the per-portco detail page <title> for ALL portcos so the displayed name is
  the curated marketing name (e.g. "Powering agent banking in Uganda with ABC"
  detail page resolves to title "Oradian | Goodwell Investments" — we strip
  the " | Goodwell Investments" suffix). Fallback to the slug-derived name.
- This is an UMBRELLA scrape attributed to `goodwell-umunthu-ii`. Goodwell
  manages multiple vehicles (uMunthu, uMunthu II, Goodwell VI, IYBA WE4A,
  Growth Capital Fund, Feeder Funds, GWAMDC, AGIMDC, AGIMDC II) — same
  umbrella-attribution caveat as MCV/AgDevCo/KRIF. Per-fund attribution is a
  future enrichment.
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

SCRAPER_NAME = "fund_portfolio_scraper_GoodwellUmunthu"
SOURCE_URL = "https://goodwell.nl/portfolio/"
FUND_SLUG = "goodwell-umunthu-ii"
INGO_SLUG = ""  # non-INGO

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

_DETAIL_PATH_RE = re.compile(r"^https://goodwell\.nl/portfolio/([a-z0-9-]+)/?$")
_TITLE_SUFFIX_RE = re.compile(r"\s*\|\s*Goodwell Investments\s*$", flags=re.I)


def _name_from_slug(slug: str) -> str:
    """Slug like 'good-nature-agro' -> 'Good Nature Agro'. Last-resort fallback."""
    return " ".join(part.capitalize() for part in slug.split("-"))


def _name_from_detail(client: httpx.Client, detail_url: str, slug: str) -> str:
    """Fetch detail page, extract <title>, strip ' | Goodwell Investments' suffix.

    Falls back to a slug-derived name on any error so a single dead link
    can't fail the whole scrape.
    """
    try:
        r = client.get(detail_url)
        r.raise_for_status()
        tree = HTMLParser(r.content.decode("utf-8", errors="replace"))
        t = tree.css_first("title")
        if t:
            raw = (t.text() or "").strip()
            cleaned = _TITLE_SUFFIX_RE.sub("", raw).strip()
            if cleaned:
                return cleaned
    except Exception:
        pass
    return _name_from_slug(slug)


def scrape(run_number: int, output_dir: Path | str) -> int:
    today = date.today().isoformat()
    rows: list[dict] = []

    with httpx.Client(
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        r = client.get(SOURCE_URL)
        r.raise_for_status()
        tree = HTMLParser(r.content.decode("utf-8", errors="replace"))

        seen_slugs: set[str] = set()
        for a in tree.css("a"):
            href = (a.attributes.get("href") or "").strip()
            m = _DETAIL_PATH_RE.match(href)
            if not m:
                continue
            path_slug = m.group(1)
            if path_slug in seen_slugs:
                continue
            seen_slugs.add(path_slug)

            detail_url = href if href.endswith("/") else href + "/"
            company_name = _name_from_detail(client, detail_url, path_slug)
            company_slug = slugify(company_name)
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

    if len(rows) < 10:
        # Page should have ~16 portcos; less than 10 means the structure changed.
        raise RuntimeError(
            f"{SCRAPER_NAME}: only found {len(rows)} portcos at {SOURCE_URL} "
            f"(expected ~16) — page structure may have changed."
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
