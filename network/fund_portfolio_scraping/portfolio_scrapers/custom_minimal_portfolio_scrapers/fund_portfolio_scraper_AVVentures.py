"""fund_portfolio_scraper_AVVentures.py

Scrapes AV Ventures' multi-fund portfolio page and emits one CSV per fund.
Source: https://av-ventures.com/our_funds/

AV Ventures is ACDI/VOCA's fund-management arm. The single page lists
three funds, each with a section heading followed by portfolio anchors:

  - "Impact for Kenya (INK) Fund"  -> av-ventures-ink-kenya
  - "AV Ventures Ghana (AVVG) Fund" -> av-ventures-ghana
  - "Central Asia Impact Fund (CAIF)" -> av-frontiers-caif

Per advice doc lesson 11, this is one bespoke scraper for one bespoke
page. It just happens to write three CSVs because the page already
encodes fund attribution. Future operators: if AV Ventures launches a
4th fund and adds it to the same page, add the heading-to-slug mapping
in `FUND_MAP` below.

Each portfolio entry is `<a href="/portfolio/<slug>/"><img alt="..." />`.
The img alt is unfortunately a logo filename (e.g. "cemes avv 400x159"),
so we derive the canonical name from the URL slug instead.
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

SCRAPER_NAME = "fund_portfolio_scraper_AVVentures"
SOURCE_URL = "https://av-ventures.com/our_funds/"
INGO_SLUG = "acdi-voca"

# Heading substring (case-insensitive) -> fund slug in our catalogue
FUND_MAP = [
    ("Impact for Kenya (INK) Fund",          "av-ventures-ink-kenya"),
    ("AV Ventures Ghana (AVVG) Fund",        "av-ventures-ghana"),
    ("Central Asia Impact Fund (CAIF)",      "av-frontiers-caif"),
]

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


def _name_from_path_slug(path_slug: str) -> str:
    """av-ventures.com/portfolio/<slug>/ — slug like 'maphlix-trust-ghana-limited'."""
    return " ".join(part.capitalize() for part in path_slug.replace("_", "-").split("-"))


def scrape(run_number: int, output_dir: Path | str) -> dict[str, int]:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    html = r.content.decode("utf-8", errors="replace")

    # Find each fund's heading specifically inside an <h3> (Elementor-style).
    # The labels also appear in nav menus, intro paragraphs, and "About the"
    # h4 footers, so a plain text search would give nonsense section bounds.
    headings: list[tuple[str, str, int]] = []  # (display_heading, fund_slug, position)
    for label, slug in FUND_MAP:
        pattern = re.compile(r"<h3[^>]*>\s*" + re.escape(label) + r"\s*</h3>", re.IGNORECASE)
        m = pattern.search(html)
        if not m:
            print(f"  WARN  <h3>{label}</h3> not found on page — skipping {slug}")
            continue
        headings.append((label, slug, m.start()))
    if not headings:
        raise RuntimeError(f"{SCRAPER_NAME}: no expected <h3> fund headings found")
    headings.sort(key=lambda h: h[2])

    # For each heading, collect portfolio anchors until the next heading.
    today = date.today().isoformat()
    counts: dict[str, int] = {}
    seen_global: set[str] = set()  # so the same portco isn't claimed by two funds

    portco_pattern = re.compile(
        r'href="https?://av-ventures\.com/portfolio/([a-z0-9_-]+)/?"',
        re.IGNORECASE,
    )

    for i, (label, fund_slug, start) in enumerate(headings):
        end = headings[i + 1][2] if i + 1 < len(headings) else len(html)
        section = html[start:end]

        rows: list[dict] = []
        for path_slug in dict.fromkeys(portco_pattern.findall(section)):  # dedup, preserve order
            if path_slug in seen_global:
                continue
            seen_global.add(path_slug)
            name = _name_from_path_slug(path_slug)
            rows.append(
                {
                    "Fund Slug": fund_slug,
                    "INGO Slug": INGO_SLUG,
                    "Company Name": name,
                    "Company Slug": slugify(name),
                    "Company Website": f"https://av-ventures.com/portfolio/{path_slug}/",
                    "Round": "unknown",
                    "Round Date": "",
                    "Lead": "unknown",
                    "Source URL": SOURCE_URL,
                    "Source Date": today,
                    "Scraping Method Used": SCRAPER_NAME,
                }
            )

        out_path = Path(output_dir) / f"run_{run_number}" / f"{fund_slug}.csv"
        write_rows(out_path, OUTPUT_HEADERS, rows)
        counts[fund_slug] = len(rows)

    return counts


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "network" / "fund_portfolio_scraping" / "individual_fund_portfolios"),
    )
    args = parser.parse_args()
    counts = scrape(args.run, args.output_dir)
    for slug, n in counts.items():
        print(f"{SCRAPER_NAME}: {slug} -> {n} rows")


if __name__ == "__main__":
    main()
