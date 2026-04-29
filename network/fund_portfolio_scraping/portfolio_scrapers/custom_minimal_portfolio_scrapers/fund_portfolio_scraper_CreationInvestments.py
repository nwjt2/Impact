"""fund_portfolio_scraper_CreationInvestments.py

Scrapes Creation Investments' portfolio (umbrella) and attributes it to
Fund IV (creation-investments-iv) — the most recent vintage in our catalogue.

Source: https://www.creationinvestments.com/portfolio/

Notes for next operator:
- Squarespace site, fully static HTML. The portfolio page lists ~25 portfolio
  company anchors as outbound links to each company's website.
- Each anchor's text is a "<Country> | <Sector>" label, NOT the company name.
  The company name must be derived from the URL hostname (e.g.
  https://fusionmicrofinance.com/ → "Fusion Microfinance"). This is the same
  pattern as Mercy Corps Ventures Squarespace.
- This is an UMBRELLA scrape attributed to `creation-investments-iv`. Creation
  manages multiple Social Venture Funds (I, II, III, IV) — same umbrella-
  attribution caveat as MCV/AgDevCo/KRIF/Goodwell.
"""
from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import httpx
from selectolax.parser import HTMLParser

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "fund_portfolio_scraper_CreationInvestments"
SOURCE_URL = "https://www.creationinvestments.com/portfolio/"
FUND_SLUG = "creation-investments-iv"
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

# Anchor text labels are "<Country> | <Sector>". Anchors with this exact
# pipe-separated structure are the portco anchors we want.
_LABEL_RE = re.compile(r"^[A-Z][A-Za-z &]+ \| [A-Za-z0-9 &]+$")

# Domains to exclude from the company-anchor sweep — site nav, social,
# Creation's own subdomains, etc.
_EXCLUDED_HOST_SUFFIXES = (
    "creationinvestments.com",
    "linkedin.com",
    "youtube.com",
    "twitter.com",
    "x.com",
    "facebook.com",
    "instagram.com",
)


def _name_from_hostname(host: str) -> str:
    """fusionmicrofinance.com -> 'Fusionmicrofinance'.

    We can't reliably split run-together brand names without a curated dictionary.
    The combine step's PRESERVE_FIELDS keeps operator edits to Company Name
    across runs, so cleanup happens once and sticks. (Same pattern as MCV.)
    """
    # Remove www., subdomain prefix where unambiguous, common TLDs.
    host = host.removeprefix("www.")
    # Strip TLD (last 1-2 segments). Keep brand only.
    parts = host.split(".")
    if len(parts) >= 2:
        brand = parts[0]
    else:
        brand = host
    # Capitalize first letter; leave rest as-is so the operator can clean up
    # multi-word brand names manually.
    return brand[:1].upper() + brand[1:]


def scrape(run_number: int, output_dir: Path | str) -> int:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    tree = HTMLParser(r.content.decode("utf-8", errors="replace"))

    today = date.today().isoformat()
    rows: list[dict] = []
    seen_hosts: set[str] = set()

    for a in tree.css("a"):
        href = (a.attributes.get("href") or "").strip()
        text = (a.text() or "").strip()
        if not href.startswith("http"):
            continue
        if not _LABEL_RE.match(text):
            continue

        try:
            parsed = urlparse(href)
        except Exception:
            continue
        host = parsed.netloc.lower()
        if not host:
            continue
        if any(host == suf or host.endswith("." + suf) for suf in _EXCLUDED_HOST_SUFFIXES):
            continue
        # Dedupe by host (Creation's page lists a few companies twice with
        # different sector labels — collapse to one row).
        if host in seen_hosts:
            continue
        seen_hosts.add(host)

        company_name = _name_from_hostname(host)
        company_slug = slugify(company_name)
        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "Company Name": company_name,
                "Company Slug": company_slug,
                "Company Website": href,
                "Round": "unknown",
                "Round Date": "",
                "Lead": "unknown",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if len(rows) < 15:
        raise RuntimeError(
            f"{SCRAPER_NAME}: only found {len(rows)} portcos at {SOURCE_URL} "
            f"(expected ~22) — page structure may have changed."
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
