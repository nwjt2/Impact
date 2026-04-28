"""fund_portfolio_scraper_CIVentures.py

Scrapes Conservation International Ventures' portfolio page.
Source: https://www.conservation.org/ci-ventures

Page structure (Sanity-CMS-rendered Next.js):
- Three H4 region headings: "Africa", "Oceans", "Americas".
- Within each region, a grid of <a href="...conservation.org/ci-ventures/<slug>">
  card links. Each anchor contains a <span class="button-text ...">NAME...</span>
  with the company name (the H4 child of the anchor is unicode-obfuscated and
  unreadable, but the button-text span carries the same name in plain text).
- After the visible name there's a trailing "↗" link-arrow plus zero-width
  Unicode characters (U+200B-U+200D, U+FEFF, U+FE0F, U+2061-U+2064 and friends)
  used as text-tracking obfuscation. Strip those before emitting.

Per advice doc lesson 11: bespoke scraper for one bespoke page.
"""
from __future__ import annotations

import re
import sys
import unicodedata
from datetime import date
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "fund_portfolio_scraper_CIVentures"
SOURCE_URL = "https://www.conservation.org/ci-ventures"
FUND_SLUG = "ci-ventures"
INGO_SLUG = "conservation-international"

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

# Each portco card is an <a> wrapping a <span class="button-text...">NAME</span>.
_ANCHOR_RE = re.compile(
    r'<a\s+href="https?://www\.conservation\.org/ci-ventures/([a-z0-9][a-z0-9-]*)"[^>]*>'
    r'(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)
_BUTTON_RE = re.compile(
    r'<span[^>]*class="button-text[^"]*"[^>]*>(.*?)</span>',
    re.DOTALL | re.IGNORECASE,
)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HTML_ENTITY_AMP_RE = re.compile(r"&amp;")
_HTML_ENTITY_APOS_RE = re.compile(r"&#x27;|&#39;|&apos;")


def _name_from_slug(path_slug: str) -> str:
    return " ".join(part.capitalize() for part in path_slug.replace("_", "-").split("-"))


def _clean_name(raw: str) -> str:
    # Strip nested HTML.
    txt = _HTML_TAG_RE.sub("", raw)
    txt = _HTML_ENTITY_AMP_RE.sub("&", txt)
    txt = _HTML_ENTITY_APOS_RE.sub("'", txt)
    # Drop anything from the link arrow onward — also catches '?' surrogate
    # placeholders that decoded from the arrow when the response wasn't UTF-8.
    txt = re.split(r"[→↗➜?]", txt, maxsplit=1)[0]
    # Strip zero-width / formatting / non-printable Unicode (Cf category).
    txt = "".join(ch for ch in txt if unicodedata.category(ch) not in ("Cf", "Cc"))
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def scrape(run_number: int, output_dir: Path | str) -> int:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    html = r.content.decode("utf-8", errors="replace")

    today = date.today().isoformat()
    rows: list[dict] = []
    seen: set[str] = set()

    for m in _ANCHOR_RE.finditer(html):
        path_slug = m.group(1).lower()
        if path_slug in seen:
            continue
        body = m.group(2)
        btn = _BUTTON_RE.search(body)
        if not btn:
            # Anchors without a button-text span are non-portco links (nav,
            # report download, etc.). Skip silently.
            continue
        name = _clean_name(btn.group(1))
        if not name:
            name = _name_from_slug(path_slug)
        seen.add(path_slug)

        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "Company Name": name,
                "Company Slug": slugify(name),
                "Company Website": f"https://www.conservation.org/ci-ventures/{path_slug}",
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
        default=str(
            REPO_ROOT
            / "network"
            / "fund_portfolio_scraping"
            / "individual_fund_portfolios"
        ),
    )
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{FUND_SLUG}.csv")


if __name__ == "__main__":
    main()
