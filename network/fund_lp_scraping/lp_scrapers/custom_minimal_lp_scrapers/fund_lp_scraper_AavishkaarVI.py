"""fund_lp_scraper_AavishkaarVI.py

Scrapes the named LPs of the Aavishkaar Bharat Fund (Aavishkaar India Fund VI)
from the November 2017 PR Newswire press release announcing first close at
INR 594 Crore (~US$92 million).

Source: https://www.prnewswire.com/in/news-releases/aavishkaar-announces-the-first-close-of-its-6th-fund-the-aavishkaar-bharat-fund-abf-at-inr-594-crore-us-92-million-658181123.html

Approach: hand-curated (canonical_name, needle) tuples — same prose-pattern
as KRIF / FEFISOL II. The PR explicitly names the four anchor LPs in the
sentence "The Aavishkaar Bharat Fund is a SEBI registered AIF anchored by
SIDBI, CDC Group Plc, Munjal Family Office (Hero Corp) and TIAA". The
fund went on to a final close at ~$200m by 2023; LPs added between first
and final close are not consistently in primary sources, so this scraper
emits only the original four anchors.

Note: Aavishkaar's fund vehicle in the catalogue carries the slug
`aavishkaar-india-vi`, which corresponds to the "Aavishkaar Bharat Fund" /
Aavishkaar India Fund VI marketing name (the same fund — Aavishkaar
sometimes calls it ABF, sometimes Fund VI in their materials).
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "fund_lp_scraper_AavishkaarVI"
SOURCE_URL = (
    "https://www.prnewswire.com/in/news-releases/"
    "aavishkaar-announces-the-first-close-of-its-6th-fund-the-aavishkaar-"
    "bharat-fund-abf-at-inr-594-crore-us-92-million-658181123.html"
)
FUND_SLUG = "aavishkaar-india-vi"
INGO_SLUG = ""  # non-INGO
COMMITMENT_YEAR = "2017"  # first close 17 Nov 2017

# (canonical LP name, substring needle to verify in source HTML)
ABF_LPS: list[tuple[str, str]] = [
    ("Small Industries Development Bank of India (SIDBI)", "SIDBI"),
    ("CDC Group Plc", "CDC Group"),
    ("Munjal Family Office (Hero Enterprises)", "Munjal Family Office"),
    ("TIAA", "TIAA"),
]

OUTPUT_HEADERS = [
    "Fund Slug",
    "INGO Slug",
    "LP Name",
    "LP Slug",
    "Commitment Year",
    "Source URL",
    "Source Date",
    "Confidence",
    "Scraping Method Used",
]


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
    missing: list[str] = []

    for canonical, needle in ABF_LPS:
        if needle not in html:
            missing.append(canonical)
            continue
        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "LP Name": canonical,
                "LP Slug": slugify(canonical),
                "Commitment Year": COMMITMENT_YEAR,
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Confidence": "confirmed",
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if missing:
        raise RuntimeError(
            f"{SCRAPER_NAME}: {len(missing)} expected LP(s) not found in source "
            f"(page may have been edited): {missing}"
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
        default=str(REPO_ROOT / "network" / "fund_lp_scraping" / "individual_fund_lps"),
    )
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{FUND_SLUG}.csv")


if __name__ == "__main__":
    main()
