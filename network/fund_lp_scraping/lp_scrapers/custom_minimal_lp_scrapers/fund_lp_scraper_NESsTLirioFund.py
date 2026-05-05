"""fund_lp_scraper_NESsTLirioFund.py

Scrapes the LP edge for the NESsT Lirio Fund from Sorenson Impact
Foundation's own press release announcing its PRI commitment.

Source: https://sorensonimpactfoundation.org/sorenson-impact-foundation-announces-program-related-investment-into-nesst-lirio-fund-to-support-impact-financing-in-the-andes-and-amazon/

Verbatim primary-source quote:
    "Sorenson Impact Foundation is thrilled to announce its
     Program-Related Investment into NESsT Lirio Fund, a groundbreaking
     debt fund targeting the 'missing middle' in the Andes-Amazon
     region of Latin America."
Publication date: 23 January 2024.

Other LPs / supporters of the fund (IDB, Kiva, Ceniarth, CapShift, DF
Impact Capital, SK2 Fund) appear in NESsT's own February 2026 fund
update at https://www.nesst.org/nesst/2026/2/5/the-nesst-lirio-fund-disburses-over-20-million-usd-...
but are NOT named in the Sorenson press release. This scraper draws
only from the Sorenson source, so it emits a single LP row. Per advice
doc lesson 11, future operators may add a second source if they want
to broaden coverage — extend the LP list here, do not write a generic
scraper.
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

SCRAPER_NAME = "fund_lp_scraper_NESsTLirioFund"
SOURCE_URL = (
    "https://sorensonimpactfoundation.org/"
    "sorenson-impact-foundation-announces-program-related-investment-"
    "into-nesst-lirio-fund-to-support-impact-financing-in-the-"
    "andes-and-amazon/"
)
FUND_SLUG = "nesst-lirio-fund"
INGO_SLUG = ""  # non-INGO fund; manager NESsT is itself a 501(c)(3) but not in our INGO catalogue
COMMITMENT_YEAR = "2024"  # Sorenson PRI announced 23 January 2024

# Each tuple: (canonical name to write, substring to verify in source)
NESST_LIRIO_LPs: list[tuple[str, str]] = [
    ("Sorenson Impact Foundation", "Sorenson Impact Foundation"),
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

    for canonical, needle in NESST_LIRIO_LPs:
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
