"""lp_portfolio_scraper_BambooCP.py

LP-portfolio scraper for Bamboo Capital Partners.

Bamboo CP is a fund manager / GP that anchors and sub-advises multiple
INGO-adjacent vehicles. Where Bamboo commits GP-side capital or is the named
manager-anchor at fund launch, that is a real LP-style edge for the network's
warm-lead signal.

Approach: prose-press-release pattern using Bamboo's own press archive.
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

SCRAPER_NAME = "lp_portfolio_scraper_BambooCP"
LP_NAME = "Bamboo Capital Partners"
LP_SLUG = "bamboo-capital-partners"

BAMBOO_COMMITMENTS: list[tuple[str, str, str, str, str, str]] = [
    (
        "CARE-SheTrades Impact Fund",
        "care-shetrades",
        "fund",
        "2020",
        "https://bamboocp.com/the-international-trade-centre-care-enterprises-and-bamboo-capital-partners-join-forces-to-boost-gender-equality-with-the-care-shetrades-impact-fund/",
        "Bamboo Capital Partners",
    ),
    (
        "Bamboo BLOC Smart Africa Impact Fund",
        "bamboo-bloc-smart-africa",
        "fund",
        "2021",
        "https://bamboocp.com/bloc-smart-africa-impact-fund-launched-with-governments-of-luxembourg-and-cote-divoire-as-anchor-sponsors/",
        "Bamboo Capital Partners",
    ),
]

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


def scrape(run_number: int, output_dir: Path | str) -> int:
    today = date.today().isoformat()
    html_by_url: dict[str, str] = {}
    rows: list[dict] = []
    missing: list[str] = []

    for name, slug, kind, year, url, needle in BAMBOO_COMMITMENTS:
        if url not in html_by_url:
            r = httpx.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=30.0,
                follow_redirects=True,
            )
            r.raise_for_status()
            html_by_url[url] = r.content.decode("utf-8", errors="replace")
        if needle not in html_by_url[url]:
            missing.append(f"{name!r} needle {needle!r} not in {url}")
            continue
        rows.append(
            {
                "LP Slug": LP_SLUG,
                "Investee Name": name,
                "Investee Slug": slug,
                "Investee Type": kind,
                "Commitment Year": year,
                "Source URL": url,
                "Source Date": today,
                "Confidence": "confirmed",
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if missing:
        raise RuntimeError(
            f"{SCRAPER_NAME}: {len(missing)} expected commitment(s) not found "
            f"in source: {missing}"
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
