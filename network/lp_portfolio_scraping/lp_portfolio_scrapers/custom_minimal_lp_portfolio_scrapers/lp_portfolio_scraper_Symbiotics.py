"""lp_portfolio_scraper_Symbiotics.py

LP-portfolio scraper for Symbiotics.

Symbiotics is a Geneva-based investment manager / sub-advisor for multiple
INGO microfinance and SMB vehicles. Where it is the named manager-anchor or
sub-advisor at fund launch, that is a real LP-style edge for the network.

Approach: prose-press-release pattern. Each commitment is verified against
the fund's own launch / partnership announcement.
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

SCRAPER_NAME = "lp_portfolio_scraper_Symbiotics"
LP_NAME = "Symbiotics"
LP_SLUG = "symbiotics"

SYMBIOTICS_COMMITMENTS: list[tuple[str, str, str, str, str, str]] = [
    (
        "Small Enterprise Impact Investing Fund (SEIIF)",
        "oxfam-seiif",
        "fund",
        "2012",
        "https://www.thirdsector.co.uk/oxfam-launches-fund-impact-investments-developing-world/finance/article/1154243",
        "Symbiotics",
    ),
    (
        "MicroBuild Fund 2",
        "microbuild-fund-2",
        "fund",
        "2025",
        "https://www.habitat.org/newsroom/2025/habitat-humanity-international-and-symbiotics-join-forces-accelerate-and-scale",
        "Symbiotics",
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

    for name, slug, kind, year, url, needle in SYMBIOTICS_COMMITMENTS:
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
