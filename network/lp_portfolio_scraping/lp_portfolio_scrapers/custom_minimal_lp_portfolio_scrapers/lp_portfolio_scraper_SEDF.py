"""lp_portfolio_scraper_SEDF.py

LP-portfolio scraper for the Soros Economic Development Fund (SEDF), the
impact-investing arm of Open Society Foundations.

Approach: prose-press-release pattern. SEDF does not publish a structured
portfolio page; opensocietyfoundations.org's SEDF program page redirects to
generic OSF themes. So we use the multi-source pattern: each SEDF commitment
is verified via the *fund-side* close press release that names SEDF.

Investee Type per row: all `fund` (SEDF direct-company investments aren't
publicly aggregated in a clean primary source).
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

SCRAPER_NAME = "lp_portfolio_scraper_SEDF"
LP_NAME = "Soros Economic Development Fund"
LP_SLUG = "soros-economic-development-fund"

# (Investee Name, Investee Slug, Investee Type, Year, Source URL, Verification needle)
SEDF_COMMITMENTS: list[tuple[str, str, str, str, str, str]] = [
    (
        "Kiva Refugee Investment Fund (KRIF)",
        "kiva-refugee-investment-fund",
        "fund",
        "2021",
        "https://www.prnewswire.com/news-releases/kiva-capital-management-announces-final-close-of-kiva-refugee-investment-fund-to-scale-lending-to-fragile-communities-globally-301264464.html",
        "Soros Economic Development Fund",
    ),
    (
        "WWB Capital Partners Fund II",
        "wwb-capital-partners-fund-ii",
        "fund",
        "2022",
        "https://www.womensworldbanking.org/insights/womens-world-banking-asset-management-closes-second-fund-for-financial-inclusion-at-103m/",
        "Soros Economic Development Fund",
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

    for name, slug, kind, year, url, needle in SEDF_COMMITMENTS:
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
