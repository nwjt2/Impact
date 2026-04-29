"""lp_portfolio_scraper_BIOInvest.py

LP-portfolio scraper for BIO (Belgian Investment Company for Developing
Countries) — bio-invest.be.

Approach: prose-press-release pattern. Per `discovery_skip_list.csv`-style
note (added separately), bio-invest.be/en/investments only renders 9 entries
in static HTML — the rest of the portfolio is JS-paginated. So we use the
same multi-source pattern as fund_lp_scraper_IncofinAgRIF: hardcoded
(Investee, year, source URL, needle) tuples drawn from the funds' own
authoritative close press releases / shareholder pages, where BIO is named
as a committed LP.

Investee Type per row: all `fund`. BIO direct-company investments would
require scraping bio-invest.be/en/investments at scale (JS-rendered).

Slug discipline: canonical Fund Slugs match impact_funds.csv where present.
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

SCRAPER_NAME = "lp_portfolio_scraper_BIOInvest"
LP_NAME = "BIO (Belgian Investment Company for Developing Countries)"
# Match the slug already in investors.csv from fund_lp_scraper_FEFISOLII.
LP_SLUG = "bio-belgian-investment-company-for-developing-countries"

# (Investee Name, Investee Slug, Investee Type, Year, Source URL, Verification needle)
BIO_COMMITMENTS: list[tuple[str, str, str, str, str, str]] = [
    (
        "FEFISOL II",
        "fefisol-ii",
        "fund",
        "2022",
        "https://www.eib.org/en/press/all/2022-254-launch-of-the-new-european-solidarity-financing-fund-for-africa-fefisol-ii-with-a-first-closing-of-eur2-5-million-and-a-technical-support-envelope-of-1-million-euros",
        "BIO",
    ),
    (
        "Incofin agRIF",
        "incofin-agrif",
        "fund",
        "2015",
        "https://incofin.com/new-investors-in-agrif-increase-access-to-finance-for-the-agricultural-sector/",
        "BIO",
    ),
    (
        "Kampani",
        "kampani",
        "fund",
        "2015",
        "https://www.kampani.org/about-us",
        # Kampani's about-us logo grid uses Webflow detail links; BIO's tile points
        # to /shareholder/bio---belgian-investment-company-for-developing-countries.
        "bio---belgian-investment-company-for-developing-countries",
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

    for name, slug, kind, year, url, needle in BIO_COMMITMENTS:
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
