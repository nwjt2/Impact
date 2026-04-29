"""lp_portfolio_scraper_EIB.py

LP-portfolio scraper for the European Investment Bank (EIB).

Approach: prose-press-release pattern (same as fund_lp_scraper_FEFISOLII /
fund_lp_scraper_IncofinAgRIF). For each known EIB fund commitment, we hold a
tuple (Fund Name, Fund Slug, Year, Source URL, Verification Substring). On each
run we fetch each unique source URL once, verify the substring is present, and
emit one row per confirmed commitment.

Why this pattern: EIB's project register at www.eib.org/en/projects/all/ is
huge (10s of thousands) and not easily filtered to "EIB-as-LP-into-fund"
commitments only. The fund-side primary sources we already scrape for
fund_lps.csv (FEFISOL II, EcoEnterprises III, Incofin agRIF, WWB II launch
press releases) are themselves authoritative confirmations of EIB's role.
We re-emit them here so the *EIB* node densifies — without a scraper of this
shape, EIB only appears as an LP edge into funds we have other LP scrapers
for. With this scraper, EIB-known-LP-positions surface even when the fund
itself doesn't have a fund_lp_scraper.

Investee Type per row: all `fund` (EIB direct-company investments — green-bond
underwritings, etc. — are not in scope here).

Slug discipline: the canonical Fund Slug values must match
network/catalogue/impact_funds.csv. Where they don't (e.g. an EIB-financed
fund that's not yet in the catalogue), the combine step's `_update_funds`
inserts the fund as Pipeline Status=pending_onboard.
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

SCRAPER_NAME = "lp_portfolio_scraper_EIB"
LP_NAME = "European Investment Bank"
LP_SLUG = "european-investment-bank"

# (Investee Name, Investee Slug, Investee Type, Year, Source URL, Verification needle)
EIB_COMMITMENTS: list[tuple[str, str, str, str, str, str]] = [
    (
        "FEFISOL II",
        "fefisol-ii",
        "fund",
        "2022",
        "https://www.eib.org/en/press/all/2022-254-launch-of-the-new-european-solidarity-financing-fund-for-africa-fefisol-ii-with-a-first-closing-of-eur2-5-million-and-a-technical-support-envelope-of-1-million-euros",
        "FEFISOL II",
    ),
    (
        "EcoEnterprises Fund III",
        "ecoenterprises-iii",
        "fund",
        "2018",
        "https://www.eib.org/en/products/equity/funds/ecoenterprises-fund-iii",
        "EcoEnterprises",
    ),
    (
        "Incofin agRIF",
        "incofin-agrif",
        "fund",
        "2015",
        "https://incofin.com/new-investors-in-agrif-increase-access-to-finance-for-the-agricultural-sector/",
        "European Investment Bank",
    ),
    (
        "WWB Capital Partners Fund II",
        "wwb-capital-partners-fund-ii",
        "fund",
        "2022",
        "https://www.womensworldbanking.org/insights/womens-world-banking-asset-management-closes-second-fund-for-financial-inclusion-at-103m/",
        "European Investment Bank",
    ),
    (
        "FEFISOL I (Fonds Européen de Financement Solidaire pour l'Afrique)",
        "fefisol-i",
        "fund",
        "2011",
        "https://www.eib.org/en/press/all/2011-116-africas-first-microfinance-fund-for-sustainable-farmers-launched",
        "FEFISOL",
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

    # Cache fetches per URL.
    html_by_url: dict[str, str] = {}
    rows: list[dict] = []
    missing: list[str] = []

    for name, slug, kind, year, url, needle in EIB_COMMITMENTS:
        if url not in html_by_url:
            r = httpx.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=30.0,
                follow_redirects=True,
            )
            r.raise_for_status()
            html_by_url[url] = r.content.decode("utf-8", errors="replace")
        html = html_by_url[url]
        if needle not in html:
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
            f"in source (page may have been edited): {missing}"
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
