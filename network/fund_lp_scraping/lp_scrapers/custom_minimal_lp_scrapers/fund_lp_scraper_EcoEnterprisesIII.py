"""fund_lp_scraper_EcoEnterprisesIII.py

Scrapes the named LPs of EcoEnterprises Partners III, LP from three
distinct primary-source DFI/MDB project pages — each LP's commitment is
disclosed on its own institution's site (EcoEnterprises Fund itself does
not publish an LP roster; ecoenterprisesfund.com is a single-pager).

Sources (three distinct DFI/IGO project pages):
  1. https://www.eib.org/en/products/equity/funds/ecoenterprises-fund-iii
     -- EIB project sheet; discloses USD 15m EIB commitment.
  2. https://common-fund.org/ecoenterprises-fund-iii-0
     -- Common Fund for Commodities project page; discloses CFC's USD 1m
        commitment as shareholder of EcoE III at first close.
  3. https://www.2xchallenge.org/new-blog/2019/9/16/investing-with-purpose-how-ecoenterprises-iii-became-findev-canada-first-2x-challenge-investment
     -- 2X Challenge blog post; discloses FinDev Canada's investment
        announced May 2019 as its first 2X Challenge investment.

Approach: same prose-pattern as KRIF / FEFISOL II / etc., extended to
multiple source URLs (per-row Source URL). 3 LPs from primary sources.

DEVIATION FROM SINGLE-SOURCE PRECEDENT: same as fund_lp_scraper_BambooBLOC
and fund_lp_scraper_IncofinAgRIF. The schema's per-row Source URL is the
right fit when no single primary source names all the LPs of a fund —
honesty discipline trumps the convenience of a single fetch.

Other LPs (FMO, Hivos Triodos Fund, Oikocredit, JP Morgan, The Nature
Conservancy, MIF/Fomin, Calvert Foundation, GEF, ImpactAssets, Mosaico,
Talgra, family foundations) are mentioned in secondary sources as having
participated in EcoE Funds I/II/III collectively but per-fund attribution
isn't disclosed in primary sources for Fund III specifically — excluded
per honesty discipline.
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

SCRAPER_NAME = "fund_lp_scraper_EcoEnterprisesIII"
FUND_SLUG = "ecoenterprises-iii"
INGO_SLUG = ""

EIB_URL = "https://www.eib.org/en/products/equity/funds/ecoenterprises-fund-iii"
CFC_URL = "https://common-fund.org/ecoenterprises-fund-iii-0"
TWOX_URL = (
    "https://www.2xchallenge.org/new-blog/2019/9/16/"
    "investing-with-purpose-how-ecoenterprises-iii-became-"
    "findev-canada-first-2x-challenge-investment"
)

# (canonical LP name, substring needle, source URL, commitment year)
ECOE_III_LPS: list[tuple[str, str, str, str]] = [
    # Canonical "European Investment Bank" matches existing FEFISOL II
    # scraper so the EIB slug collapses across funds.
    ("European Investment Bank", "EcoEnterprises", EIB_URL, "2018"),
    ("Common Fund for Commodities", "Common Fund for Commodities", CFC_URL, "2018"),
    ("FinDev Canada", "FinDev Canada", TWOX_URL, "2019"),
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
    today = date.today().isoformat()

    # Fetch each unique source URL once.
    source_html: dict[str, str] = {}
    with httpx.Client(
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        for url in {EIB_URL, CFC_URL, TWOX_URL}:
            r = client.get(url)
            r.raise_for_status()
            source_html[url] = r.content.decode("utf-8", errors="replace")

    rows: list[dict] = []
    missing: list[str] = []

    for canonical, needle, src_url, year in ECOE_III_LPS:
        html = source_html[src_url]
        if needle not in html:
            missing.append(f"{canonical} (needle {needle!r} in {src_url[-40:]})")
            continue
        rows.append(
            {
                "Fund Slug": FUND_SLUG,
                "INGO Slug": INGO_SLUG,
                "LP Name": canonical,
                "LP Slug": slugify(canonical),
                "Commitment Year": year,
                "Source URL": src_url,
                "Source Date": today,
                "Confidence": "confirmed",
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if missing:
        raise RuntimeError(
            f"{SCRAPER_NAME}: {len(missing)} expected LP(s) not found in source(s) "
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
