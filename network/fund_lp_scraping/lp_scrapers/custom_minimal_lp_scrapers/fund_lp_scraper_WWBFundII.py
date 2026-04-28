"""fund_lp_scraper_WWBFundII.py

Scrapes LP roster of WWB Capital Partners Fund II from the Women's World
Banking final-close announcement.

Source: https://www.womensworldbanking.org/insights/womens-world-banking-asset-management-closes-second-fund-for-financial-inclusion-at-103m/

Approach: hand-curated list of LP names, each verified by substring match
against the fetched HTML. The press release prose lists 12 named LPs,
including the EU and BMZ as anchors and a follow-on roster of DFIs and
foundations.
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

SCRAPER_NAME = "fund_lp_scraper_WWBFundII"
SOURCE_URL = (
    "https://www.womensworldbanking.org/insights/"
    "womens-world-banking-asset-management-closes-second-fund-for-financial-"
    "inclusion-at-103m/"
)
FUND_SLUG = "wwb-capital-partners-fund-ii"
INGO_SLUG = "women-s-world-banking"
COMMITMENT_YEAR = "2022"  # final close March 2022 at $103m

# (canonical LP name, substring to verify in source HTML)
WWBII_LPs: list[tuple[str, str]] = [
    ("European Union", "European Union"),
    ("German Federal Ministry for Economic Cooperation and Development (BMZ)", "BMZ"),
    ("KfW", "KfW"),
    ("U.S. International Development Finance Corporation", "Development Finance Corporation"),
    ("European Investment Bank", "European Investment Bank"),
    ("Japan International Cooperation Agency", "Japan International Cooperation Agency"),
    ("Soros Economic Development Fund", "Soros Economic Development Fund"),
    ("Sasakawa Peace Foundation", "Sasakawa Peace Foundation"),
    ("MEDA", "MEDA"),
    ("Dreilinden", "Dreilinden"),
    ("Ceniarth", "Ceniarth"),
    ("USAID", "USAID"),
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

    for canonical, needle in WWBII_LPs:
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
