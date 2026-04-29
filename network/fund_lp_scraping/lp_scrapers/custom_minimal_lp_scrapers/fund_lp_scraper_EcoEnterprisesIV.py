"""fund_lp_scraper_EcoEnterprisesIV.py

Scrapes the named LPs of EcoEnterprises Fund IV (the 2025 successor
vintage of EcoEnterprises Fund III) from the Soros Economic Development
Fund (SEDF) press release announcing its $7.5m commitment.

Source: https://www.soroseconomicdevelopmentfund.org/newsroom/soros-economic-development-fund-backs-women-led-ecoenterprises-fund-iv-to-drive-climate-action-and-inclusive-development-in-latin-america
Press release date: 2025-12-08

Verbatim attribution sentence (foundation tie):
  "The Soros Economic Development Fund (SEDF), the impact investment arm
   of the Open Society Foundations, is investing $7.5 million in
   EcoEnterprises Fund IV."

Verbatim coalition sentence (other named LPs):
  "FinDev Canada, FMO, IDB Invest, International Finance Corporation,
   Proparco, JICA, SIFEM, Visa Foundation, and Wire Group"

Canonicalisation choices:
  - "Open Society Foundations" used (not "SEDF") so the LP collapses to
    the parent foundation entry and gets classified as `foundation` by
    combine_fund_lps._classify_investor_type. SEDF is the impact arm —
    surfacing the parent is what /add-foundation-edges is meant to do.
  - "Japan International Cooperation Agency" canonical to match the
    convention in fund_lp_scraper_WWBFundII.py.
  - "SIFEM" canonical to match fund_lp_scraper_IncofinAgRIF.py.
  - Other names left as their press-release short forms.

Per advice doc lesson 11: one custom scraper per fund. This is bespoke
to the SEDF press-release prose — it does not generalise.
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

SCRAPER_NAME = "fund_lp_scraper_EcoEnterprisesIV"
SOURCE_URL = (
    "https://www.soroseconomicdevelopmentfund.org/newsroom/"
    "soros-economic-development-fund-backs-women-led-ecoenterprises-fund-iv-"
    "to-drive-climate-action-and-inclusive-development-in-latin-america"
)
FUND_SLUG = "ecoenterprises-iv"
INGO_SLUG = ""
COMMITMENT_YEAR = "2025"

# (canonical LP name, substring needle to verify in source HTML)
ECOE_IV_LPS: list[tuple[str, str]] = [
    ("Open Society Foundations", "Open Society Foundations"),
    ("FinDev Canada", "FinDev Canada"),
    ("FMO", "FMO"),
    ("IDB Invest", "IDB Invest"),
    ("International Finance Corporation", "International Finance Corporation"),
    ("Proparco", "Proparco"),
    ("Japan International Cooperation Agency", "JICA"),
    ("SIFEM", "SIFEM"),
    ("Visa Foundation", "Visa Foundation"),
    ("Wire Group", "Wire Group"),
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

    for canonical, needle in ECOE_IV_LPS:
        if needle not in html:
            missing.append(f"{canonical} (needle {needle!r})")
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
