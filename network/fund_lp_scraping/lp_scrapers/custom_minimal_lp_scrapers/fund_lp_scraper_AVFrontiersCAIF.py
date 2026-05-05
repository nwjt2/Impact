"""fund_lp_scraper_AVFrontiersCAIF.py

Scrapes the LP roster of AV Frontiers Central Asia Investment Fund (CAIF)
from ACDI/VOCA's launch press release. The 2020 announcement names KMF-Demeu
as the anchor capital provider raising the fund's first $2m.

Source: https://www.acdivoca.org/2020/04/acdi-voca-launches-central-asia-impact-fund-for-small-and-growing-businesses/

Per advice doc lesson 11: one custom scraper per fund.

Excluded by editorial discipline:
- ACDI/VOCA itself ("anchored with capital from ... ACDI/VOCA" on av-ventures.com).
  ACDI/VOCA is the INGO sponsor — captured via INGO Slug rather than an LP edge,
  consistent with KRIF/Kiva precedent.
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

SCRAPER_NAME = "fund_lp_scraper_AVFrontiersCAIF"
SOURCE_URL = (
    "https://www.acdivoca.org/2020/04/"
    "acdi-voca-launches-central-asia-impact-fund-for-small-and-growing-businesses/"
)
FUND_SLUG = "av-frontiers-caif"
INGO_SLUG = "acdi-voca"
COMMITMENT_YEAR = "2020"  # fund launch / initial capital raise

# Each tuple: (canonical name to write, substring to verify in source)
LPs: list[tuple[str, str]] = [
    ("KMF-Demeu", "KMF-Demeu"),
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

    for canonical, needle in LPs:
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
