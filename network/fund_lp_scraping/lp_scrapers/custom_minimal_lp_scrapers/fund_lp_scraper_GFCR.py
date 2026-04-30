"""fund_lp_scraper_GFCR.py

Scrapes named LP / co-initiator / philanthropic-contributor roster of the
Global Fund for Coral Reefs (GFCR) — a UN-administered blended-finance
fund for coral reef conservation (SDG 14), launched 2020-09-16.

Structure: $125m grant window (UN MPTF: UNDP / UNEP / UNCDF) +
~$500m investment window managed by Pegasus Capital Advisors.

Source: Prince Albert II of Monaco Foundation initiative page —
https://www.fpa2.org/en/initiatives/global-fund-for-coral-reefs-008

Verbatim:
"Initiated by the Prince Albert II of Monaco Foundation and the Paul G.
Allen Family Foundation, and conceptualized during a workshop held in the
Principality of Monaco in 2018, the GFCR has since become a global
partnership that includes Member States (Germany, France), the United
Kingdom, philanthropic organizations (the Paul G. Allen Family Foundation
and the Prince Albert II of Monaco Foundation) ..."

Per advice doc lesson 11: one custom scraper per fund. This is bespoke to
the FPA2 initiative-page prose layout.
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

SCRAPER_NAME = "fund_lp_scraper_GFCR"
SOURCE_URL = "https://www.fpa2.org/en/initiatives/global-fund-for-coral-reefs-008"
FUND_SLUG = "gfcr-global-fund-for-coral-reefs"
INGO_SLUG = ""
COMMITMENT_YEAR = "2020"  # GFCR officially launched 2020-09-16

GFCR_LPs: list[tuple[str, str]] = [
    ("Prince Albert II of Monaco Foundation", "Prince Albert II of Monaco Foundation"),
    ("Paul G. Allen Family Foundation", "Paul G. Allen Family Foundation"),
]

CONTEXT_NEEDLES = [
    "Global Fund for Coral Reefs",
    "Initiated by the Prince Albert II of Monaco Foundation",
    "philanthropic organizations",
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

    missing_context = [n for n in CONTEXT_NEEDLES if n not in html]
    if missing_context:
        raise RuntimeError(
            f"{SCRAPER_NAME}: source no longer describes the GFCR co-initiator "
            f"relationship — missing context: {missing_context}"
        )

    rows: list[dict] = []
    today = date.today().isoformat()
    missing: list[str] = []

    for canonical, needle in GFCR_LPs:
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
