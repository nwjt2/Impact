"""fund_lp_scraper_GFCR.py

Scrapes named LP / co-initiator / philanthropic-contributor roster of the
Global Fund for Coral Reefs (GFCR) — a UN-administered blended-finance
fund for coral reef conservation (SDG 14), launched 2020-09-16.

Structure: $125m grant window (UN MPTF: UNDP / UNEP / UNCDF) +
~$500m investment window managed by Pegasus Capital Advisors.

Sources (per-LP):
  - Prince Albert II of Monaco Foundation initiative page (FPA2) —
    https://www.fpa2.org/en/initiatives/global-fund-for-coral-reefs-008
    Names co-initiators: Prince Albert II Foundation + Paul G. Allen Family Foundation.

  - GFCR coalition / news page (globalfundcoralreefs.org) —
    https://globalfundcoralreefs.org/reef-plus/news/bloomberg-builders-join-gfcr-for-coral-protection/
    Names 2022 Grant Fund contributors: Bloomberg Philanthropies, Builders Vision.

Per advice doc lesson 11: one custom scraper per fund. This is bespoke to
the GFCR co-initiator + Grant Fund contributor disclosure pattern.

Note on edge classification: Grant Fund contributions are treated as LP-
equivalent edges, mirroring the precedent set by the FPA2 entry (the
co-initiators contributed via the same Grant Fund window, not the
investment window). If the operator wants to tighten this rule, drop the
SECOND source block and the Bloomberg/Builders entries.
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
FUND_SLUG = "gfcr-global-fund-for-coral-reefs"
INGO_SLUG = ""

# Each source: (url, commitment_year, [(canonical_name, needle), ...], [context_needles])
SOURCES: list[tuple[str, str, list[tuple[str, str]], list[str]]] = [
    (
        "https://www.fpa2.org/en/initiatives/global-fund-for-coral-reefs-008",
        "2020",  # GFCR officially launched 2020-09-16
        [
            ("Prince Albert II of Monaco Foundation", "Prince Albert II of Monaco Foundation"),
            ("Paul G. Allen Family Foundation", "Paul G. Allen Family Foundation"),
        ],
        [
            "Global Fund for Coral Reefs",
            "Initiated by the Prince Albert II of Monaco Foundation",
            "philanthropic organizations",
        ],
    ),
    (
        "https://globalfundcoralreefs.org/reef-plus/news/bloomberg-builders-join-gfcr-for-coral-protection/",
        "2022",  # Bloomberg / Builders 2022 Grant Fund contributions
        [
            ("Bloomberg Philanthropies", "Bloomberg Philanthropies"),
            ("Builders Vision", "Builders Vision"),
        ],
        [
            "Global Fund for Coral Reefs",
            "Grant Fund",
        ],
    ),
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
    rows: list[dict] = []
    today = date.today().isoformat()
    all_missing: list[str] = []

    for source_url, year, lps, context_needles in SOURCES:
        r = httpx.get(
            source_url,
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
            follow_redirects=True,
        )
        r.raise_for_status()
        html = r.content.decode("utf-8", errors="replace")

        missing_context = [n for n in context_needles if n not in html]
        if missing_context:
            raise RuntimeError(
                f"{SCRAPER_NAME}: source {source_url} no longer describes the GFCR "
                f"relationship — missing context: {missing_context}"
            )

        for canonical, needle in lps:
            if needle not in html:
                all_missing.append(f"{canonical} (in {source_url})")
                continue
            rows.append(
                {
                    "Fund Slug": FUND_SLUG,
                    "INGO Slug": INGO_SLUG,
                    "LP Name": canonical,
                    "LP Slug": slugify(canonical),
                    "Commitment Year": year,
                    "Source URL": source_url,
                    "Source Date": today,
                    "Confidence": "confirmed",
                    "Scraping Method Used": SCRAPER_NAME,
                }
            )

    if all_missing:
        raise RuntimeError(
            f"{SCRAPER_NAME}: {len(all_missing)} expected LP(s) not found in source "
            f"(page may have been edited): {all_missing}"
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
