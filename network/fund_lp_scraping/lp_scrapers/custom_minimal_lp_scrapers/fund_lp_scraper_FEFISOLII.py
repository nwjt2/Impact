"""fund_lp_scraper_FEFISOLII.py

Scrapes LP roster of FEFISOL II from the EIB launch press release.

Source: https://www.eib.org/en/press/all/2022-254-launch-of-the-new-european-solidarity-financing-fund-for-africa-fefisol-ii-with-a-first-closing-of-eur2-5-million-and-a-technical-support-envelope-of-1-million-euros

Approach: same as fund_lp_scraper_KRIF — hand-curated list of LP names sourced
from this specific press release, each verified by substring match. If any
expected name is missing the scraper exits non-zero so the operator notices.

FEFISOL II's promoters are SIDI (a CCFD-Terre Solidaire subsidiary) and
Alterfin; both also commit capital, so they appear here as LPs. EIB anchored
the fund. Other named investors below come from the EIB press text.
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

SCRAPER_NAME = "fund_lp_scraper_FEFISOLII"
SOURCE_URL = (
    "https://www.eib.org/en/press/all/"
    "2022-254-launch-of-the-new-european-solidarity-financing-fund-for-africa-"
    "fefisol-ii-with-a-first-closing-of-eur2-5-million-and-a-technical-support-"
    "envelope-of-1-million-euros"
)
FUND_SLUG = "fefisol-ii"
INGO_SLUG = "sidi-ccfd-terre-solidaire-subsidiary"
COMMITMENT_YEAR = "2022"  # first close June 2022

# (canonical LP name, substring to verify in source HTML)
FEFISOL_II_LPs: list[tuple[str, str]] = [
    ("European Investment Bank", "European Investment Bank"),
    ("Proparco", "Proparco"),
    ("BIO (Belgian Investment Company for Developing Countries)", "BIO"),
    ("Banca Etica", "Banca Etica"),
    ("Crédit Coopératif", "Crédit Coopératif"),
    ("SOS Faim Luxembourg", "SOS Faim Luxembourg"),
    ("SIDI (Solidarité Internationale pour le Développement et l'Investissement)", "SIDI"),
    ("Alterfin", "Alterfin"),
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

    for canonical, needle in FEFISOL_II_LPs:
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
