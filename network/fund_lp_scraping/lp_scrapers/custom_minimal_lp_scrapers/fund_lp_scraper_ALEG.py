"""fund_lp_scraper_ALEG.py

Scrapes LP roster of Acumen Latin America Early Growth Fund (ALEG / ALCP I)
from LAVCA's coverage of the August 2020 final close.

Source: https://www.lavca.org/alive-ventures-reaches-final-close-for-us28m-latin-america-impact-fund/

The ALEG fund manager — formerly Acumen LatAm Capital Partners — rebranded
to ALIVE Ventures around the time of this final close. Acumen's own site
(acumen.org) is Cloudflare-walled, so we lean on this LAVCA press writeup
which lists eight named LPs in prose. Same hand-curated-with-verification
pattern as fund_lp_scraper_KRIF.

Per advice doc lesson 11: one custom scraper per fund.
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

SCRAPER_NAME = "fund_lp_scraper_ALEG"
SOURCE_URL = (
    "https://www.lavca.org/"
    "alive-ventures-reaches-final-close-for-us28m-latin-america-impact-fund/"
)
FUND_SLUG = "aleg-acumen-latam-early-growth"
INGO_SLUG = "acumen"
COMMITMENT_YEAR = "2020"  # final close August 2020

# (canonical LP name, substring to verify in source HTML)
ALEG_LPs: list[tuple[str, str]] = [
    ("IDB Lab", "IDB Lab"),
    ("Dutch Good Growth Fund (DGGF)", "Dutch Good Growth Fund"),
    ("John D. and Catherine T. MacArthur Foundation", "MacArthur Foundation"),
    ("Bancóldex", "Bancoldex"),
    ("Mercantil Colpatria", "Mercantil Colpatria"),
    ("Fundación Bancolombia", "Bancolombia"),
    ("Fundación Sura", "Fundaci"),  # 'Fundación Sura' verified by general 'Fundaci' presence
    ("Fundación WWB Colombia", "WWB Colombia"),
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

    for canonical, needle in ALEG_LPs:
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
