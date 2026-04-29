"""fund_lp_scraper_IncofinAgRIF.py

Scrapes the named LPs of the Incofin agRIF fund from Incofin's two
publicly disclosed close press releases (2017 first/second close adding
new investors, and 2020 debt-commitments close adding pension funds and
new DFI partners).

Sources (two distinct Incofin press releases):
  1. https://incofin.com/new-investors-in-agrif-increase-access-to-finance-for-the-agricultural-sector/
     -- April 2017 PR; lists ALL initial investors (EIB, Proparco, SIFEM,
        BIO, Volksvermogen, ACV-CSC Metea, Incofin IM) AND new investors
        added at second close (AXA IM, KBC Pensioenfonds, Korys, Invest in
        Visions, MRBB).
  2. https://incofin.com/incofin-secures-usd-76-million-in-commitments-for-its-agrif-fund/
     -- January 2020 PR; lists 2020 debt commitments (vdk, SPF, SPOV) plus
        new partners (BNP Paribas, OeEB).

Approach: same prose-pattern as KRIF / FEFISOL II / etc., extended to
two source URLs (per-row Source URL). 12 named LPs total — the most
comprehensive LP roster of any non-INGO fund in this batch.

DEVIATION FROM SINGLE-SOURCE PRECEDENT: same as fund_lp_scraper_BambooBLOC.
The schema supports per-row Source URL; using two PRs captures the full
roster across the fund's two disclosed closes more accurately than picking
one and skip-listing the other LPs.
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

SCRAPER_NAME = "fund_lp_scraper_IncofinAgRIF"
FUND_SLUG = "incofin-agrif"
INGO_SLUG = ""

PR_2017_URL = (
    "https://incofin.com/"
    "new-investors-in-agrif-increase-access-to-finance-for-the-agricultural-sector/"
)
PR_2020_URL = (
    "https://incofin.com/"
    "incofin-secures-usd-76-million-in-commitments-for-its-agrif-fund/"
)

# (canonical LP name, substring needle, source URL, commitment year)
AGRIF_LPS: list[tuple[str, str, str, str]] = [
    # Initial investors (named in 2017 PR as "Initial investors").
    # NB: canonical names match existing FEFISOL II / Kampani scrapers so
    # cross-fund LPs (EIB, Proparco, BIO, SIFEM) collapse to the same slug.
    ("European Investment Bank", "European Investment Bank (EIB)", PR_2017_URL, "2015"),
    ("Proparco", "Proparco", PR_2017_URL, "2015"),
    ("SIFEM", "Swiss Investment Fund for Emerging Markets", PR_2017_URL, "2015"),
    ("BIO (Belgian Investment Company for Developing Countries)", "Belgische Investeringsmaatschappij voor Ontwikkelingslanden", PR_2017_URL, "2015"),
    ("Volksvermogen", "Volksvermogen", PR_2017_URL, "2015"),
    ("ACV-CSC Metea", "ACV-CSC Metea", PR_2017_URL, "2015"),
    ("Incofin Investment Management", "Incofin Investment Management", PR_2017_URL, "2015"),
    # 2017 second-close additions (named in 2017 PR as "new investors").
    ("AXA Investment Managers", "AXA Investment Managers", PR_2017_URL, "2017"),
    ("KBC Pensioenfonds", "KBC Pensioenfonds", PR_2017_URL, "2017"),
    ("Korys", "Korys", PR_2017_URL, "2017"),
    ("Invest in Visions Global Social Impact Fund", "Invest in Visions Global Social Impact Fund", PR_2017_URL, "2017"),
    ("Maatschappij voor Roerend Bezit van de Boerenbond (MRBB)", "Maatschappij voor Roerend Bezit van de Boerenbond", PR_2017_URL, "2017"),
    # 2020 debt commitments (named in 2020 PR as existing investors).
    ("vdk bank", "vdk", PR_2020_URL, "2020"),
    ("SPF (Dutch railway pension fund)", "Dutch railway pension fund SPF", PR_2020_URL, "2020"),
    ("SPOV (public transport pension fund)", "public transport pension fund SPOV", PR_2020_URL, "2020"),
    # 2020 new partners (named in 2020 PR as "new partners").
    ("BNP Paribas", "BNP Paribas", PR_2020_URL, "2020"),
    ("Oesterreichische Entwicklungsbank (OeEB)", "OeEB", PR_2020_URL, "2020"),
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
        for url in {PR_2017_URL, PR_2020_URL}:
            r = client.get(url)
            r.raise_for_status()
            source_html[url] = r.content.decode("utf-8", errors="replace")

    rows: list[dict] = []
    missing: list[str] = []

    for canonical, needle, src_url, year in AGRIF_LPS:
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
