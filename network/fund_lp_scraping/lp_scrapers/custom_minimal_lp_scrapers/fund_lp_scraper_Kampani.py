"""fund_lp_scraper_Kampani.py

Scrapes Kampani's shareholder roster from its public about page.
Source: https://www.kampani.org/about-us  (section: "The Shareholders")

Approach: hand-curated list of named shareholders disclosed on the page,
each verified by an outbound href in the shareholder logo grid (alts are
empty on the Webflow build, so we anchor on the destination URL substring
instead). If any expected shareholder is missing the scraper exits non-zero.

Per the catalogue: Kampani was incorporated 2015 by five Belgian INGOs
(Rikolto/Vredeseilanden, Broederlijk Delen, Trias, Oxfam-Solidarité,
Louvain Coopération) plus Alterfin, SIDI, Boerenbond, King Baudouin
Foundation, and other private investors. The current shareholder grid on
about-us also lists CLAC, Solidaridad, KU Leuven, BIO, and a "Private
individuals" tile (skipped — not a named entity).
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

SCRAPER_NAME = "fund_lp_scraper_Kampani"
SOURCE_URL = "https://www.kampani.org/about-us"
FUND_SLUG = "kampani"
INGO_SLUG = "rikolto-broederlijk-delen-trias-oxfam-solidarite-belgium-louvain-cooperation-sidi"
COMMITMENT_YEAR = "2015"  # base capital subscribed at incorporation in 2015

# (canonical name, substring to verify in source HTML — typically the
# outbound shareholder logo href)
KAMPANI_SHAREHOLDERS: list[tuple[str, str]] = [
    ("King Baudouin Foundation", "kbs-frb.be"),
    ("Rikolto", "rikolto.org"),
    ("Boerenbond", "boerenbond.be"),
    ("Alterfin", "alterfin.be"),
    ("Louvain Coopération", "louvaindev.org"),
    ("Trias", "trias.ngo"),
    ("SIDI (Solidarité Internationale pour le Développement et l'Investissement)", "sidi.fr"),
    ("Broederlijk Delen", "broederlijkdelen.be"),
    ("CLAC (Coordinadora Latinoamericana y del Caribe de Pequeños Productores)", "clac-comerciojusto.org"),
    ("Oxfam-Solidarité Belgium", "oxfambelgie.be"),
    ("KU Leuven", "kuleuven.be"),
    ("Solidaridad", "solidaridad-network"),
    ("BIO (Belgian Investment Company for Developing Countries)", "bio---belgian-investment-company-for-developing-countries"),
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

    for canonical, needle in KAMPANI_SHAREHOLDERS:
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
            f"{SCRAPER_NAME}: {len(missing)} expected shareholder(s) not found "
            f"in source (page may have been edited): {missing}"
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
