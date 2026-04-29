"""fund_lp_scraper_AccionVentureLabII.py

Scrapes the named LPs of Accion Venture Lab Fund II from the PR Newswire
mirror of Accion's final-close press release (8 September 2025).

Source: https://www.prnewswire.com/news-releases/accion-announces-close-of-61-6m-second-accion-venture-lab-fund-investing-in-early-stage-inclusive-fintech-302548169.html

Why the PR Newswire mirror, not accion.org: accion.org's news pages return
HTTP 403 to our scraper user-agent (Cloudflare). PR Newswire reposts the
identical release without the same protection. Verbatim LP sentence:
"Limited Partners in the fund include the Dutch entrepreneurial
development bank FMO, Proparco, ImpactAssets, Ford Foundation, MetLife,
and Mastercard."

Per advice doc lesson 11: one custom scraper per fund. This is bespoke
to the PR Newswire prose layout — it does not generalise.
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

SCRAPER_NAME = "fund_lp_scraper_AccionVentureLabII"
SOURCE_URL = (
    "https://www.prnewswire.com/news-releases/"
    "accion-announces-close-of-61-6m-second-accion-venture-lab-fund-"
    "investing-in-early-stage-inclusive-fintech-302548169.html"
)
FUND_SLUG = "accion-venture-lab-fund-ii"
INGO_SLUG = "accion"
COMMITMENT_YEAR = "2025"  # final close 8 September 2025

# (canonical LP name, substring needle to verify in source HTML)
ACCION_VL2_LPS: list[tuple[str, str]] = [
    ("FMO", "FMO"),
    ("Proparco", "Proparco"),
    ("ImpactAssets", "ImpactAssets"),
    ("Ford Foundation", "Ford Foundation"),
    ("MetLife", "MetLife"),
    ("Mastercard", "Mastercard"),
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

    for canonical, needle in ACCION_VL2_LPS:
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
