"""fund_lp_scraper_TransformHealthFund.py

Scrapes the named LPs of the Transform Health Fund (AfricInvest /
Health Finance Coalition) from Grand Challenges Canada's final-close
announcement (3 October 2024).

Source: https://www.grandchallenges.ca/2024/thf-final-close/

Why GCC's page rather than AfricInvest or HFC: the GCC announcement
lists the full LP roster verbatim and is reachable without Cloudflare
challenges. AfricInvest's site renders the LP roster via JS; HFC's
press release page is hosted on Squarespace with lazy-load (lesson 14).

Per advice doc lesson 11: one custom scraper per fund. This is bespoke
to the GCC announcement layout - it does not generalise.
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

SCRAPER_NAME = "fund_lp_scraper_TransformHealthFund"
SOURCE_URL = "https://www.grandchallenges.ca/2024/thf-final-close/"
FUND_SLUG = "transform-health-fund"
INGO_SLUG = ""  # AfricInvest-managed; no INGO parent
COMMITMENT_YEAR = "2024"  # final close 3 October 2024

# (canonical LP name, substring needle to verify in source HTML)
THF_LPS: list[tuple[str, str]] = [
    ("Royal Philips", "Royal Philips"),
    ("International Finance Corporation", "International Finance Corporation"),
    ("Swedfund", "Swedfund"),
    ("U.S. International Development Finance Corporation", "U.S. International Development Finance Corporation"),
    ("Proparco", "Proparco"),
    ("Merck & Co., Inc.", "Merck &amp; Co"),
    ("FSD Africa Investments", "FSD Africa Investments"),
    ("Grand Challenges Canada", "Grand Challenges Canada"),
    ("ImpactAssets Inc.", "ImpactAssets"),
    ("Global Health Investment Corporation", "Global Health Investment Corporation"),
    ("Ceniarth", "Ceniarth"),
    ("UBS Optimus Foundation", "UBS Optimus Foundation"),
    ("Skoll Foundation", "Skoll Foundation"),
    ("Chemonics International", "Chemonics"),
    ("Anesvad Foundation", "Anesvad"),
    ("Netri Foundation", "Netri"),
    ("U.S. Agency for International Development", "U.S. Agency for International Development"),
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

    for canonical, needle in THF_LPS:
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
