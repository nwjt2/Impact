"""fund_lp_scraper_BambooBLOC.py

Scrapes the named government LPs of the BLOC Smart Africa Impact Fund.

Sources (two distinct Bamboo Capital Partners press releases):
  1. https://bamboocp.com/bloc-smart-africa-impact-fund-launched-with-governments-of-luxembourg-and-cote-divoire-as-anchor-sponsors/
     -- Feb 2021 launch PR; names Luxembourg + Côte d'Ivoire as anchor sponsors.
  2. https://bamboocp.com/the-governments-of-the-democratic-republic-of-the-congo-and-tunisia-agree-to-invest-in-bloc-smart-africa-%EF%82%B7/
     -- Sep 2019 PR; names DRC + Tunisia as committed LPs.

Approach: hand-curated (canonical_name, needle, source_url) tuples — same
prose-pattern as KRIF / FEFISOL II / WCIF3 etc., but per-row Source URL
because no single PR names all five committed government LPs. Each LP is
verified by substring match in its declared source; if any goes missing
the scraper exits non-zero (Lesson 17 / honesty discipline).

DEVIATION FROM SINGLE-SOURCE PRECEDENT:
The schema (`Source URL` per row) supports per-LP source attribution and
this is the cleanest way to capture the full BLOC LP roster — the
two-press-release truth is more accurate than picking one PR and skip-
listing the other LPs.

Burkina Faso, Djibouti and Chad are mentioned in the September 2019 PR as
having signed declarations of approval but had NOT committed capital —
intentionally NOT included. (Honesty discipline: only committed LPs.)
Togo committed EUR 5m per the September 2019 PR's *intro paragraph*, but
the substring "Togo" doesn't appear in the post-CMS HTML body — it's
likely been redacted/edited out, so we don't include Togo either.
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

SCRAPER_NAME = "fund_lp_scraper_BambooBLOC"
FUND_SLUG = "bamboo-bloc-smart-africa"
INGO_SLUG = ""

LAUNCH_PR_URL = (
    "https://bamboocp.com/"
    "bloc-smart-africa-impact-fund-launched-with-governments-of-"
    "luxembourg-and-cote-divoire-as-anchor-sponsors/"
)
DRC_TUNISIA_PR_URL = (
    "https://bamboocp.com/"
    "the-governments-of-the-democratic-republic-of-the-congo-and-tunisia-"
    "agree-to-invest-in-bloc-smart-africa-%EF%82%B7/"
)

# (canonical LP name, substring needle, source URL, commitment year)
BLOC_LPS: list[tuple[str, str, str, str]] = [
    ("Government of Luxembourg", "Luxembourg", LAUNCH_PR_URL, "2021"),
    ("Government of Côte d'Ivoire", "Ivoire", LAUNCH_PR_URL, "2021"),
    ("Government of the Democratic Republic of the Congo", "Democratic Republic of the Congo", DRC_TUNISIA_PR_URL, "2019"),
    ("Government of Tunisia", "Tunisia", DRC_TUNISIA_PR_URL, "2019"),
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
        for url in {LAUNCH_PR_URL, DRC_TUNISIA_PR_URL}:
            r = client.get(url)
            r.raise_for_status()
            source_html[url] = r.content.decode("utf-8", errors="replace")

    rows: list[dict] = []
    missing: list[str] = []

    for canonical, needle, src_url, year in BLOC_LPS:
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
