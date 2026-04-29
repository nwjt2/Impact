"""lp_portfolio_scraper_ShellFoundation.py

LP-portfolio scraper for Shell Foundation.

Source: https://shellfoundation.org/wp-json/wp/v2/portfolio (custom WordPress
post type, exposes all 62 portfolio entries as JSON in one request).

Approach: structured JSON + hand-curated allowlist. Shell Foundation's
"Our Portfolio" page mixes three relationship kinds under a single 'partner'
label:
  1. Companies / programmes Shell has actually invested in (the investees).
  2. Funds Shell has committed capital to / sub-advised through.
  3. Co-funders and donors that share governance with Shell rather than
     receive Shell capital (FCDO, BII, FMO, US DFC, Mastercard, Gates,
     ITC, GIZ, Trane, etc.).

For LP-portfolio purposes only (1) and (2) are real LP-side edges. We
hand-curate which Shell-feed slugs go in which bucket; the co-funder set
listed in `_CO_FUNDER_SLUGS` is explicitly skipped (honesty discipline).
The scraper exits non-zero if the feed shape changes (zero rows of either
type emitted), so the operator notices.

Slug discipline: canonical Investee Slugs are kebab-cased to match
catalogue conventions; combine_fund_lps and combine_portco_investors
find-or-create with Pipeline Status=pending_onboard.
"""
from __future__ import annotations

import html as html_lib
import json
import sys
from datetime import date
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "lp_portfolio_scraper_ShellFoundation"
LP_NAME = "Shell Foundation"
LP_SLUG = "shell-foundation"
SOURCE_URL = "https://shellfoundation.org/our-portfolio/"
WPJSON_URL = "https://shellfoundation.org/wp-json/wp/v2/portfolio?per_page=100"

# Shell-feed slugs that are co-funders / donors / governance partners, NOT
# Shell investees. These are skipped at scrape time.
_CO_FUNDER_SLUGS: set[str] = {
    "british_international_investment",
    "dfc",
    "fmo-2",
    "foreign-commonwealth-development-office",
    "gates-foundation",
    "giz",
    "itc-limited",
    "mastercard",
    "trane-technologies",
    "rabo_agrifinance",
    "sterling-bank",  # Shell-FCDO grant programme partner, not investee
    "davisandshirtliff",  # Shell scale partner, not direct investee
    "small-industries-development-bank-of-india-sidbi",  # capital partner, not investee
    "the-co-operative-bank-of-kenya-ltd",  # banking partner
}

# Shell-feed slugs that are FUNDS (Shell-committed capital → fund vehicle).
_FUND_SLUGS: set[str] = {
    "catalyst-fund-resilience",
    "triplejump-energy-entrepreneur-growth-fund",
    "persistent_energy",
    "bix_capital",
    "equator",
    "nithio",
    "responsability-investments",
    "mirova-sunfunder-inc",
    "sima-funds",
    "echo-venture-capital-partners-echovc",
    "factore-ventures",
}

# All other Shell-feed slugs default to `company`. Override the human-readable
# canonical name when Shell's title is ugly or all-caps.
_NAME_OVERRIDES: dict[str, str] = {
    # (slug from feed) -> canonical display name
    "afex": "AFEX Fair Trade Limited",
    "ampersand-solar": "Ampersand",
    "bix_capital": "BIX Capital",
    "echo-venture-capital-partners-echovc": "EchoVC (Echo Venture Capital Partners)",
    "equator": "Equator Africa Fund",
    "factore-ventures": "Factor[e] Ventures",
    "good-machine-2": "Good Machine",
    "intellecap": "Intellecap",
    "jaza_energy": "Jaza Energy",
    "junior-achievement-india-services-jais": "Junior Achievement India Services (JAIS)",
    "keep_it_cool": "Keep IT Cool",
    "kofa-2": "Kofa Holdings Limited",
    "m-kopa-labs": "M-KOPA Labs",
    "max": "Max (Metro Africa Express)",
    "mesh": "MESH (Player First)",
    "mirova-sunfunder-inc": "Mirova SunFunder",
    "ororo-collections-and-distribution-management-ltd": "Ororo Collections and Distribution Management Ltd",
    "pash-advisory-llp": "PASH Global",
    "persistent_energy": "Persistent Energy Capital",
    "responsability-investments": "responsAbility Investments",
    "s4s-science-for-society-techno-private-ltd": "S4S Technologies",
    "sistema-bio": "Sistema.bio",
    "sima-funds": "SIMA Funds",
    "triplejump-energy-entrepreneur-growth-fund": "TripleJump Energy Entrepreneur Growth Fund",
    "upaya-social-ventures": "Upaya Social Ventures",
    "villgro-innovations-foundation": "Villgro Innovations Foundation",
}

# Canonical slug overrides — keep slugs aligned with existing catalogue
# entries when there's a mismatch.
_SLUG_OVERRIDES: dict[str, str] = {
    "ampersand-solar": "ampersand",
    "good-machine-2": "good-machine",
    "kofa-2": "kofa-holdings-limited",
    "pash-advisory-llp": "pash-global",
    "afex": "afex-fair-trade-limited",
    "ororo-collections-and-distribution-management-ltd": "ororo",
    "s4s-science-for-society-techno-private-ltd": "s4s-technologies",
    # Funds:
    "bix_capital": "bix-capital",
    "equator": "equator-africa-fund",
    "factore-ventures": "factor-e-ventures",
    "jaza_energy": "jaza-energy",
    "keep_it_cool": "keep-it-cool",
    "mirova-sunfunder-inc": "mirova-sunfunder",
    "persistent_energy": "persistent-energy-capital",
}

OUTPUT_HEADERS = [
    "LP Slug",
    "Investee Name",
    "Investee Slug",
    "Investee Type",
    "Commitment Year",
    "Source URL",
    "Source Date",
    "Confidence",
    "Scraping Method Used",
]


def scrape(run_number: int, output_dir: Path | str) -> int:
    r = httpx.get(
        WPJSON_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    data = json.loads(r.content.decode("utf-8", errors="replace"))

    today = date.today().isoformat()
    rows: list[dict] = []
    seen_slugs: set[str] = set()

    for d in data:
        feed_slug = d.get("slug") or ""
        if not feed_slug or feed_slug in _CO_FUNDER_SLUGS:
            continue
        title = html_lib.unescape(
            (d.get("title") or {}).get("rendered") or feed_slug
        ).strip()
        canonical_name = _NAME_OVERRIDES.get(feed_slug, title)
        canonical_slug = _SLUG_OVERRIDES.get(feed_slug, slugify(canonical_name))
        if canonical_slug in seen_slugs:
            continue
        seen_slugs.add(canonical_slug)
        kind = "fund" if feed_slug in _FUND_SLUGS else "company"
        rows.append(
            {
                "LP Slug": LP_SLUG,
                "Investee Name": canonical_name,
                "Investee Slug": canonical_slug,
                "Investee Type": kind,
                "Commitment Year": "",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Confidence": "confirmed",
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    found_companies = sum(1 for r in rows if r["Investee Type"] == "company")
    found_funds = sum(1 for r in rows if r["Investee Type"] == "fund")
    if found_companies == 0 or found_funds == 0:
        raise RuntimeError(
            f"{SCRAPER_NAME}: feed-shape regression — emitted "
            f"{found_companies} companies and {found_funds} funds. Check "
            f"{WPJSON_URL} (Shell may have changed slug naming)."
        )

    out_path = Path(output_dir) / f"run_{run_number}" / f"{LP_SLUG}.csv"
    write_rows(out_path, OUTPUT_HEADERS, rows)
    return len(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "network" / "lp_portfolio_scraping" / "individual_lp_portfolios"),
    )
    args = parser.parse_args()
    n = scrape(args.run, args.output_dir)
    print(f"{SCRAPER_NAME}: wrote {n} rows to {args.output_dir}/run_{args.run}/{LP_SLUG}.csv")


if __name__ == "__main__":
    main()
