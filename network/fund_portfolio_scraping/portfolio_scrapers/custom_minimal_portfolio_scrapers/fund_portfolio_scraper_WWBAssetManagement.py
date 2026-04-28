"""fund_portfolio_scraper_WWBAssetManagement.py

Scrapes WWB Asset Management's portfolio page for both Fund I and Fund II.
Source: https://www.womensworldbanking.org/asset-management/

Per advice doc lesson 11, this is one bespoke scraper for one bespoke page.
The page already encodes per-fund attribution via three Elementor divider
<h3>s ("FUND II", "FUND I", "FUND I EXITED"); we lock to those headers and
collect the portfolio anchors that follow each one until the next divider.

Each portfolio entry on the page is rendered as an <a href="<company-site>"
target="_blank"><img alt="<logo-filename>"></a>. The img alt is a logo-
filename (e.g. "amartha logo 2020"), so we don't trust it for naming —
instead we hand-curate canonical (name, website) pairs and verify each
website appears within the right fund's section. If an expected portco is
missing, the scraper exits non-zero so the operator notices.

Output is split across two fund slugs:
  - wwb-capital-partners-fund-ii  (Fund II current portfolio)
  - wwb-capital-partners-fund     (Fund I current + exited)
"""
from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

SCRAPER_NAME = "fund_portfolio_scraper_WWBAssetManagement"
SOURCE_URL = "https://www.womensworldbanking.org/asset-management/"
INGO_SLUG = "women-s-world-banking"

# Each Elementor <h3 class="elementor-divider__text..."> label maps to a
# fund slug. Fund I and Fund I Exited both belong to the same fund vehicle.
FUND_BY_DIVIDER = {
    "FUND II": "wwb-capital-partners-fund-ii",
    "FUND I": "wwb-capital-partners-fund",
    "FUND I EXITED": "wwb-capital-partners-fund",
}

# (fund_divider_label, canonical_company_name, company_website)
PORTFOLIO: list[tuple[str, str, str]] = [
    # Fund II
    ("FUND II", "Sitara (SGR Limited)",      "https://www.sgrlimited.in/"),
    ("FUND II", "Pula",                      "https://www.pula-advisors.com/"),
    ("FUND II", "Aflore",                    "https://www.aflore.co/"),
    ("FUND II", "Amartha",                   "https://amartha.com/"),
    ("FUND II", "UGAFODE Microfinance",      "https://www.ugafode.co.ug/"),
    ("FUND II", "Tugende",                   "https://gotugende.com/"),
    ("FUND II", "BikeBazaar",                "https://bikebazaar.com/"),
    ("FUND II", "Pezesha",                   "https://pezesha.com/"),
    ("FUND II", "Tienda Pago",               "https://www.tiendapago.com/"),
    ("FUND II", "Igloo",                     "https://iglooinsure.com/"),
    ("FUND II", "Lulalend",                  "https://www.lulalend.co.za/"),
    ("FUND II", "Platcorp Group",            "https://www.platcorpgroup.com/"),
    # Fund I current
    ("FUND I", "Annapurna Finance",          "https://annapurnafinance.in/"),
    ("FUND I", "BancoSol",                   "https://www.bancosol.com.bo/"),
    ("FUND I", "Sanad for Microfinance",     "https://sanadcomjo.com/AboutUs"),
    # Fund I exited
    ("FUND I EXITED", "Banco W",             "https://www.bancow.com.co/"),
]

OUTPUT_HEADERS = [
    "Fund Slug",
    "INGO Slug",
    "Company Name",
    "Company Slug",
    "Company Website",
    "Round",
    "Round Date",
    "Lead",
    "Source URL",
    "Source Date",
    "Scraping Method Used",
]

_DIVIDER_RE = re.compile(
    r'<h3 class="elementor-divider__text[^"]*"[^>]*>\s*([A-Z][A-Z\s]+?)\s*</h3>',
    re.IGNORECASE,
)


def _section_for(html: str, label: str, divider_positions: list[tuple[str, int]]) -> str:
    """Return the slice of HTML for the divider with text == label."""
    for i, (lab, start) in enumerate(divider_positions):
        if lab == label:
            end = divider_positions[i + 1][1] if i + 1 < len(divider_positions) else len(html)
            return html[start:end]
    return ""


def scrape(run_number: int, output_dir: Path | str) -> dict[str, int]:
    r = httpx.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    html = r.content.decode("utf-8", errors="replace")

    idx_p = html.find("Portfolio Companies")
    idx_i = html.find("Investors</h2>")
    if idx_p < 0 or idx_i < 0 or idx_i < idx_p:
        raise RuntimeError(
            f"{SCRAPER_NAME}: portfolio/investors section markers not found "
            f"on {SOURCE_URL}"
        )
    section = html[idx_p:idx_i]

    dividers = [(m.group(1).strip(), m.start()) for m in _DIVIDER_RE.finditer(section)]
    seen_labels = {lab for lab, _ in dividers}
    for required in FUND_BY_DIVIDER:
        if required not in seen_labels:
            raise RuntimeError(
                f"{SCRAPER_NAME}: expected divider <h3>{required}</h3> not found "
                f"in portfolio section (page may have been edited)"
            )

    # Bucket portcos by fund slug
    rows_by_fund: dict[str, list[dict]] = {}
    today = date.today().isoformat()
    missing: list[str] = []
    for divider, name, website in PORTFOLIO:
        sec = _section_for(section, divider, dividers)
        if website not in sec:
            missing.append(f"{name} ({website})")
            continue
        fund_slug = FUND_BY_DIVIDER[divider]
        rows_by_fund.setdefault(fund_slug, []).append(
            {
                "Fund Slug": fund_slug,
                "INGO Slug": INGO_SLUG,
                "Company Name": name,
                "Company Slug": slugify(name),
                "Company Website": website,
                "Round": "unknown",
                "Round Date": "",
                "Lead": "unknown",
                "Source URL": SOURCE_URL,
                "Source Date": today,
                "Scraping Method Used": SCRAPER_NAME,
            }
        )

    if missing:
        raise RuntimeError(
            f"{SCRAPER_NAME}: {len(missing)} expected portco link(s) not found in "
            f"the right section: {missing}"
        )

    counts: dict[str, int] = {}
    for fund_slug, rows in rows_by_fund.items():
        out_path = Path(output_dir) / f"run_{run_number}" / f"{fund_slug}.csv"
        write_rows(out_path, OUTPUT_HEADERS, rows)
        counts[fund_slug] = len(rows)
    return counts


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "network" / "fund_portfolio_scraping" / "individual_fund_portfolios"),
    )
    args = parser.parse_args()
    counts = scrape(args.run, args.output_dir)
    for slug, n in counts.items():
        print(f"{SCRAPER_NAME}: {slug} -> {n} rows")


if __name__ == "__main__":
    main()
