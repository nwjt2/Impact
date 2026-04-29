"""Combine per-portco investor scraper output into investors.csv + portco_investors.csv.

Reads:
  - network/portco_investor_scraping/individual_portco_investors/run_<N>/*.csv  (primary)
  - network/lp_portfolio_scraping/individual_lp_portfolios/run_<N>/*.csv
    (secondary; rows with Investee Type=company are folded in as
     investor→company co-investor edges)
Writes:
  network/portco_investor_scraping/combined_portco_investors/run_<N>/all.csv  (audit, gitignored)
  network/catalogue/investors.csv                                              (find-or-create)
  network/catalogue/portfolio_companies.csv                                    (find-or-create — for companies discovered via LP-portfolio scrapers only)
  network/dashboard_prep/portco_investors.csv                                  (rebuilt each run)

Symmetric to combine_fund_lps.py.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import read_rows, write_rows  # noqa: E402
from network.utils.aliases import canonicalize_investor_slug, is_deprecated_investor_slug  # noqa: E402

INDIVIDUAL_DIR = REPO_ROOT / "network" / "portco_investor_scraping" / "individual_portco_investors"
COMBINED_DIR = REPO_ROOT / "network" / "portco_investor_scraping" / "combined_portco_investors"
LP_PORTFOLIO_DIR = REPO_ROOT / "network" / "lp_portfolio_scraping" / "individual_lp_portfolios"
INVESTORS_CSV = REPO_ROOT / "network" / "catalogue" / "investors.csv"
PORTFOLIO_COMPANIES_CSV = REPO_ROOT / "network" / "catalogue" / "portfolio_companies.csv"
PORTCO_INVESTORS_CSV = REPO_ROOT / "network" / "dashboard_prep" / "portco_investors.csv"
RUN_STATE_JSON = REPO_ROOT / "network" / "run_state.json"

INVESTORS_HEADERS = [
    "Investor Name",
    "Investor Slug",
    "Investor Type",
    "Impact Focus",
    "Website",
    "HQ Country",
    "AUM Bucket",
    "Status",
    "Notes",
]

PORTFOLIO_COMPANIES_HEADERS = [
    "Company Name",
    "Company Slug",
    "Website",
    "HQ Country",
    "Sector",
    "Stage",
    "Status",
    "Pipeline Status",
    "Notes",
]

PORTCO_INVESTORS_HEADERS = [
    "Company Slug",
    "Investor Slug",
    "Round",
    "Round Date",
    "Lead",
    "Source URL",
    "Source Date",
    "Scraping Method Used",
]

COMBINED_HEADERS = [
    "Company Slug",
    "Investor Name",
    "Investor Slug",
    "Round",
    "Round Date",
    "Lead",
    "Source URL",
    "Source Date",
    "Scraping Method Used",
]


def _read_run_number() -> int:
    if not RUN_STATE_JSON.exists():
        return 1
    state = json.loads(RUN_STATE_JSON.read_text(encoding="utf-8"))
    return max(1, int(state.get("current_run") or 1))


def _classify_investor_type(name: str) -> str:
    """Best-guess investor type for newly discovered LPs / co-investors.

    Operator review can refine. Heuristic only.
    Order matters: most-specific tests first.
    """
    n = name.lower()

    # Family offices first — "Family Foundation" should not be classified as plain foundation.
    if "family" in n and ("foundation" in n or "fund" in n or "office" in n):
        return "family-office"

    # Government / agency
    if any(w in n for w in ["usaid", "fcdo", "norad", "sida", "ministry", "department of"]):
        return "government"
    if n.startswith("government of"):
        return "government"

    # DFIs
    if "development finance" in n or " dfi " in f" {n} ":
        return "dfi"

    # VC / equity funds — pattern: ends in Ventures / Capital / Partners,
    # OR is a well-known VC firm (a16z, etc.). Note: this comes before
    # "foundation" to ensure e.g. "Coinbase Ventures" maps to vc, not other.
    vc_suffixes = (" ventures", " capital", " partners", " angel", " holdings")
    if any(n.endswith(s) for s in vc_suffixes):
        return "vc"
    well_known_vc = (
        "andreessen horowitz", "a16z", "sequoia", "tiger global",
        "y combinator", "kindred", "variant", "sv angel", "coinbase",
        "lowercarbon", "floating point",
    )
    if any(w in n for w in well_known_vc):
        return "vc"

    # Foundations / religious / charitable
    if "foundation" in n or "philanthropy" in n or "philanthrophy" in n:
        return "foundation"
    if "missionary" in n or "sister" in n or "religious" in n:
        return "foundation"
    if "investment services" in n or "charitable" in n:
        return "foundation"

    # Wealth advisors etc → other
    if "advisors" in n or "advisor" in n:
        return "other"

    return "other"


def _read_primary_rows(run_number: int) -> tuple[list[dict], int, int]:
    run_dir = INDIVIDUAL_DIR / f"run_{run_number}"
    rows: list[dict] = []
    invalid = 0
    files_seen = 0
    if not run_dir.exists():
        return rows, files_seen, invalid
    for csv_path in sorted(run_dir.glob("*.csv")):
        files_seen += 1
        these = read_rows(csv_path)
        valid = [r for r in these if r.get("Scraping Method Used")]
        dropped = len(these) - len(valid)
        if dropped > 0:
            print(f"  WARN  {csv_path.name}: {dropped} rows missing 'Scraping Method Used' — DROPPED")
            invalid += dropped
        rows.extend(valid)
    return rows, files_seen, invalid


def _read_secondary_rows(run_number: int) -> tuple[list[dict], int, int]:
    """Read LP-portfolio scraper rows, filter to Investee Type=company,
    map to portco-investor shape. Returns (rows, files_seen, dropped)."""
    run_dir = LP_PORTFOLIO_DIR / f"run_{run_number}"
    rows: list[dict] = []
    invalid = 0
    files_seen = 0
    if not run_dir.exists():
        return rows, files_seen, invalid
    for csv_path in sorted(run_dir.glob("*.csv")):
        files_seen += 1
        these = read_rows(csv_path)
        valid = [r for r in these if r.get("Scraping Method Used")]
        dropped = len(these) - len(valid)
        if dropped > 0:
            print(f"  WARN  {csv_path.name} (lp_portfolio): {dropped} rows missing 'Scraping Method Used' — DROPPED")
            invalid += dropped
        for r in valid:
            if (r.get("Investee Type") or "").strip().lower() != "company":
                continue
            rows.append(
                {
                    "Company Slug": r.get("Investee Slug") or "",
                    # Investor Name: stash for find-or-create. Combine doesn't
                    # always have an investor-name column; we fall back to slug.
                    "Investor Name": "",
                    "Investor Slug": r.get("LP Slug") or "",
                    "Round": "unknown",
                    "Round Date": r.get("Commitment Year") or "",
                    "Lead": "unknown",
                    "Source URL": r.get("Source URL") or "",
                    "Source Date": r.get("Source Date") or "",
                    "Scraping Method Used": r.get("Scraping Method Used") or "",
                    "_Investee Name": r.get("Investee Name") or "",
                }
            )
    return rows, files_seen, invalid


def combine(run_number: int) -> dict:
    primary_rows, primary_files, primary_dropped = _read_primary_rows(run_number)
    secondary_rows, secondary_files, secondary_dropped = _read_secondary_rows(run_number)

    if not primary_rows and not secondary_rows:
        print(f"  No rows to combine for run {run_number}.")
        return {
            "primary_files": primary_files,
            "secondary_files": secondary_files,
            "rows": 0,
        }

    audit_rows = primary_rows + [
        {h: r.get(h, "") for h in COMBINED_HEADERS} for r in secondary_rows
    ]
    write_rows(COMBINED_DIR / f"run_{run_number}" / "all.csv", COMBINED_HEADERS, audit_rows)

    inv_added = _update_investors(primary_rows + secondary_rows)
    cos_added = _update_companies(secondary_rows)
    edges_written = _rebuild_portco_investors(primary_rows + secondary_rows)

    return {
        "primary_files": primary_files,
        "secondary_files": secondary_files,
        "primary_rows": len(primary_rows),
        "secondary_rows": len(secondary_rows),
        "rows_dropped": primary_dropped + secondary_dropped,
        "investors_added": inv_added,
        "companies_added": cos_added,
        "portco_investors_edges": edges_written,
    }


def _update_investors(all_rows: list[dict]) -> int:
    raw_existing = {r["Investor Slug"]: r for r in read_rows(INVESTORS_CSV)}
    existing = {s: r for s, r in raw_existing.items() if not is_deprecated_investor_slug(s)}
    discovered_by_slug: dict[str, dict] = {}
    for row in all_rows:
        slug = canonicalize_investor_slug(row.get("Investor Slug"))
        if not slug:
            continue
        discovered_by_slug.setdefault(slug, row)

    output = [{h: r.get(h, "") for h in INVESTORS_HEADERS} for r in existing.values()]

    added = 0
    for slug, row in discovered_by_slug.items():
        if slug in existing:
            continue
        name = row.get("Investor Name") or slug
        output.append(
            {
                "Investor Name": name,
                "Investor Slug": slug,
                "Investor Type": _classify_investor_type(name),
                "Impact Focus": "unknown",
                "Website": "",
                "HQ Country": "",
                "AUM Bucket": "unknown",
                "Status": "active",
                "Notes": "Discovered via portco-investor scraper; awaiting operator review.",
            }
        )
        added += 1

    write_rows(INVESTORS_CSV, INVESTORS_HEADERS, output)
    return added


def _update_companies(secondary_rows: list[dict]) -> int:
    """Find-or-create company rows in portfolio_companies.csv for companies
    discovered via LP-portfolio scrapers. New companies land with
    Pipeline Status = pending_onboard for operator review (Lesson 32)."""
    existing = {r["Company Slug"]: r for r in read_rows(PORTFOLIO_COMPANIES_CSV)}

    discovered: dict[str, dict] = {}
    for row in secondary_rows:
        slug = row.get("Company Slug")
        if not slug or slug in existing or slug in discovered:
            continue
        discovered[slug] = row

    output = [{h: existing[s].get(h, "") for h in PORTFOLIO_COMPANIES_HEADERS} for s in existing]
    added = 0
    for slug, row in discovered.items():
        name = row.get("_Investee Name") or slug
        output.append(
            {
                "Company Name": name,
                "Company Slug": slug,
                "Website": "",
                "HQ Country": "",
                "Sector": "",
                "Stage": "",
                "Status": "active",
                "Pipeline Status": "pending_onboard",
                "Notes": "Discovered via LP-portfolio scraper; awaiting operator review.",
            }
        )
        added += 1

    write_rows(PORTFOLIO_COMPANIES_CSV, PORTFOLIO_COMPANIES_HEADERS, output)
    return added


def _rebuild_portco_investors(all_rows: list[dict]) -> int:
    """Rebuild portco_investors.csv. Dedup on (Company Slug, Investor Slug)."""
    seen: set[tuple[str, str]] = set()
    edges: list[dict] = []
    for row in all_rows:
        company_slug = row.get("Company Slug")
        investor_slug = canonicalize_investor_slug(row.get("Investor Slug"))
        if not company_slug or not investor_slug:
            continue
        key = (company_slug, investor_slug)
        if key in seen:
            continue
        seen.add(key)
        edges.append(
            {
                "Company Slug": company_slug,
                "Investor Slug": investor_slug,
                "Round": row.get("Round") or "unknown",
                "Round Date": row.get("Round Date") or "",
                "Lead": row.get("Lead") or "unknown",
                "Source URL": row.get("Source URL") or "",
                "Source Date": row.get("Source Date") or "",
                "Scraping Method Used": row.get("Scraping Method Used") or "",
            }
        )
    write_rows(PORTCO_INVESTORS_CSV, PORTCO_INVESTORS_HEADERS, edges)
    return len(edges)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=None)
    args = parser.parse_args()
    run_number = args.run if args.run is not None else _read_run_number()
    print(f"Combining portco-investor run {run_number} from {INDIVIDUAL_DIR} + {LP_PORTFOLIO_DIR}")
    stats = combine(run_number)
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
