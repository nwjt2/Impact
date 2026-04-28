"""Combine per-fund scraper output into the catalogue + edge tables.

Reads:  network/fund_portfolio_scraping/individual_fund_portfolios/run_<N>/*.csv
Writes:
  network/fund_portfolio_scraping/combined_fund_portfolios/run_<N>/all_portcos.csv  (audit, gitignored)
  network/catalogue/portfolio_companies.csv                                          (find-or-create)
  network/dashboard_prep/fund_investments.csv                                        (rebuilt each run)

Lesson 1 (advice doc): rows missing `Scraping Method Used` are dropped — but
loudly, with a warning. The doc said "silently drops" but that's the bug we
were warned about; we surface it.

Lesson 32: new portcos land as Pipeline Status = pending_onboard. Operator
review promotes them to `active` (and writes a fund_investment_scraper for
that portco if we want to scrape its cap table — Pipeline 3 territory).

Lesson 4: portfolio_companies.csv is the ONE source of truth for portcos.
This script preserves operator-edited fields (Sector, Stage, Status, Notes)
when a slug already exists.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import read_rows, write_rows  # noqa: E402

INDIVIDUAL_DIR = REPO_ROOT / "network" / "fund_portfolio_scraping" / "individual_fund_portfolios"
COMBINED_DIR = REPO_ROOT / "network" / "fund_portfolio_scraping" / "combined_fund_portfolios"
PORTFOLIO_COMPANIES_CSV = REPO_ROOT / "network" / "catalogue" / "portfolio_companies.csv"
FUND_INVESTMENTS_CSV = REPO_ROOT / "network" / "dashboard_prep" / "fund_investments.csv"
RUN_STATE_JSON = REPO_ROOT / "network" / "run_state.json"

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

FUND_INVESTMENTS_HEADERS = [
    "Fund Slug",
    "Company Slug",
    "Round",
    "Round Date",
    "Lead",
    "Source URL",
    "Source Date",
    "Scraping Method Used",
]

COMBINED_HEADERS = [
    "Fund Slug",
    "INGO Slug",
    "Company Name",
    "Company Slug",
    "Company Website",
    "Country",
    "Round",
    "Round Date",
    "Lead",
    "Source URL",
    "Source Date",
    "Scraping Method Used",
    "Notes",
]

# Operator-edited fields on portfolio_companies.csv that the combine step
# MUST preserve when a slug already exists. Once an operator has cleaned up a
# Company Name (e.g. "Satellitesonfire" -> "Satellites on Fire"), we don't
# want the scraper's naive name to overwrite it on the next run.
# Website is intentionally NOT preserved — if a portco's URL changes upstream,
# the scraper's value is the truth.
PRESERVE_FIELDS = ("Company Name", "Sector", "Stage", "Status", "Pipeline Status", "Notes")


def _read_run_number() -> int:
    if not RUN_STATE_JSON.exists():
        return 1
    state = json.loads(RUN_STATE_JSON.read_text(encoding="utf-8"))
    return max(1, int(state.get("current_run") or 1))


def combine(run_number: int) -> dict:
    run_dir = INDIVIDUAL_DIR / f"run_{run_number}"
    if not run_dir.exists():
        raise FileNotFoundError(f"No scraper output at {run_dir}")

    all_rows: list[dict] = []
    invalid_count = 0
    files_seen = 0

    for csv_path in sorted(run_dir.glob("*.csv")):
        files_seen += 1
        rows = read_rows(csv_path)
        valid = [r for r in rows if r.get("Scraping Method Used")]
        dropped = len(rows) - len(valid)
        if dropped > 0:
            print(
                f"  WARN  {csv_path.name}: {dropped} rows missing 'Scraping Method Used' "
                f"— DROPPED (lesson 1)"
            )
            invalid_count += dropped
        all_rows.extend(valid)

    if not all_rows:
        print(f"  No rows to combine for run {run_number}.")
        return {"files": files_seen, "rows": 0}

    combined_run_dir = COMBINED_DIR / f"run_{run_number}"
    write_rows(combined_run_dir / "all_portcos.csv", COMBINED_HEADERS, all_rows)

    pc_added, pc_kept = _update_portfolio_companies(all_rows)
    edges_written = _rebuild_fund_investments(all_rows)
    orphans_dropped = _drop_orphan_portfolio_companies()

    return {
        "files": files_seen,
        "rows": len(all_rows),
        "rows_dropped": invalid_count,
        "portfolio_companies_added": pc_added,
        "portfolio_companies_kept": pc_kept,
        "portfolio_companies_orphans_dropped": orphans_dropped,
        "fund_investments_edges": edges_written,
    }


def _drop_orphan_portfolio_companies() -> int:
    """Drop portfolio_companies rows that no longer have a fund_investments edge.

    Phase 3 limitation — when Pipeline 3 (portco_investors) lands, a portco
    might legitimately exist with no fund_investments edge (it could be a
    co-investee). Until then, every portco should trace to at least one fund.
    """
    portcos = read_rows(PORTFOLIO_COMPANIES_CSV)
    edges = read_rows(FUND_INVESTMENTS_CSV)
    referenced = {e["Company Slug"] for e in edges if e.get("Company Slug")}
    kept = [r for r in portcos if r["Company Slug"] in referenced]
    dropped = len(portcos) - len(kept)
    if dropped > 0:
        write_rows(PORTFOLIO_COMPANIES_CSV, PORTFOLIO_COMPANIES_HEADERS, kept)
    return dropped


def _update_portfolio_companies(all_rows: list[dict]) -> tuple[int, int]:
    """Find-or-create rows in portfolio_companies.csv. Returns (added, kept)."""
    existing = {r["Company Slug"]: r for r in read_rows(PORTFOLIO_COMPANIES_CSV)}

    # Group scraped rows by slug — first occurrence wins for descriptive fields.
    scraped_by_slug: dict[str, dict] = {}
    for row in all_rows:
        slug = row.get("Company Slug")
        if not slug:
            continue
        scraped_by_slug.setdefault(slug, row)

    output: list[dict] = []

    # Refresh any existing entries with newly-scraped descriptive fields,
    # but PRESERVE operator-edited fields per PRESERVE_FIELDS.
    for slug, existing_row in existing.items():
        if slug in scraped_by_slug:
            scraped = scraped_by_slug[slug]
            refreshed = {
                "Company Slug": slug,
                "Website": scraped.get("Company Website") or existing_row.get("Website", ""),
                "HQ Country": scraped.get("Country") or existing_row.get("HQ Country", ""),
            }
            for f in PRESERVE_FIELDS:
                refreshed[f] = existing_row.get(f, "") or scraped.get(f, "")
            # If operator hasn't set Company Name, fall back to the latest scrape.
            if not refreshed.get("Company Name"):
                refreshed["Company Name"] = scraped.get("Company Name") or ""
            output.append(refreshed)
        else:
            # Not in this run's scrape; keep existing row verbatim
            output.append({h: existing_row.get(h, "") for h in PORTFOLIO_COMPANIES_HEADERS})

    # Append newly discovered portcos
    added = 0
    for slug, row in scraped_by_slug.items():
        if slug in existing:
            continue
        output.append(
            {
                "Company Name": row.get("Company Name") or "",
                "Company Slug": slug,
                "Website": row.get("Company Website") or "",
                "HQ Country": row.get("Country") or "",
                "Sector": "",
                "Stage": "",
                "Status": "active",
                "Pipeline Status": "pending_onboard",
                "Notes": "",
            }
        )
        added += 1

    write_rows(PORTFOLIO_COMPANIES_CSV, PORTFOLIO_COMPANIES_HEADERS, output)
    return added, len(existing)


def _rebuild_fund_investments(all_rows: list[dict]) -> int:
    """Rebuild fund_investments.csv from this run's scraper output.

    Each unique (Fund Slug, Company Slug) is one edge. Edges are NOT
    incremental — we rebuild from the latest scrape on each run. Historical
    edges live in the timeline (Phase 6).
    """
    seen: set[tuple[str, str]] = set()
    edges: list[dict] = []
    for row in all_rows:
        fund_slug = row.get("Fund Slug")
        company_slug = row.get("Company Slug")
        if not fund_slug or not company_slug:
            continue
        key = (fund_slug, company_slug)
        if key in seen:
            continue
        seen.add(key)
        edges.append(
            {
                "Fund Slug": fund_slug,
                "Company Slug": company_slug,
                "Round": row.get("Round") or "unknown",
                "Round Date": row.get("Round Date") or "",
                "Lead": row.get("Lead") or "unknown",
                "Source URL": row.get("Source URL") or "",
                "Source Date": row.get("Source Date") or "",
                "Scraping Method Used": row.get("Scraping Method Used") or "",
            }
        )

    write_rows(FUND_INVESTMENTS_CSV, FUND_INVESTMENTS_HEADERS, edges)
    return len(edges)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=None, help="Run number (default: latest)")
    args = parser.parse_args()
    run_number = args.run if args.run is not None else _read_run_number()

    print(f"Combining run {run_number} from {INDIVIDUAL_DIR}")
    stats = combine(run_number)
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
