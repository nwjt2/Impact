"""Combine per-fund LP scraper output into investors.csv + fund_lps.csv.

Reads:
  - network/fund_lp_scraping/individual_fund_lps/run_<N>/*.csv         (primary)
  - network/lp_portfolio_scraping/individual_lp_portfolios/run_<N>/*.csv
    (secondary; rows with Investee Type=fund are folded in as LP→fund edges)
Writes:
  network/fund_lp_scraping/combined_fund_lps/run_<N>/all_lps.csv  (audit, gitignored)
  network/catalogue/investors.csv                                  (find-or-create)
  network/catalogue/impact_funds.csv                               (find-or-create — for funds discovered via LP-portfolio scrapers only)
  network/dashboard_prep/fund_lps.csv                              (rebuilt each run)

Symmetric to combine_fund_portfolios.py.

Lesson 1: rows missing Scraping Method Used are dropped, loudly.
Lesson 32: new investors land as Status = active (default lifecycle), with
Investor Type = other and Impact Focus = unknown for operator review.
Lesson 32: new funds discovered via LP-portfolio scrapers land as
Pipeline Status = pending_onboard with INGO Slug empty for operator review.
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
from network.utils.investor_classifier import classify_investor_type as _classify_investor_type  # noqa: E402

INDIVIDUAL_DIR = REPO_ROOT / "network" / "fund_lp_scraping" / "individual_fund_lps"
COMBINED_DIR = REPO_ROOT / "network" / "fund_lp_scraping" / "combined_fund_lps"
LP_PORTFOLIO_DIR = REPO_ROOT / "network" / "lp_portfolio_scraping" / "individual_lp_portfolios"
INVESTORS_CSV = REPO_ROOT / "network" / "catalogue" / "investors.csv"
IMPACT_FUNDS_CSV = REPO_ROOT / "network" / "catalogue" / "impact_funds.csv"
FUND_LPS_CSV = REPO_ROOT / "network" / "dashboard_prep" / "fund_lps.csv"
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

IMPACT_FUNDS_HEADERS = [
    "Fund Name",
    "Fund Slug",
    "INGO Slug",
    "Website",
    "Founded Year",
    "AUM (USD M)",
    "Thesis Tags",
    "Status",
    "Portfolio Page URL",
    "LP Page URL",
    "Fund Type",
    "Pipeline Status",
    "Notes",
]

FUND_LPS_HEADERS = [
    "Fund Slug",
    "LP Slug",
    "Commitment Year",
    "Source URL",
    "Source Date",
    "Confidence",
    "Scraping Method Used",
]

COMBINED_HEADERS = [
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

# Confidence ranking — used to choose the best row when multiple sources
# report the same (Fund Slug, LP Slug) edge.
_CONFIDENCE_RANK = {"confirmed": 3, "likely": 2, "speculative": 1, "": 0}


# Operator-edited fields on investors.csv that the combine step preserves.
PRESERVE_FIELDS = (
    "Investor Name",
    "Investor Type",
    "Impact Focus",
    "Website",
    "HQ Country",
    "AUM Bucket",
    "Status",
    "Notes",
)


def _read_run_number() -> int:
    if not RUN_STATE_JSON.exists():
        return 1
    state = json.loads(RUN_STATE_JSON.read_text(encoding="utf-8"))
    return max(1, int(state.get("current_run") or 1))


def _read_primary_rows(run_number: int) -> tuple[list[dict], int, int]:
    """Read primary fund-lp scraper rows. Returns (rows, files_seen, dropped)."""
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
    """Read LP-portfolio scraper rows, filter to Investee Type=fund, map to fund-lp shape.

    Returns (rows in fund-lp shape, files_seen, dropped).
    """
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
            if (r.get("Investee Type") or "").strip().lower() != "fund":
                continue
            # Map LP-portfolio row → fund-lp shape.
            # Investee Name → LP Name? No: in the fund-lp shape, "LP" is the funding-side
            # entity (the LP), and "Fund" is the receiving fund. The LP-portfolio scraper
            # captures (LP Slug, Investee Slug=fund_slug). So:
            #   Fund Slug ← Investee Slug
            #   LP Slug   ← LP Slug
            #   LP Name   ← we do not have this column directly, but combine_fund_lps
            #               only uses LP Name to seed investors.csv when the slug is
            #               not already there. The LP itself is the SCRAPER's subject,
            #               so it is virtually always already in investors.csv.
            #               We pass "" here; _update_investors will not insert a new
            #               investor unless slug is unknown, in which case it falls
            #               back to slug-as-name.
            rows.append(
                {
                    "Fund Slug": r.get("Investee Slug") or "",
                    "INGO Slug": "",  # unknown to LP-portfolio scrapers
                    "LP Name": "",
                    "LP Slug": r.get("LP Slug") or "",
                    "Commitment Year": r.get("Commitment Year") or "",
                    "Source URL": r.get("Source URL") or "",
                    "Source Date": r.get("Source Date") or "",
                    "Confidence": r.get("Confidence") or "confirmed",
                    "Scraping Method Used": r.get("Scraping Method Used") or "",
                    # Stash investee name so _update_funds can name-find-or-create.
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

    # The audit file holds the combined-shape rows (LP Name retained where
    # available — primary rows have it, secondary rows don't).
    all_rows = primary_rows + [
        {h: r.get(h, "") for h in COMBINED_HEADERS} for r in secondary_rows
    ]
    write_rows(COMBINED_DIR / f"run_{run_number}" / "all_lps.csv", COMBINED_HEADERS, all_rows)

    inv_added = _update_investors(primary_rows + secondary_rows)
    funds_added = _update_funds(secondary_rows)
    edges_written = _rebuild_fund_lps(primary_rows + secondary_rows)

    return {
        "primary_files": primary_files,
        "secondary_files": secondary_files,
        "primary_rows": len(primary_rows),
        "secondary_rows": len(secondary_rows),
        "rows_dropped": primary_dropped + secondary_dropped,
        "investors_added": inv_added,
        "funds_added": funds_added,
        "fund_lps_edges": edges_written,
    }


def _update_investors(all_rows: list[dict]) -> int:
    raw_existing = {r["Investor Slug"]: r for r in read_rows(INVESTORS_CSV)}
    # Drop rows whose slug is now a deprecated alias — the canonical row
    # (or one we're about to add) supersedes them.
    existing = {s: r for s, r in raw_existing.items() if not is_deprecated_investor_slug(s)}
    discovered_by_slug: dict[str, dict] = {}
    for row in all_rows:
        slug = canonicalize_investor_slug(row.get("LP Slug"))
        if not slug:
            continue
        discovered_by_slug.setdefault(slug, row)

    output: list[dict] = []
    # Preserve all existing rows verbatim — investors.csv was seeded from
    # entities.yml and we don't want to lose those.
    for slug, existing_row in existing.items():
        output.append({h: existing_row.get(h, "") for h in INVESTORS_HEADERS})

    added = 0
    for slug, row in discovered_by_slug.items():
        if slug in existing:
            continue
        name = row.get("LP Name") or slug
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
                "Notes": "Discovered via LP scraper; awaiting operator review.",
            }
        )
        added += 1

    write_rows(INVESTORS_CSV, INVESTORS_HEADERS, output)
    return added


def _update_funds(secondary_rows: list[dict]) -> int:
    """Find-or-create fund rows in impact_funds.csv for funds discovered via LP-portfolio scrapers.

    Existing funds are preserved verbatim. New funds land with Pipeline Status =
    pending_onboard, INGO Slug = "" (unknown — operator review). Lesson 32.
    """
    existing = {r["Fund Slug"]: r for r in read_rows(IMPACT_FUNDS_CSV)}

    discovered: dict[str, dict] = {}
    for row in secondary_rows:
        slug = row.get("Fund Slug")
        if not slug or slug in existing or slug in discovered:
            continue
        discovered[slug] = row

    if not discovered:
        # No changes — still rewrite to keep formatting deterministic.
        output = [{h: existing[s].get(h, "") for h in IMPACT_FUNDS_HEADERS} for s in existing]
        write_rows(IMPACT_FUNDS_CSV, IMPACT_FUNDS_HEADERS, output)
        return 0

    output = [{h: existing[s].get(h, "") for h in IMPACT_FUNDS_HEADERS} for s in existing]
    added = 0
    for slug, row in discovered.items():
        name = row.get("_Investee Name") or slug
        output.append(
            {
                "Fund Name": name,
                "Fund Slug": slug,
                "INGO Slug": "",
                "Website": "",
                "Founded Year": "",
                "AUM (USD M)": "",
                "Thesis Tags": "",
                "Status": "deployed",
                "Portfolio Page URL": "",
                "LP Page URL": "",
                "Fund Type": "unclassified",
                "Pipeline Status": "pending_onboard",
                "Notes": "Discovered via LP-portfolio scraper; awaiting operator review.",
            }
        )
        added += 1

    write_rows(IMPACT_FUNDS_CSV, IMPACT_FUNDS_HEADERS, output)
    return added


def _rebuild_fund_lps(all_rows: list[dict]) -> int:
    """Rebuild fund_lps.csv. Dedup on (Fund Slug, LP Slug); prefer highest Confidence."""
    best_by_key: dict[tuple[str, str], dict] = {}
    for row in all_rows:
        fund_slug = row.get("Fund Slug")
        lp_slug = canonicalize_investor_slug(row.get("LP Slug"))
        if not fund_slug or not lp_slug:
            continue
        key = (fund_slug, lp_slug)
        existing = best_by_key.get(key)
        if existing is None:
            best_by_key[key] = row
            continue
        # Prefer highest-confidence row.
        if _CONFIDENCE_RANK.get(row.get("Confidence") or "", 0) > _CONFIDENCE_RANK.get(
            existing.get("Confidence") or "", 0
        ):
            best_by_key[key] = row

    edges: list[dict] = []
    for (fund_slug, lp_slug), row in best_by_key.items():
        edges.append(
            {
                "Fund Slug": fund_slug,
                "LP Slug": lp_slug,  # already canonicalized via alias map at key time
                "Commitment Year": row.get("Commitment Year") or "",
                "Source URL": row.get("Source URL") or "",
                "Source Date": row.get("Source Date") or "",
                "Confidence": row.get("Confidence") or "confirmed",
                "Scraping Method Used": row.get("Scraping Method Used") or "",
            }
        )
    write_rows(FUND_LPS_CSV, FUND_LPS_HEADERS, edges)
    return len(edges)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=None)
    args = parser.parse_args()
    run_number = args.run if args.run is not None else _read_run_number()
    print(f"Combining LP run {run_number} from {INDIVIDUAL_DIR} + {LP_PORTFOLIO_DIR}")
    stats = combine(run_number)
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
