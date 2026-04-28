"""Combine per-fund LP scraper output into investors.csv + fund_lps.csv.

Reads:  network/fund_lp_scraping/individual_fund_lps/run_<N>/*.csv
Writes:
  network/fund_lp_scraping/combined_fund_lps/run_<N>/all_lps.csv  (audit, gitignored)
  network/catalogue/investors.csv                                  (find-or-create)
  network/dashboard_prep/fund_lps.csv                              (rebuilt each run)

Symmetric to combine_fund_portfolios.py.

Lesson 1: rows missing Scraping Method Used are dropped, loudly.
Lesson 32: new investors land as Status = active (default lifecycle), with
Investor Type = other and Impact Focus = unknown for operator review.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import read_rows, write_rows  # noqa: E402

INDIVIDUAL_DIR = REPO_ROOT / "network" / "fund_lp_scraping" / "individual_fund_lps"
COMBINED_DIR = REPO_ROOT / "network" / "fund_lp_scraping" / "combined_fund_lps"
INVESTORS_CSV = REPO_ROOT / "network" / "catalogue" / "investors.csv"
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


def _classify_investor_type(name: str) -> str:
    """Best-guess investor type for newly discovered LPs.

    Operator review can refine. Heuristic only.
    """
    n = name.lower()
    if "family" in n and ("foundation" in n or "fund" in n):
        return "family-office"
    if "foundation" in n:
        return "foundation"
    if "missionary" in n or "sister" in n or "religious" in n:
        return "foundation"
    if "investment services" in n or "charitable" in n:
        return "foundation"
    if "advisors" in n or "advisor" in n:
        return "other"
    if "development finance" in n or "dfi" in n:
        return "dfi"
    return "other"


def combine(run_number: int) -> dict:
    run_dir = INDIVIDUAL_DIR / f"run_{run_number}"
    if not run_dir.exists():
        raise FileNotFoundError(f"No LP scraper output at {run_dir}")

    all_rows: list[dict] = []
    invalid_count = 0
    files_seen = 0

    for csv_path in sorted(run_dir.glob("*.csv")):
        files_seen += 1
        rows = read_rows(csv_path)
        valid = [r for r in rows if r.get("Scraping Method Used")]
        dropped = len(rows) - len(valid)
        if dropped > 0:
            print(f"  WARN  {csv_path.name}: {dropped} rows missing 'Scraping Method Used' — DROPPED")
            invalid_count += dropped
        all_rows.extend(valid)

    if not all_rows:
        print(f"  No rows to combine for run {run_number}.")
        return {"files": files_seen, "rows": 0}

    write_rows(COMBINED_DIR / f"run_{run_number}" / "all_lps.csv", COMBINED_HEADERS, all_rows)

    inv_added = _update_investors(all_rows)
    edges_written = _rebuild_fund_lps(all_rows)

    return {
        "files": files_seen,
        "rows": len(all_rows),
        "rows_dropped": invalid_count,
        "investors_added": inv_added,
        "fund_lps_edges": edges_written,
    }


def _update_investors(all_rows: list[dict]) -> int:
    existing = {r["Investor Slug"]: r for r in read_rows(INVESTORS_CSV)}
    discovered_by_slug: dict[str, dict] = {}
    for row in all_rows:
        slug = row.get("LP Slug")
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


def _rebuild_fund_lps(all_rows: list[dict]) -> int:
    seen: set[tuple[str, str]] = set()
    edges: list[dict] = []
    for row in all_rows:
        fund_slug = row.get("Fund Slug")
        lp_slug = row.get("LP Slug")
        if not fund_slug or not lp_slug:
            continue
        key = (fund_slug, lp_slug)
        if key in seen:
            continue
        seen.add(key)
        edges.append(
            {
                "Fund Slug": fund_slug,
                "LP Slug": lp_slug,
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
    print(f"Combining LP run {run_number} from {INDIVIDUAL_DIR}")
    stats = combine(run_number)
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
