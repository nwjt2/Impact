"""Apply approved Company Name changes to portfolio_companies.csv.

Operator-driven. Pass changes as a CSV via stdin or pass --file.
CSV format: Company Slug,New Company Name
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import read_rows, write_rows  # noqa: E402

PORTFOLIO_COMPANIES_CSV = REPO_ROOT / "network" / "catalogue" / "portfolio_companies.csv"

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


def apply_changes(changes: dict[str, str]) -> tuple[int, list[str]]:
    rows = read_rows(PORTFOLIO_COMPANIES_CSV)
    applied = 0
    not_found = []
    for slug, new_name in changes.items():
        match = next((r for r in rows if r["Company Slug"] == slug), None)
        if not match:
            not_found.append(slug)
            continue
        if match["Company Name"] == new_name:
            continue
        match["Company Name"] = new_name
        applied += 1
    write_rows(PORTFOLIO_COMPANIES_CSV, PORTFOLIO_COMPANIES_HEADERS, rows)
    return applied, not_found


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, default=None, help="CSV file with two columns: slug,new_name")
    args = parser.parse_args()
    if args.file:
        with open(args.file, encoding="utf-8") as f:
            reader = csv.reader(f)
            changes = {row[0]: row[1] for row in reader if len(row) >= 2}
    else:
        reader = csv.reader(sys.stdin)
        changes = {row[0]: row[1] for row in reader if len(row) >= 2}
    applied, not_found = apply_changes(changes)
    print(f"Applied {applied} renames. {len(not_found)} not found: {not_found}")


if __name__ == "__main__":
    main()
