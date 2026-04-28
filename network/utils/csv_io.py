"""CSV read/write helpers that enforce repo conventions (advice doc 06, lesson 8).

- Title-case headers with spaces (e.g. "Fund Name", "Investor Slug").
- Boolean values: Y / N / unknown.
- Date format: YYYY-MM-DD (or YYYY-MM if day unknown).
- UTF-8, no BOM.
- Pipe-separated lists for multi-value columns.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

BOOL_VALUES = {"Y", "N", "unknown"}
DATE_RE = re.compile(r"^\d{4}-\d{2}(-\d{2})?$")


def write_rows(path: Path | str, fieldnames: list[str], rows: list[dict]) -> int:
    """Write rows to a CSV with the given fieldnames in declared order."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for row in rows:
            w.writerow({k: ("" if row.get(k) is None else row[k]) for k in fieldnames})
    return len(rows)


def write_header_only(path: Path | str, fieldnames: list[str]) -> None:
    """Initialize an empty CSV with just its header row."""
    write_rows(path, fieldnames, [])


def read_rows(path: Path | str) -> list[dict]:
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def is_title_case_header(header: str) -> bool:
    """A header is acceptable if every space-separated token starts with an
    uppercase letter (or is a parenthesized unit like "(USD M)").
    """
    for token in header.split():
        if token.startswith("("):
            continue
        if not token[0].isupper():
            return False
    return True
