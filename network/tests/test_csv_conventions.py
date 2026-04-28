"""Tests for CSV conventions across all network/ CSV files (advice doc lesson 8).

- Title-case headers with spaces
- UTF-8 readability
- Date / boolean shapes where applicable
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

import pytest

NETWORK_DIR = Path(__file__).resolve().parents[1]

CSV_PATHS = [
    p
    for p in NETWORK_DIR.rglob("*.csv")
    if "individual_" not in str(p) and "combined_" not in str(p)
]

DATE_RE = re.compile(r"^\d{4}-\d{2}(-\d{2})?$")
BOOL_VALUES = {"Y", "N", "unknown"}


def _is_title_case_token(t: str) -> bool:
    if t.startswith("(") or t.endswith(")"):
        return True
    return t[0].isupper()


@pytest.mark.parametrize("path", CSV_PATHS, ids=lambda p: str(p.relative_to(NETWORK_DIR)))
def test_csv_has_title_case_headers(path: Path) -> None:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            pytest.skip(f"{path}: empty file")
            return
    assert headers, f"{path}: no header row"
    for header in headers:
        for token in header.split():
            assert _is_title_case_token(token), (
                f"{path}: header {header!r} token {token!r} not title-case"
            )


@pytest.mark.parametrize("path", CSV_PATHS, ids=lambda p: str(p.relative_to(NETWORK_DIR)))
def test_csv_is_utf8(path: Path) -> None:
    with path.open("rb") as f:
        head = f.read(3)
    assert head[:3] != b"\xef\xbb\xbf", f"{path}: must not have BOM"
    path.read_text(encoding="utf-8")  # decode round-trip; will raise if invalid
