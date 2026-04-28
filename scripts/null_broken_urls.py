"""Null out confirmed-broken (HTTP 404) URLs in content YAMLs.

Reads scripts/url_check_report.txt, finds every HTTP_404 occurrence
(file:line, url), and rewrites the YAML so the URL value becomes `null`.

Conservative — only touches lines whose form is `<key>: <url>` where the
URL exactly matches what we found broken. Comments and surrounding
formatting are preserved untouched. Lines whose URL is embedded in
prose or notes are left alone (we'd need broader context to fix those
safely; the rendered links won't break since note text is plain).

Also applies a small set of targeted swaps where we know the working
substitute.

Run: python scripts/null_broken_urls.py
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CONTENT = REPO / "content"
REPORT = Path(__file__).resolve().parent / "url_check_report.txt"

# Targeted swaps: known working substitute for a known-broken URL.
# (Currently empty — DFC's /our-products redirects to /asset-classes
# correctly, so the existing URL still works in browsers.)
TARGETED_SWAPS: dict[str, str] = {}


def parse_report() -> list[tuple[Path, int, str]]:
    """Return list of (file_abs, line_no, url) for HTTP_404 occurrences."""
    out: list[tuple[Path, int, str]] = []
    for raw in REPORT.read_text(encoding="utf-8").splitlines():
        if not raw or raw.startswith("#"):
            continue
        cols = raw.split("\t")
        if len(cols) < 3:
            continue
        status, fileline, url = cols[0], cols[1], cols[2]
        if status != "HTTP_404":
            continue
        if ":" not in fileline:
            continue
        rel, ln = fileline.rsplit(":", 1)
        out.append((REPO / rel, int(ln), url))
    return out


KEY_VAL_RE = re.compile(r"^(\s*[\w_]+:\s*)(\S.*)$")


def patch_file(path: Path, urls_to_null: set[str]) -> int:
    """Null every line in the file whose value is exactly one of the URLs.

    Line numbers from the report can drift if other edits have happened
    since, so we re-scan the whole file by URL match.
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=False)
    changed = 0
    for idx, line in enumerate(lines):
        m = KEY_VAL_RE.match(line)
        if not m:
            continue
        prefix, value = m.group(1), m.group(2).strip()
        if value not in urls_to_null:
            continue
        if value in TARGETED_SWAPS:
            lines[idx] = prefix + TARGETED_SWAPS[value]
        else:
            lines[idx] = prefix + "null"
        changed += 1
    if changed:
        path.write_text("\n".join(lines) + ("\n" if text.endswith("\n") else ""), encoding="utf-8")
    return changed


def main() -> int:
    occ = parse_report()
    by_file: dict[Path, set[str]] = {}
    for f, _ln, url in occ:
        by_file.setdefault(f, set()).add(url)

    total = 0
    for f, urls in sorted(by_file.items()):
        n = patch_file(f, urls)
        total += n
        print(f"  {f.relative_to(REPO).as_posix()}: nulled {n}")
    print(f"\nTotal lines patched: {total}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
