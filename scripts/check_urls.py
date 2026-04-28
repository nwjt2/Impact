"""Batch URL health check for all content YAML files.

Extracts every http(s) URL from content/*.yml, runs concurrent HEAD/GET
requests with a realistic browser User-Agent, and writes a status report.

Output: scripts/url_check_report.txt (status, file:line, url, note).

Sites that block bots on HEAD often respond on GET — script falls back
automatically. Status legend:

  OK_200    — page works as expected
  REDIR     — followed redirect chain to a 200 (notes final URL)
  HTTP_403  — explicit forbidden (real broken OR bot-block; manual review)
  HTTP_404  — gone
  HTTP_5xx  — server error
  TIMEOUT   — no response within budget
  CONN_ERR  — DNS / TLS / connection failure
"""

from __future__ import annotations

import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parents[1]
CONTENT = REPO / "content"
REPORT = Path(__file__).resolve().parent / "url_check_report.txt"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 15.0
WORKERS = 12

URL_RE = re.compile(r"https?://[^\s\"'<>)]+")


def collect_urls() -> list[tuple[Path, int, str]]:
    """Return list of (file, line_no, url) tuples across all content YAMLs."""
    rows: list[tuple[Path, int, str]] = []
    for yml in sorted(CONTENT.glob("*.yml")):
        for i, line in enumerate(yml.read_text(encoding="utf-8").splitlines(), start=1):
            for m in URL_RE.findall(line):
                rows.append((yml, i, m.rstrip(".,;:")))
    return rows


def check(url: str) -> tuple[str, str]:
    """Return (status_string, note). Try HEAD first, GET on bot-block / 405."""
    try:
        with httpx.Client(
            headers=HEADERS, timeout=TIMEOUT, follow_redirects=True, http2=False
        ) as client:
            try:
                r = client.head(url)
            except httpx.HTTPError:
                r = None
            if r is None or r.status_code in (403, 405, 501) or r.status_code >= 500:
                # Some servers refuse HEAD; retry GET
                r = client.get(url)
            status = r.status_code
            final = str(r.url)
            note = f"final={final}" if final.rstrip("/") != url.rstrip("/") else ""
            if 200 <= status < 300:
                return ("REDIR" if note else "OK_200", note)
            if status == 403:
                return ("HTTP_403", note)
            if status == 404:
                return ("HTTP_404", note)
            return (f"HTTP_{status}", note)
    except httpx.TimeoutException:
        return ("TIMEOUT", "")
    except httpx.HTTPError as e:
        return ("CONN_ERR", str(e)[:120])
    except Exception as e:  # noqa: BLE001
        return ("CONN_ERR", str(e)[:120])


def main() -> int:
    rows = collect_urls()
    unique = list(dict.fromkeys(r[2] for r in rows))
    print(f"collected {len(rows)} url-occurrences ({len(unique)} unique)", file=sys.stderr)

    started = time.time()
    results: dict[str, tuple[str, str]] = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(check, u): u for u in unique}
        for n, fut in enumerate(as_completed(futures), 1):
            u = futures[fut]
            results[u] = fut.result()
            if n % 25 == 0:
                print(f"  ... {n}/{len(unique)} ({int(time.time() - started)}s)", file=sys.stderr)

    lines = []
    counts: dict[str, int] = {}
    for f, ln, u in rows:
        st, note = results[u]
        counts[st] = counts.get(st, 0) + 1
        lines.append(f"{st}\t{f.relative_to(REPO).as_posix()}:{ln}\t{u}\t{note}")

    summary = ["# URL health check"]
    summary.append(f"# {len(rows)} occurrences across {len(unique)} unique URLs")
    summary.append(f"# elapsed: {int(time.time() - started)}s")
    summary.append("# " + "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    summary.append("")
    REPORT.write_text("\n".join(summary + sorted(lines)) + "\n", encoding="utf-8")
    print("\n".join(summary), file=sys.stderr)
    print(f"wrote {REPORT}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
