"""Deeper-pass cleanup for AgDevCo investee names.

For each portfolio_companies.csv row whose Website is an agdevco.com detail
page, fetch the detail page and extract the company name from the <title>.
AgDevCo's title format is consistently "AgDevCo - <CompanyName>" so the
parse is reliable.

Writes proposals to network/scripts/clean_agdevco_proposals.md.
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import read_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402

PORTFOLIO_COMPANIES_CSV = REPO_ROOT / "network" / "catalogue" / "portfolio_companies.csv"
REPORT_PATH = REPO_ROOT / "network" / "scripts" / "clean_agdevco_proposals.md"

_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_AGDEVCO_PREFIX_RE = re.compile(r"^\s*AgDevCo\s*[-–—]\s*", re.IGNORECASE)


def fetch_title(url: str, *, timeout: float = 10.0) -> str:
    try:
        r = httpx.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            follow_redirects=True,
        )
        if r.status_code != 200:
            return ""
        m = _TITLE_RE.search(r.text)
        if not m:
            return ""
        return re.sub(r"\s+", " ", m.group(1)).strip()
    except Exception:
        return ""


def main() -> None:
    rows = read_rows(PORTFOLIO_COMPANIES_CSV)
    candidates = [r for r in rows if "agdevco.com" in (r.get("Website") or "").lower()]
    print(f"Candidates: {len(candidates)}")

    proposals = []
    for i, row in enumerate(candidates, 1):
        slug = row["Company Slug"]
        current = row["Company Name"]
        url = row["Website"]
        title = fetch_title(url)
        if not title:
            proposals.append((slug, current, "", "(fetch failed)", url))
        else:
            cleaned = _AGDEVCO_PREFIX_RE.sub("", title).strip()
            proposals.append((slug, current, cleaned, title, url))
        time.sleep(1.5)
        if i % 5 == 0:
            print(f"  {i}/{len(candidates)}...")

    lines = [
        "# AgDevCo Detail-Page Cleanup Proposals",
        "",
        f"{len(proposals)} candidates from agdevco.com detail pages.",
        "",
        "| # | Slug | Current | Proposed | Detail-page title |",
        "|---|---|---|---|---|",
    ]
    for i, (slug, current, proposed, title, url) in enumerate(proposals, 1):
        prop = proposed if proposed else "_(fetch failed)_"
        lines.append(f"| {i} | `{slug}` | {current} | **{prop}** | {title[:80]} |")
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nWrote report to {REPORT_PATH}")


if __name__ == "__main__":
    main()
