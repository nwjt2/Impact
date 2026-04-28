"""Operator-review pass: propose clean Company Names by fetching each
portco's website and parsing the <title> tag.

Reads:  network/catalogue/portfolio_companies.csv
Writes: network/scripts/clean_company_names_proposals.md (review report)

Does NOT auto-apply. Intentional separation of concerns:
- This script proposes.
- Operator reviews.
- Operator (or a follow-up `--apply` invocation) writes back.

Heuristic for "ugly name":
- All-lowercase (after first cap) AND no spaces AND > 6 chars
  e.g. "Satellitesonfire", "Poweredbypeople", "Openforestprotocol"
- Trailing acronym in name slug (URL slug had a parenthesised acronym appended)
  e.g. "Pee Pee Tanzania Limited Pptl", "Saise Farming Enterprises Ltd Sfel"
- Single-word name with len <= 4 (might be uppercase acronym)
  e.g. "Eca", "Emf", "Feav", "A2p"
"""
from __future__ import annotations

import argparse
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
REPORT_PATH = REPO_ROOT / "network" / "scripts" / "clean_company_names_proposals.md"

# Hosts whose URLs are detail pages on the FUNDER's site, not the company's
# own site. The <title> there is "Company X | Funder" — useful but the funder
# brand needs to be stripped.
FUNDER_DETAIL_HOSTS = {
    "agdevco.com": "AgDevCo",
    "kiva.org": "Kiva",
    "mercycorpsventures.com": "Mercy Corps Ventures",
}

# Title separators in priority order.
_SEPS = [" - ", " | ", " :: ", " — ", " – ", " : ", " · "]

# Common boilerplate to strip.
_BOILERPLATE = re.compile(
    r"^(home|welcome to|official site|official website)\s*[:\-|]?\s*",
    re.IGNORECASE,
)


def looks_ugly(name: str) -> bool:
    """Heuristic for names that probably need a human-readable correction."""
    if not name:
        return True
    n = name.strip()
    parts = n.split()
    # All-lowercase (after the first letter) AND no spaces AND not too short.
    # Catches "Satellitesonfire", "Openforestprotocol".
    if " " not in n and len(n) > 6 and n[0].isupper() and n[1:].islower():
        return True
    # Single-word name <= 4 chars; likely acronym that lost its caps.
    if " " not in n and len(n) <= 4:
        return True
    # Trailing token looks like an acronym appended via URL slug:
    #   "Pee Pee Tanzania Limited Pptl" -> last token Pptl
    #   "Quinta Da Bela Vista Limitada Qbv" -> Qbv
    if len(parts) >= 3:
        last = parts[-1]
        if 2 <= len(last) <= 5 and last[0].isupper() and last[1:].islower():
            # Plausibly an acronym lower-cased by title-casing
            # (real words rarely match "Capitalised then lowercase 1-4 chars")
            # Only flag if at least one earlier token is a legal-form word
            legal_forms = {"limited", "ltd", "limitada", "inc", "llc", "plc", "sa"}
            earlier_lower = {p.lower() for p in parts[:-1]}
            if earlier_lower & legal_forms:
                return True
    return False


def extract_title(html: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    title = m.group(1)
    # Decode common HTML entities + collapse whitespace
    title = title.replace("&amp;", "&").replace("&#039;", "'").replace("&quot;", '"')
    title = title.replace("&apos;", "'").replace("&ndash;", "–").replace("&mdash;", "—")
    title = re.sub(r"\s+", " ", title).strip()
    return title


def propose_from_title(title: str, fallback: str) -> str:
    """Take everything before the first separator. Strip boilerplate."""
    if not title:
        return fallback
    cleaned = _BOILERPLATE.sub("", title)
    # Take prefix before the first separator
    earliest = len(cleaned)
    for sep in _SEPS:
        i = cleaned.find(sep)
        if 0 < i < earliest:
            earliest = i
    proposed = cleaned[:earliest].strip()
    # Don't return something nonsensical
    if len(proposed) < 2 or len(proposed) > 80:
        return fallback
    return proposed


def host_of(url: str) -> str:
    m = re.match(r"https?://([^/]+)", url)
    if not m:
        return ""
    return m.group(1).lower().removeprefix("www.")


def is_funder_detail(url: str) -> bool:
    h = host_of(url)
    for funder_host in FUNDER_DETAIL_HOSTS:
        if h == funder_host or h.endswith("." + funder_host):
            return True
    return False


def fetch_title(url: str, *, timeout: float = 10.0) -> tuple[str, str]:
    """Returns (title, error). title="" on failure."""
    try:
        r = httpx.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            follow_redirects=True,
        )
        if r.status_code != 200:
            return "", f"HTTP {r.status_code}"
        return extract_title(r.text), ""
    except Exception as e:
        return "", f"{type(e).__name__}: {e}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N candidates (debugging)",
    )
    parser.add_argument(
        "--include-funder-pages",
        action="store_true",
        help="Also fetch funder detail pages (slower; off by default).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.5,
        help="Seconds to sleep between fetches (politeness)",
    )
    args = parser.parse_args()

    rows = read_rows(PORTFOLIO_COMPANIES_CSV)
    if not rows:
        print(f"No rows in {PORTFOLIO_COMPANIES_CSV}; nothing to clean.")
        return

    # Identify candidates
    candidates = []
    for row in rows:
        name = row.get("Company Name", "")
        url = row.get("Website", "")
        slug = row.get("Company Slug", "")
        if not looks_ugly(name):
            continue
        if not url:
            candidates.append((row, "no-website"))
            continue
        if is_funder_detail(url) and not args.include_funder_pages:
            candidates.append((row, "funder-detail"))
            continue
        candidates.append((row, "fetch"))

    if args.limit:
        candidates = candidates[: args.limit]

    print(f"Found {len(candidates)} candidates needing cleanup of {len(rows)} total rows.")

    proposals = []
    fetch_n = sum(1 for _, action in candidates if action == "fetch")
    print(f"Will fetch {fetch_n} URLs (~{fetch_n * args.sleep:.0f}s with sleep).")

    for i, (row, action) in enumerate(candidates, 1):
        slug = row["Company Slug"]
        name = row["Company Name"]
        url = row.get("Website", "")
        if action == "no-website":
            proposals.append((slug, name, "", "(no website to fetch)", url, action))
            continue
        if action == "funder-detail":
            proposals.append((slug, name, "", f"(funder detail page: {host_of(url)})", url, action))
            continue
        title, err = fetch_title(url)
        if err:
            proposals.append((slug, name, "", f"(fetch error: {err})", url, action))
        else:
            proposed = propose_from_title(title, fallback=name)
            proposals.append((slug, name, proposed, title, url, action))
        time.sleep(args.sleep)
        if i % 5 == 0:
            print(f"  {i}/{len(candidates)}...")

    # Write Markdown report
    lines = [
        "# Company Name Cleanup Proposals",
        "",
        f"Generated by `network/scripts/clean_company_names.py`. {len(candidates)} candidates of {len(rows)} total rows.",
        "",
        "Operator: review the **Proposed** column. Edit the markdown if needed, then approve. The applier will write to `network/catalogue/portfolio_companies.csv`.",
        "",
        "| # | Slug | Current | Proposed | Title (or note) | URL |",
        "|---|---|---|---|---|---|",
    ]
    for i, (slug, current, proposed, note, url, action) in enumerate(proposals, 1):
        prop = proposed if proposed else "_(no proposal — review manually)_"
        lines.append(
            f"| {i} | `{slug}` | {current} | **{prop}** | {note[:60]} | {url[:60]} |"
        )
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nWrote report to {REPORT_PATH}")
    auto = sum(1 for _, _, p, _, _, a in proposals if p and a == "fetch")
    print(f"Auto-proposable (good fetch + clean title): {auto}")
    print(f"Funder-detail (need --include-funder-pages): {sum(1 for *_, a in proposals if a == 'funder-detail')}")
    print(f"No website / fetch error: {sum(1 for *_, a in proposals if a in ('no-website',)) + sum(1 for s,c,p,n,u,a in proposals if a=='fetch' and not p)}")


if __name__ == "__main__":
    main()
