"""Resolve US foundations in content/foundation_lps.yml to ProPublica EINs.

Per network/CLAUDE.md: free APIs first, polite UA, surface anomalies as warnings,
honest-null discipline. ProPublica's name search is fuzzy and ambiguous (e.g.
'Ford Foundation' returns 'Foundation For Rocky Ford Schools' as the top hit
ahead of the real Ford Foundation), so we auto-confirm only on high-confidence
matches and flag everything else for operator review.

Writes:  network/lp_990_signals/ein_registry.csv
         (operator-editable; resolver preserves rows already marked confirmed)

Re-running is idempotent:
  - Rows with Confidence = 'confirmed' are kept verbatim (operator's word stands).
  - Rows with Confidence = 'needs_review' or missing are re-resolved.
  - Foundations that have left the YAML linger as orphans with a warning.

This is NOT a scraper in the lesson-11 sense (no per-fund Python). It's a
metadata lookup against a free public API.
"""
from __future__ import annotations

import argparse
import csv
import difflib
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

import yaml  # noqa: E402

from network.utils.csv_io import read_rows, write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402

FOUNDATIONS_YAML = REPO_ROOT / "content" / "foundation_lps.yml"
EIN_REGISTRY = REPO_ROOT / "network" / "lp_990_signals" / "ein_registry.csv"
PROPUBLICA_BASE = "https://projects.propublica.org/nonprofits/api/v2"

# NTEE prefix-match — code starts with one of these means it's a foundation.
# T20–T29 covers private foundations (T20Z = "Other private grantmaking foundations" etc).
# T30–T31 covers public/community foundations.
FOUNDATION_NTEE_PREFIXES = ("T20", "T21", "T22", "T23", "T24", "T25", "T26", "T27", "T28", "T29", "T30", "T31")


def is_foundation_ntee(code: str | None) -> bool:
    if not code:
        return False
    return any(code.startswith(p) for p in FOUNDATION_NTEE_PREFIXES)

REGISTRY_HEADERS = [
    "LP Slug",
    "LP Name",
    "EIN",
    "Resolved Name",
    "NTEE",
    "State",
    "Confidence",
    "Top Candidates",
    "Resolved Date",
    "Notes",
]


def normalise_name(s: str) -> str:
    """Lowercase, strip 'the ', strip trailing 'inc'/'inc.'/' inc', collapse spaces, drop punct."""
    s = s.lower().strip()
    s = re.sub(r"^the\s+", "", s)
    s = re.sub(r"[\.,&]", " ", s)
    s = re.sub(r"\s+inc\b\.?", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def name_similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, normalise_name(a), normalise_name(b)).ratio()


def search_propublica(query: str, *, sleep: float = 0.5) -> list[dict]:
    """Returns [] on 404 (ProPublica rejects some queries) — caller can retry with another variant."""
    url = f"{PROPUBLICA_BASE}/search.json?q={urllib.parse.quote(query)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            time.sleep(sleep)
            return []
        raise
    time.sleep(sleep)
    return body.get("organizations", [])


def best_similarity(hit_name: str, variants: list[str]) -> float:
    return max((name_similarity(v, hit_name) for v in variants if v), default=0.0)


def score_hit(variants: list[str], hit: dict) -> tuple[float, str]:
    """Returns (best name-similarity, short reason)."""
    h_name = hit.get("name") or ""
    sim = best_similarity(h_name, variants)
    ntee = hit.get("ntee_code") or ""
    fdn_tag = "fdn" if is_foundation_ntee(ntee) else (f"ntee={ntee}" if ntee else "no-ntee")
    return sim, fdn_tag


def candidates_field(hits: list[dict]) -> str:
    parts = []
    for h in hits[:5]:
        parts.append(
            f"{h.get('ein','')}|{(h.get('name') or '').replace('|','')}|{h.get('ntee_code') or ''}|{h.get('state') or ''}"
        )
    return ";;".join(parts)


def load_us_foundations() -> list[dict]:
    with FOUNDATIONS_YAML.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    fdns = data.get("foundations", [])
    return [f for f in fdns if (f.get("country") or "").upper() == "US"]


def load_existing_registry() -> dict[str, dict]:
    rows = read_rows(EIN_REGISTRY)
    return {r["LP Slug"]: r for r in rows}


def name_variants(yaml_name: str, aliases: list[str] | None) -> list[str]:
    seen = []
    for v in [yaml_name] + (aliases or []):
        if v and v not in seen:
            seen.append(v)
    return seen


def resolve_one(yaml_name: str, aliases: list[str] | None) -> dict:
    """Try canonical name first, then each alias if no hits. Score against all variants."""
    variants = name_variants(yaml_name, aliases)
    hits: list[dict] = []
    queries_tried: list[str] = []
    for q in variants:
        queries_tried.append(q)
        hits = search_propublica(q)
        if hits:
            break  # first non-empty hit set wins; aliases just unblock 404s and zero-result queries

    if not hits:
        return {
            "EIN": "",
            "Resolved Name": "",
            "NTEE": "",
            "State": "",
            "Confidence": "unresolved",
            "Top Candidates": "",
            "Notes": f"no search hits (tried: {' | '.join(queries_tried)})",
        }

    # Score every hit against all name variants. ProPublica's own ranking
    # generally puts the most prominent match at top (e.g. real Rockefeller-NY
    # ahead of similarly-named LA org, even when NY's NTEE is missing). We
    # respect that ordering: take the TOP hit by ProPublica's order, then
    # verify it's not ambiguous.
    top = hits[0]
    top_sim = best_similarity(top.get("name") or "", variants)

    # Ambiguity guardrail: count how many hits are plausibly the same entity.
    high_sim_hits = [h for h in hits if best_similarity(h.get("name") or "", variants) >= 0.85]
    ambiguous = len(high_sim_hits) >= 2

    top_ntee = top.get("ntee_code") or ""
    top_is_fdn = is_foundation_ntee(top_ntee)

    if ambiguous:
        confidence = "needs_review"
        reason = f"ambiguous: {len(high_sim_hits)} similar-name hits; top sim={top_sim:.2f} ntee={top_ntee or 'none'}"
    elif top_sim >= 0.95 and top_is_fdn:
        confidence = "confirmed"
        reason = f"exact-name + foundation-ntee ({top_ntee})"
    elif top_sim >= 0.90 and top_is_fdn:
        confidence = "confirmed"
        reason = f"close-name + foundation-ntee ({top_ntee})"
    elif top_sim >= 0.95:
        # exact name match but no foundation NTEE — common for real entities
        # whose NTEE is missing in ProPublica's index. Confirm only because
        # there's no ambiguity (already checked).
        confidence = "confirmed"
        reason = f"exact-name (ntee={top_ntee or 'missing'}); only similar-name hit"
    else:
        confidence = "needs_review"
        reason = f"top sim={top_sim:.2f} ntee={top_ntee or 'none'}"

    return {
        "EIN": top.get("ein") or "",
        "Resolved Name": top.get("name") or "",
        "NTEE": top.get("ntee_code") or "",
        "State": top.get("state") or "",
        "Confidence": confidence,
        "Top Candidates": candidates_field(hits),
        "Notes": reason,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="cap to first N (0 = all)")
    parser.add_argument("--reresolve-confirmed", action="store_true",
                        help="overwrite operator-confirmed rows too (rare; for full refresh)")
    args = parser.parse_args()

    fdns = load_us_foundations()
    if args.limit:
        fdns = fdns[: args.limit]
    print(f"resolving {len(fdns)} US foundations…")

    existing = load_existing_registry()
    today = time.strftime("%Y-%m-%d")
    out: list[dict] = []
    auto = 0
    review = 0
    unresolved = 0
    kept = 0

    for fdn in fdns:
        slug = fdn["slug"]
        name = fdn["name"]
        prev = existing.get(slug)
        if prev and prev.get("Confidence") == "confirmed" and not args.reresolve_confirmed:
            kept += 1
            out.append(prev)
            continue
        try:
            res = resolve_one(name, fdn.get("aliases") or [])
        except Exception as exc:
            print(f"  ERROR on {slug} ({name}): {exc!r}")
            res = {
                "EIN": "",
                "Resolved Name": "",
                "NTEE": "",
                "State": "",
                "Confidence": "unresolved",
                "Top Candidates": "",
                "Notes": f"error: {exc}",
            }
        row = {
            "LP Slug": slug,
            "LP Name": name,
            "EIN": res["EIN"],
            "Resolved Name": res["Resolved Name"],
            "NTEE": res["NTEE"],
            "State": res["State"],
            "Confidence": res["Confidence"],
            "Top Candidates": res["Top Candidates"],
            "Resolved Date": today,
            "Notes": res["Notes"],
        }
        out.append(row)
        if res["Confidence"] == "confirmed":
            auto += 1
        elif res["Confidence"] == "needs_review":
            review += 1
        else:
            unresolved += 1
        ein_str = str(res["EIN"]) if res["EIN"] else "???"
        print(f"  [{res['Confidence']:14s}] {slug:40s} -> {ein_str:>10s}  {res['Notes']}")

    # Surface orphans (rows in registry but no longer in YAML).
    yaml_slugs = {f["slug"] for f in fdns}
    if not args.limit:  # only when full pass
        for slug, prev in existing.items():
            if slug not in yaml_slugs:
                print(f"  WARNING orphan: {slug} no longer in foundation_lps.yml; preserving row")
                out.append(prev)

    write_rows(EIN_REGISTRY, REGISTRY_HEADERS, out)
    print()
    print(f"summary: confirmed-now={auto}, needs_review={review}, unresolved={unresolved}, kept-confirmed-prior={kept}")
    print(f"wrote {EIN_REGISTRY.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
