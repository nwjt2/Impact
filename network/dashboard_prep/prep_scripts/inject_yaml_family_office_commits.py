"""Inject family-office / faith-based / philanthropy-LLC LP edges from
content/family_office_lps.yml into fund_lps.csv.

Sister of inject_yaml_dfi_commitments.py. Same shape, different YAML source.

The brief's curated family-office registry holds primary-source-backed LP
commitments that scrapers haven't (and in many cases can't) capture — most
family offices and faith-based investors don't publish their LP rolls, so
the only way an INGO-GP commitment surfaces is via a fund's first-close press
release or the LP's own newsroom note.

This step bridges the registry into the network graph so any family office
with a documented INGO-GP commitment shows up as a node in /network/ even
when no fund-LP scraper has produced an edge for it. This is the explicit
quality gate the operator wanted: every YAML row carries a public_source_url,
so a family-office page entry only flows into the network when there's a
citation to back the edge.

Reads:
  content/family_office_lps.yml             (family_offices[].known_ingo_gp_commits)
  network/catalogue/investors.csv           (existing — preserved verbatim)
  network/catalogue/impact_funds.csv        (existing — read-only here)
  network/dashboard_prep/fund_lps.csv       (output of combine_fund_lps + DFI inject)

Writes:
  network/catalogue/investors.csv           (find-or-create LP rows; type from
                                             CATEGORY_TO_INVESTOR_TYPE)
  network/dashboard_prep/fund_lps.csv       (appends YAML-curated edges)

Runs AFTER combine_fund_lps and inject_yaml_dfi_commitments. Existing
(Fund Slug, LP Slug) edges are preserved; YAML rows fill gaps only.

Lesson 1: every emitted row carries `Scraping Method Used =
"yaml-curated:family_office_lps.yml#known_ingo_gp_commits"`.
Honesty discipline: every commit's `public_source_url` is required (enforced
by the build_slots.py handshake on the brief side; we re-check here).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.aliases import canonicalize_investor_slug  # noqa: E402
from network.utils.csv_io import read_rows, write_rows  # noqa: E402

YAML_PATH = REPO_ROOT / "content" / "family_office_lps.yml"
INVESTORS_CSV = REPO_ROOT / "network" / "catalogue" / "investors.csv"
IMPACT_FUNDS_CSV = REPO_ROOT / "network" / "catalogue" / "impact_funds.csv"
FUND_LPS_CSV = REPO_ROOT / "network" / "dashboard_prep" / "fund_lps.csv"

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

SCRAPING_METHOD = "yaml-curated:family_office_lps.yml#known_ingo_gp_commits"

# Mirrors sync_catalogue_from_yaml.CATEGORY_TO_INVESTOR_TYPE. Kept here as
# its own copy so this script doesn't reach across into sync's module — both
# files have the same 5-row mapping; if it diverges, fix both.
CATEGORY_TO_INVESTOR_TYPE = {
    "family_office": "family-office",
    "philanthropy_llc": "family-office",
    "faith_based": "foundation",
    "daf": "foundation",
    "hnwi_collective": "family-office",
}


def _commit_year(commit_date) -> str:
    if not commit_date:
        return ""
    return str(commit_date)[:4]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=None)
    parser.parse_args()

    if not YAML_PATH.exists():
        print(f"  No YAML at {YAML_PATH} — nothing to inject.")
        return

    with YAML_PATH.open("r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    family_offices = doc.get("family_offices") or []
    if not family_offices:
        print("  No family_offices entries in YAML — nothing to inject.")
        return

    investors = read_rows(INVESTORS_CSV)
    inv_by_slug: dict[str, dict] = {r["Investor Slug"]: r for r in investors}

    fund_slugs = {
        r["Fund Slug"] for r in read_rows(IMPACT_FUNDS_CSV) if r.get("Fund Slug")
    }

    edges = read_rows(FUND_LPS_CSV)
    edge_keys: set[tuple[str, str]] = {
        (r["Fund Slug"], canonicalize_investor_slug(r.get("LP Slug") or ""))
        for r in edges
    }

    new_investors = 0
    new_edges = 0
    skipped_no_fund = 0
    skipped_existing_edge = 0
    skipped_no_source = 0

    for fo in family_offices:
        lp_slug = canonicalize_investor_slug((fo.get("slug") or "").strip())
        lp_name = (fo.get("name") or lp_slug).strip()
        category = (fo.get("category") or "").strip()
        commits = fo.get("known_ingo_gp_commits") or []
        if not lp_slug or not commits:
            continue

        for c in commits:
            fund_slug = (c.get("peer_fund_slug") or "").strip()
            source_url = (c.get("public_source_url") or "").strip()
            if not fund_slug:
                continue

            if fund_slug not in fund_slugs:
                print(
                    f"  WARN  fund_slug {fund_slug!r} not in impact_funds.csv "
                    f"(YAML LP {lp_name!r}) — skipping"
                )
                skipped_no_fund += 1
                continue

            # Honesty discipline: don't emit an edge without a primary source.
            # build_slots.py requires public_source_url on the brief side, so
            # this should never fire in practice — defensive belt for the day
            # someone adds a commit row without one.
            if not source_url:
                print(
                    f"  WARN  YAML commit {lp_slug} -> {fund_slug} missing "
                    "public_source_url — skipping"
                )
                skipped_no_source += 1
                continue

            # Find-or-create the LP row. Belt-and-suspenders: sync_catalogue_
            # from_yaml.py already seeds these, but if this script runs against
            # an investors.csv that was built before sync was extended, we
            # still want the row to exist.
            if lp_slug not in inv_by_slug:
                new_row = {
                    "Investor Name": lp_name,
                    "Investor Slug": lp_slug,
                    "Investor Type": CATEGORY_TO_INVESTOR_TYPE.get(category, "other"),
                    "Impact Focus": "unknown",
                    "Website": fo.get("public_newsroom_url") or "",
                    "HQ Country": (fo.get("country") or "").upper(),
                    "AUM Bucket": "unknown",
                    "Status": "active",
                    "Notes": "Seeded from content/family_office_lps.yml.",
                }
                investors.append(new_row)
                inv_by_slug[lp_slug] = new_row
                new_investors += 1

            key = (fund_slug, lp_slug)
            if key in edge_keys:
                skipped_existing_edge += 1
                continue

            commit_date = c.get("commit_date")
            edges.append(
                {
                    "Fund Slug": fund_slug,
                    "LP Slug": lp_slug,
                    "Commitment Year": _commit_year(commit_date),
                    "Source URL": source_url,
                    "Source Date": str(commit_date or "")[:10],
                    "Confidence": "confirmed",
                    "Scraping Method Used": SCRAPING_METHOD,
                }
            )
            edge_keys.add(key)
            new_edges += 1

    write_rows(INVESTORS_CSV, INVESTORS_HEADERS, investors)
    write_rows(FUND_LPS_CSV, FUND_LPS_HEADERS, edges)

    total_commits = sum(
        len(fo.get("known_ingo_gp_commits") or []) for fo in family_offices
    )
    print(f"  YAML family-office commits processed: {total_commits}")
    print(f"    new investor rows:     {new_investors}")
    print(f"    new LP edges:          {new_edges}")
    print(f"    edges already present: {skipped_existing_edge}")
    print(f"    skipped (no fund):     {skipped_no_fund}")
    print(f"    skipped (no source):   {skipped_no_source}")


if __name__ == "__main__":
    main()
