"""Inject DFI -> fund LP edges from content/dfi_ingo_commitments.yml.

The brief's editorially-curated DFI registry (`content/dfi_ingo_commitments.yml`)
holds primary-source-backed LP commitments that scrapers haven't (and in many
cases can't) capture — Cloudflare-walled fund pages, press-release-only
disclosures, IFC project-disclosure DB entries, etc.

This step bridges the registry into the network graph so a DFI with a
documented INGO-GP commitment shows up as a node in /network/ even when no
fund-LP scraper has produced an edge for it.

Reads:
  content/dfi_ingo_commitments.yml          (commitments + dfi_profiles)
  network/catalogue/investors.csv           (existing — preserved verbatim)
  network/catalogue/impact_funds.csv        (existing — read-only here)
  network/dashboard_prep/fund_lps.csv       (output of combine_fund_lps)

Writes:
  network/catalogue/investors.csv           (find-or-create DFI rows; type=dfi)
  network/dashboard_prep/fund_lps.csv       (appends YAML-curated edges)

Runs AFTER combine_fund_lps (which rebuilds fund_lps.csv from scrapers).
Existing scraped (Fund Slug, LP Slug) edges are preserved; YAML rows fill gaps.

Lesson 1: every emitted row carries `Scraping Method Used =
"yaml-curated:dfi_ingo_commitments.yml#commitments"`.
Honesty discipline: every YAML commitment carries `public_source_url`.
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
from network.utils.slugify import slugify  # noqa: E402

YAML_PATH = REPO_ROOT / "content" / "dfi_ingo_commitments.yml"
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

SCRAPING_METHOD = "yaml-curated:dfi_ingo_commitments.yml#commitments"

# A handful of YAML dfi_slugs collide with sibling entities already in
# investors.csv (e.g. YAML's "ifc" maps to International Finance Corporation,
# but investors.csv has IFC Asset Management / AIP under ifc-asset-management-aip).
# The canonical lookup goes by `dfi_name` first; this map only kicks in if the
# name doesn't match anything in investors.csv. Empty by default — extend if a
# real ambiguity surfaces.
EXPLICIT_NAME_TO_SLUG: dict[str, str] = {}


def _commit_year(commit_date: str | None) -> str:
    if not commit_date:
        return ""
    return str(commit_date)[:4]


def main() -> None:
    # Accept (and ignore) --run to stay compatible with refresh_all.py's
    # uniform invocation contract.
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, default=None)
    parser.parse_args()

    if not YAML_PATH.exists():
        print(f"  No YAML at {YAML_PATH} — nothing to inject.")
        return

    with YAML_PATH.open("r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    commitments = doc.get("commitments") or []
    if not commitments:
        print("  No commitments in YAML — nothing to inject.")
        return

    investors = read_rows(INVESTORS_CSV)
    inv_by_slug: dict[str, dict] = {r["Investor Slug"]: r for r in investors}
    inv_name_to_slug: dict[str, str] = {
        (r["Investor Name"] or "").strip().lower(): r["Investor Slug"]
        for r in investors
        if r.get("Investor Name")
    }

    fund_slugs = {r["Fund Slug"] for r in read_rows(IMPACT_FUNDS_CSV) if r.get("Fund Slug")}

    edges = read_rows(FUND_LPS_CSV)
    edge_keys: set[tuple[str, str]] = {
        (r["Fund Slug"], canonicalize_investor_slug(r.get("LP Slug") or ""))
        for r in edges
    }

    new_investors = 0
    new_edges = 0
    skipped_no_fund = 0
    skipped_existing_edge = 0

    for c in commitments:
        dfi_name = (c.get("dfi_name") or "").strip()
        fund_slug = (c.get("fund_slug") or "").strip()
        if not dfi_name or not fund_slug:
            continue

        if fund_slug not in fund_slugs:
            print(
                f"  WARN  fund_slug {fund_slug!r} not in impact_funds.csv "
                f"(YAML LP {dfi_name!r}) — skipping"
            )
            skipped_no_fund += 1
            continue

        # Resolve LP slug. Prefer existing investor by name; else slugify the
        # name; else fall through to canonicalize alias map.
        lp_slug = inv_name_to_slug.get(dfi_name.lower())
        if not lp_slug:
            lp_slug = EXPLICIT_NAME_TO_SLUG.get(dfi_name) or slugify(dfi_name)
        lp_slug = canonicalize_investor_slug(lp_slug)

        if lp_slug not in inv_by_slug:
            new_row = {
                "Investor Name": dfi_name,
                "Investor Slug": lp_slug,
                "Investor Type": "dfi",
                "Impact Focus": "unknown",
                "Website": "",
                "HQ Country": (c.get("dfi_country") or "").upper(),
                "AUM Bucket": "unknown",
                "Status": "active",
                "Notes": "Seeded from content/dfi_ingo_commitments.yml.",
            }
            investors.append(new_row)
            inv_by_slug[lp_slug] = new_row
            inv_name_to_slug[dfi_name.lower()] = lp_slug
            new_investors += 1

        key = (fund_slug, lp_slug)
        if key in edge_keys:
            skipped_existing_edge += 1
            continue

        edges.append(
            {
                "Fund Slug": fund_slug,
                "LP Slug": lp_slug,
                "Commitment Year": _commit_year(c.get("commit_date")),
                "Source URL": c.get("public_source_url") or "",
                "Source Date": str(c.get("commit_date") or "")[:10],
                "Confidence": "confirmed",
                "Scraping Method Used": SCRAPING_METHOD,
            }
        )
        edge_keys.add(key)
        new_edges += 1

    write_rows(INVESTORS_CSV, INVESTORS_HEADERS, investors)
    write_rows(FUND_LPS_CSV, FUND_LPS_HEADERS, edges)

    print(f"  YAML commitments processed: {len(commitments)}")
    print(f"    new investor rows:    {new_investors}")
    print(f"    new LP edges:         {new_edges}")
    print(f"    edges already present: {skipped_existing_edge}")
    print(f"    skipped (no fund):    {skipped_no_fund}")


if __name__ == "__main__":
    main()
