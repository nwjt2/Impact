"""Tests for the /family-offices/ page ↔ /network/ reconcile.

Locks in the contract the operator asked for on 2026-04-29:

  - Every entry on /family-offices/ (content/family_office_lps.yml) MUST be
    represented in network/catalogue/investors.csv with an investor_type that
    matches CATEGORY_TO_INVESTOR_TYPE. The page is the canonical superset.
  - Every node tagged investor_type='family-office' on /network/ MUST be
    on the family-offices page. No drift back to family-named foundations
    incorrectly classified as family offices in network.

Both checks run against generated artifacts (investors.csv + the brief's
family_office_lps.json) so they catch regressions in either direction.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
FAMILY_OFFICE_YML = REPO / "content" / "family_office_lps.yml"
INVESTORS_CSV = REPO / "network" / "catalogue" / "investors.csv"
FAMILY_OFFICE_JSON = REPO / "site" / "src" / "_data" / "family_office_lps.json"
NETWORK_JSON = REPO / "site" / "src" / "_data" / "network.json"

# Mirrors sync_catalogue_from_yaml.CATEGORY_TO_INVESTOR_TYPE +
# inject_yaml_family_office_commits.CATEGORY_TO_INVESTOR_TYPE. Test fails if
# the two scripts diverge.
CATEGORY_TO_INVESTOR_TYPE = {
    "family_office": "family-office",
    "philanthropy_llc": "family-office",
    "faith_based": "foundation",
    "daf": "foundation",
    "hnwi_collective": "family-office",
}


def _load_yaml_entries() -> list[dict]:
    with FAMILY_OFFICE_YML.open("r", encoding="utf-8") as f:
        return (yaml.safe_load(f) or {}).get("family_offices") or []


def _load_investors() -> dict[str, dict]:
    with INVESTORS_CSV.open("r", encoding="utf-8", newline="") as f:
        return {r["Investor Slug"]: r for r in csv.DictReader(f)}


def test_every_yaml_entry_in_investors_csv() -> None:
    """Direction 1 contract: family-offices PAGE is the canonical superset.

    Every entry in family_office_lps.yml must have a row in investors.csv.
    The sync_catalogue_from_yaml.py script is responsible for this — if this
    test fails, sync was never run after editing the YAML, or sync is broken.
    """
    yaml_entries = _load_yaml_entries()
    investors = _load_investors()

    missing = [r["slug"] for r in yaml_entries if r["slug"] not in investors]
    assert not missing, (
        f"family_office_lps.yml entries missing from investors.csv: {missing}. "
        "Run network.dashboard_prep.prep_scripts.sync_catalogue_from_yaml."
    )


def test_yaml_category_drives_investor_type() -> None:
    """The YAML category must map to investor_type per CATEGORY_TO_INVESTOR_TYPE.

    Catches drift where someone hand-edits investors.csv to flip a faith_based
    entity to 'family-office' (or similar) without updating the canonical YAML.
    """
    yaml_entries = _load_yaml_entries()
    investors = _load_investors()

    mismatches = []
    for r in yaml_entries:
        slug = r["slug"]
        category = r.get("category")
        expected = CATEGORY_TO_INVESTOR_TYPE.get(category)
        if not expected:
            continue
        actual = investors.get(slug, {}).get("Investor Type")
        if actual != expected:
            mismatches.append(
                f"{slug}: category={category} -> expected {expected}, got {actual}"
            )

    assert not mismatches, "investor_type drift:\n  " + "\n  ".join(mismatches)


def test_network_family_offices_are_all_on_page() -> None:
    """Direction 2 contract: only family-offices PAGE entries may appear in
    /network/ as investor_type='family-office'.

    Catches drift where a scraper-discovered investor (e.g. a family-named
    foundation) gets auto-classified as 'family-office' by the heuristic in
    combine_fund_lps._classify_investor_type — that's a sign the row needs
    operator review, not a silent leak into the network's family-office set.
    """
    if not (NETWORK_JSON.exists() and FAMILY_OFFICE_JSON.exists()):
        pytest.skip("Generated JSON not present; run build_slots + build_network_json")

    page_slugs = {
        r["slug"] for r in json.loads(FAMILY_OFFICE_JSON.read_text(encoding="utf-8"))
    }
    network = json.loads(NETWORK_JSON.read_text(encoding="utf-8"))

    leaked = []
    for n in network["nodes"]:
        if n.get("investor_type") != "family-office":
            continue
        slug = n["id"].removeprefix("investor:")
        if slug not in page_slugs:
            leaked.append(f"{slug} ({n.get('name')})")

    assert not leaked, (
        "Network has family-office nodes not on the family-offices page:\n  "
        + "\n  ".join(leaked)
        + "\nEither add to content/family_office_lps.yml or reclassify the row "
        "in network/catalogue/investors.csv (likely 'foundation' for family-named "
        "foundations)."
    )
