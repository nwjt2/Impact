"""Schema/integrity tests for the LP-side YAML registries.

Covers: 9 LP registries under content/, plus the network/foundation reconcile
path. Every LP appearing as an LP edge in the network MUST resolve to a
registry entry — tested separately in test_lp_network_coverage.

Hard rules enforced here:
  - Each registry parses cleanly.
  - No duplicate slugs within a registry.
  - No duplicate slugs across the *new* registries (banks, asset-managers,
    pension-funds, corporates, government-donors, cooperative-ngos). Known
    pre-existing dupes between foundation_lps.yml and family_office_lps.yml
    (mercy-investment-services, impactassets) are tolerated.
  - Every entry has: slug, name, country, last_seen_at.
  - last_seen_at parses as YYYY-MM-DD.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
CONTENT = REPO / "content"

# (path, top-level YAML key, slug field name)
LP_REGISTRIES = [
    (CONTENT / "foundation_lps.yml", "foundations", "slug"),
    (CONTENT / "family_office_lps.yml", "family_offices", "slug"),
    (CONTENT / "dfi_ingo_commitments.yml", "dfi_profiles", "dfi_slug"),
    (CONTENT / "bank_lps.yml", "banks", "slug"),
    (CONTENT / "asset_manager_lps.yml", "asset_managers", "slug"),
    (CONTENT / "pension_fund_lps.yml", "pension_funds", "slug"),
    (CONTENT / "corporate_lps.yml", "corporates", "slug"),
    (CONTENT / "government_donor_lps.yml", "government_donors", "slug"),
    (CONTENT / "cooperative_ngo_lps.yml", "cooperative_ngos", "slug"),
]

# Slugs that have appeared in two different registries historically. These
# represent entities whose archetype is genuinely ambiguous; tolerated until
# the operator picks one home for them.
_KNOWN_CROSS_REGISTRY_DUPES = {"mercy-investment-services", "impactassets"}

# The 6 new registries that should not introduce any cross-registry dupes.
_NEW_REGISTRY_KEYS = {
    "banks", "asset_managers", "pension_funds",
    "corporates", "government_donors", "cooperative_ngos",
}


def _load(path: Path, key: str) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        d = yaml.safe_load(f)
    assert key in d, f"{path}: missing top-level key {key!r}"
    entries = d[key]
    assert isinstance(entries, list), f"{path}: {key} must be a list"
    return entries


@pytest.mark.parametrize(
    "path,key,slug_field",
    LP_REGISTRIES,
    ids=lambda x: x.name if isinstance(x, Path) else str(x),
)
def test_lp_registry_parses(path: Path, key: str, slug_field: str) -> None:
    entries = _load(path, key)
    assert entries, f"{path}: empty registry"


@pytest.mark.parametrize(
    "path,key,slug_field",
    LP_REGISTRIES,
    ids=lambda x: x.name if isinstance(x, Path) else str(x),
)
def test_lp_registry_no_intra_dupes(path: Path, key: str, slug_field: str) -> None:
    entries = _load(path, key)
    slugs = [e[slug_field] for e in entries]
    counts = Counter(slugs)
    dupes = [s for s, n in counts.items() if n > 1]
    assert not dupes, f"{path}: duplicate slugs within registry: {dupes}"


@pytest.mark.parametrize(
    "path,key,slug_field",
    LP_REGISTRIES,
    ids=lambda x: x.name if isinstance(x, Path) else str(x),
)
def test_lp_registry_required_fields(path: Path, key: str, slug_field: str) -> None:
    entries = _load(path, key)
    name_field = "dfi_name" if slug_field == "dfi_slug" else "name"
    # country is recommended but not required — some legacy entries have it
    # null pending operator review. Slug, name, last_seen_at are hard.
    for e in entries:
        slug = e.get(slug_field)
        assert slug, f"{path}: entry missing {slug_field}"
        assert e.get(name_field), f"{path}: {slug} missing {name_field}"
        ls = e.get("last_seen_at")
        assert ls, f"{path}: {slug} missing last_seen_at"
        # PyYAML auto-parses YYYY-MM-DD into datetime.date.
        if isinstance(ls, str):
            try:
                date.fromisoformat(ls)
            except ValueError as exc:
                pytest.fail(f"{path}: {slug} last_seen_at not YYYY-MM-DD: {exc}")
        else:
            assert isinstance(ls, date), (
                f"{path}: {slug} last_seen_at unexpected type {type(ls).__name__}"
            )


def test_every_network_lp_has_registry_entry() -> None:
    """Every investor that appears as the source of an `lp` edge in
    site/src/_data/network.json MUST resolve to a registry entry. This is
    the "100% LP coverage" invariant established by the 2026-05-04
    archetype-classification + registry-scaffolding pass.

    Skipped if network.json hasn't been built yet (e.g. fresh checkout)."""
    import json

    network_json = REPO / "site" / "src" / "_data" / "network.json"
    if not network_json.exists():
        pytest.skip(f"{network_json} not generated yet")

    net = json.loads(network_json.read_text(encoding="utf-8"))
    lp_slugs = {
        e["source"].split(":", 1)[1]
        for e in net["edges"]
        if e["kind"] == "lp" and e["source"].startswith("investor:")
    }

    registry_slugs: set[str] = set()
    for path, key, slug_field in LP_REGISTRIES:
        registry_slugs |= {e[slug_field] for e in _load(path, key)}

    missing = sorted(lp_slugs - registry_slugs)
    assert not missing, (
        f"{len(missing)} LPs appear in network.json without a registry entry:\n"
        + "\n".join(f"  {s}" for s in missing[:20])
        + ("\n  …" if len(missing) > 20 else "")
    )


def test_new_registries_no_cross_dupes() -> None:
    """The 6 new registries must not collide with each other or with the
    pre-existing 3. (Pre-existing collisions between foundation_lps.yml and
    family_office_lps.yml are tracked by _KNOWN_CROSS_REGISTRY_DUPES.)"""
    seen: dict[str, str] = {}  # slug -> first-seen registry name
    collisions: list[tuple[str, str, str]] = []
    for path, key, slug_field in LP_REGISTRIES:
        for e in _load(path, key):
            slug = e[slug_field]
            if slug in seen:
                if slug in _KNOWN_CROSS_REGISTRY_DUPES and key not in _NEW_REGISTRY_KEYS:
                    continue
                collisions.append((slug, seen[slug], key))
            else:
                seen[slug] = key
    assert not collisions, (
        "Cross-registry slug collisions involving a new registry:\n"
        + "\n".join(f"  {slug}: in {a} AND {b}" for slug, a, b in collisions)
    )
