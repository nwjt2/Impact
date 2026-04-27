"""Tests for pipeline.build_slots — the module that materialises the
three public slot JSONs from the curated YAML registries.

Strategy:
  1. Unit-test each loader against small in-memory fixtures written to
     tmp_path (so the test doesn't depend on the live content/*.yml).
  2. Integration-test that the real content/ files load cleanly, all
     handshakes hold, and the emitted JSON shapes validate against the
     Pydantic models.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from pipeline import build_slots
from pipeline.schemas import (
    Deadline,
    DfiIngoCommit,
    FamilyOfficeLp,
    FoundationLp,
    PeerIngoFund,
)

REPO = Path(__file__).resolve().parents[1]
DATA_DIR = REPO / "site" / "src" / "_data"


# --------------------------------------------------------------- fixtures


def _write_fixture_peer_funds(tmp_path: Path, extra: list | None = None) -> Path:
    rows = [
        {
            "slug": "acme-impact-fund-i",
            "name": "Acme Impact Fund I",
            "manager": "Acme GP",
            "parent_ingo": "Acme Charity",
            "parent_ingo_country": "GB",
            "vintage": 2023,
            "size_usd_m": 50,
            "sector_tags": ["climate"],
            "geo_tags": ["africa"],
            "vehicle_type": "closed_end_fund",
            "public_source_url": "https://example.com/acme",
            "status": "raising",
            "last_seen_at": "2026-04-24",
        },
        {
            "slug": "microbuild-fund",
            "name": "MicroBuild Fund",
            "manager": "MicroBuild GP",
            "parent_ingo": "Habitat for Humanity",
            "parent_ingo_country": "US",
            "vintage": 2012,
            "size_usd_m": 100,
            "sector_tags": ["housing", "fi"],
            "geo_tags": ["latam", "asia"],
            "vehicle_type": "closed_end_fund",
            "public_source_url": "https://example.com/microbuild",
            "status": "deployed",
            "last_seen_at": "2026-04-24",
        },
    ]
    if extra:
        rows.extend(extra)
    p = tmp_path / "peer_funds.yml"
    p.write_text(yaml.safe_dump({"peer_funds": rows}))
    return p


def _write_fixture_dfi_commits(
    tmp_path: Path,
    commit_fund_slug: str = "microbuild-fund",
    with_bad_slug: bool = False,
) -> Path:
    commits = [
        {
            "dfi_slug": "bii",
            "dfi_name": "British International Investment",
            "dfi_country": "GB",
            "fund_slug": commit_fund_slug,
            "fund_name": "MicroBuild Fund",
            "parent_ingo": "Habitat for Humanity",
            "parent_ingo_country": "US",
            "commit_date": "2015-03-01",
            "commit_usd_m": 10,
            "sector_tags": ["housing", "fi"],
            "geo_tags": ["latam"],
            "public_source_url": "https://bii.example.com/microbuild",
        },
        {
            "dfi_slug": "bii",
            "dfi_name": "British International Investment",
            "dfi_country": "GB",
            "fund_slug": "acme-impact-fund-i",
            "fund_name": "Acme Impact Fund I",
            "parent_ingo": "Acme Charity",
            "parent_ingo_country": "GB",
            "commit_date": "2024-01-15",
            "commit_usd_m": 20,
            "public_source_url": "https://bii.example.com/acme",
        },
    ]
    if with_bad_slug:
        commits.append({
            "dfi_slug": "bii",
            "dfi_name": "British International Investment",
            "dfi_country": "GB",
            "fund_slug": "fund-that-does-not-exist",
            "fund_name": "Ghost Fund",
            "parent_ingo": "Ghost Inc",
            "commit_date": "2025-01-01",
            "commit_usd_m": 5,
            "public_source_url": "https://example.com/ghost",
        })

    p = tmp_path / "dfi_ingo_commitments.yml"
    p.write_text(yaml.safe_dump({
        "_dfi_slug_mapping": {"bii": "British International Investment"},
        "commitments": commits,
        "dfi_profiles": [{
            "dfi_slug": "bii",
            "dfi_name": "British International Investment",
            "stated_sector_preferences": ["climate", "fi"],
            "stated_geo_focus": ["africa", "south-asia"],
            "stated_thesis_url": "https://bii.example.com/strategy",
            "known_contacts_public": [],
            "typical_ticket_usd_m_min": 5,
            "typical_ticket_usd_m_max": 50,
            "emerging_manager_facility": {
                "exists": True,
                "program_name": "Kinetic",
                "application_url": "https://bii.example.com/kinetic",
                "notes": "first-time fund manager platform",
            },
        }],
    }))
    return p


def _write_fixture_deadlines(tmp_path: Path) -> Path:
    rows = [
        {
            "deadline_id": "bii-kinetic-rolling",
            "issuing_body": "BII",
            "country": "GB",
            "kind": "rolling_application",
            "title": "BII Kinetic — Emerging Manager Platform (rolling)",
            "deadline_date": None,
            "recurring": "rolling",
            "why_it_matters": "INGO-aligned DFI entry point.",
            "public_source_url": "https://bii.example.com/kinetic",
            "last_verified_at": "2026-04-24",
        },
        {
            "deadline_id": "past-one-off",
            "issuing_body": "Old Body",
            "country": "US",
            "kind": "rfp",
            "title": "Old RFP",
            "deadline_date": "2020-01-01",
            "recurring": None,
            "why_it_matters": "historical",
            "public_source_url": "https://example.com/old",
            "last_verified_at": "2024-01-01",
        },
        {
            "deadline_id": "future-one-off",
            "issuing_body": "New Body",
            "country": "US",
            "kind": "rfp",
            "title": "Future RFP",
            "deadline_date": "2099-01-01",
            "recurring": None,
            "why_it_matters": "future",
            "public_source_url": "https://example.com/new",
            "last_verified_at": "2026-04-24",
        },
    ]
    p = tmp_path / "deadlines.yml"
    p.write_text(yaml.safe_dump({"deadlines": rows}))
    return p


def _write_fixture_foundation_lps(tmp_path: Path, with_bad_slug: bool = False) -> Path:
    rows = [
        {
            "slug": "test-foundation",
            "name": "Test Foundation",
            "aliases": ["Test Foundation"],
            "country": "US",
            "foundation_type": "private",
            "stated_priority_themes": ["climate", "health"],
            "stated_geo_focus": ["global"],
            "typical_check_usd_m_min": 3,
            "typical_check_usd_m_max": 15,
            "public_newsroom_url": "https://example.com/news",
            "last_seen_at": "2026-04-27",
            "known_ingo_gp_commits": [
                {
                    "peer_fund_slug": "microbuild-fund",
                    "peer_fund_name": "MicroBuild Fund",
                    "parent_ingo": "Habitat for Humanity",
                    "commit_date": "2018-06-01",
                    "amount_usd_m": 5,
                    "public_source_url": "https://example.com/commit",
                },
            ],
        },
        {
            "slug": "test-corp-foundation",
            "name": "Test Corp Foundation",
            "country": "GB",
            "foundation_type": "corporate",
            "stated_priority_themes": ["smb"],
            "stated_geo_focus": ["africa"],
            "public_newsroom_url": "https://example.com/corpnews",
        },
    ]
    if with_bad_slug:
        rows[0]["known_ingo_gp_commits"][0]["peer_fund_slug"] = "ghost-fund-slug"
    p = tmp_path / "foundation_lps.yml"
    p.write_text(yaml.safe_dump({"foundations": rows}))
    return p


def _write_fixture_family_office_lps(tmp_path: Path) -> Path:
    rows = [
        {
            "slug": "test-famof",
            "name": "Test Family Office",
            "country": "US",
            "category": "family_office",
            "stated_priority_themes": ["fi", "smb"],
            "stated_geo_focus": ["africa"],
            "typical_check_usd_m_min": 1,
            "typical_check_usd_m_max": 5,
            "invests_via_fund_lp": True,
            "public_newsroom_url": "https://example.com/famof",
        },
        {
            "slug": "test-faith-based",
            "name": "Test Faith-Based Investor",
            "country": "US",
            "category": "faith_based",
            "stated_priority_themes": ["housing"],
            "stated_geo_focus": ["us"],
        },
    ]
    p = tmp_path / "family_office_lps.yml"
    p.write_text(yaml.safe_dump({"family_offices": rows}))
    return p


@pytest.fixture
def patch_paths(tmp_path, monkeypatch):
    """Rebind build_slots's module-level file paths to a tmp-scoped set."""
    monkeypatch.setattr(build_slots, "PEER_FUNDS_YML", tmp_path / "peer_funds.yml")
    monkeypatch.setattr(build_slots, "DFI_COMMITS_YML", tmp_path / "dfi_ingo_commitments.yml")
    monkeypatch.setattr(build_slots, "DEADLINES_YML", tmp_path / "deadlines.yml")
    monkeypatch.setattr(build_slots, "FOUNDATION_LPS_YML", tmp_path / "foundation_lps.yml")
    monkeypatch.setattr(build_slots, "FAMILY_OFFICE_LPS_YML", tmp_path / "family_office_lps.yml")
    # redirect JSON emits to tmp so we don't clobber the real site/src/_data
    data_dir = tmp_path / "_data"
    data_dir.mkdir()
    from pipeline import emit
    monkeypatch.setattr(emit, "DATA_DIR", data_dir)
    return tmp_path


# --------------------------------------------------------------- unit tests


def test_load_peer_funds_ok(patch_paths):
    _write_fixture_peer_funds(patch_paths)
    models, slugs = build_slots.load_peer_funds()
    assert len(models) == 2
    assert all(isinstance(m, PeerIngoFund) for m in models)
    assert "microbuild-fund" in slugs
    # country normalised uppercase
    assert models[0].parent_ingo_country == "GB"


def test_peer_funds_rejects_duplicate_slug(patch_paths):
    _write_fixture_peer_funds(patch_paths, extra=[{
        "slug": "microbuild-fund",  # dup
        "name": "Another Microbuild",
    }])
    with pytest.raises(build_slots.HandshakeError) as exc:
        build_slots.load_peer_funds()
    assert "duplicate slug" in str(exc.value)


def test_peer_funds_rejects_missing_slug(patch_paths):
    _write_fixture_peer_funds(patch_paths, extra=[{
        "name": "Nameless",  # no slug
    }])
    with pytest.raises(build_slots.HandshakeError) as exc:
        build_slots.load_peer_funds()
    assert "slug" in str(exc.value).lower()


def test_load_dfi_commits_handshake_ok(patch_paths):
    _write_fixture_peer_funds(patch_paths)
    _write_fixture_dfi_commits(patch_paths)
    models, _ = build_slots.load_peer_funds()
    peer_slugs = {m.slug for m in models}
    cards, raw = build_slots.load_dfi_commitments(peer_slugs)
    assert len(cards) == 1
    card = cards[0]
    assert isinstance(card, DfiIngoCommit)
    assert card.slug == "bii"
    # Both commits have INGO parent → both enter ingo_gp_commits
    assert len(card.ingo_gp_commits) == 2
    # Ticket range: n=2 qualifies (>=2)
    assert card.typical_ticket_usd_m_range is not None
    assert card.typical_ticket_usd_m_range.n == 2
    assert card.typical_ticket_usd_m_range.min == 10
    assert card.typical_ticket_usd_m_range.max == 20
    # last_known_activity_date: most recent commit
    assert card.last_known_activity_date.isoformat() == "2024-01-15"
    # EMF from profile
    assert card.emerging_manager_facility is not None
    assert card.emerging_manager_facility.program_name == "Kinetic"


def test_dfi_commits_handshake_fails_on_bad_fund_slug(patch_paths):
    _write_fixture_peer_funds(patch_paths)
    _write_fixture_dfi_commits(patch_paths, with_bad_slug=True)
    models, _ = build_slots.load_peer_funds()
    peer_slugs = {m.slug for m in models}
    with pytest.raises(build_slots.HandshakeError) as exc:
        build_slots.load_dfi_commitments(peer_slugs)
    assert "fund-that-does-not-exist" in str(exc.value)


def test_load_deadlines_drops_past_one_off(patch_paths):
    _write_fixture_deadlines(patch_paths)
    models = build_slots.load_deadlines()
    ids = {d.deadline_id for d in models}
    assert "past-one-off" not in ids  # dropped
    assert "future-one-off" in ids
    assert "bii-kinetic-rolling" in ids  # rolling w/ null date kept
    assert all(isinstance(d, Deadline) for d in models)


def test_deadlines_rejects_duplicate_id(patch_paths, tmp_path):
    rows = [
        {"deadline_id": "x", "issuing_body": "Y", "country": "GB",
         "kind": "rfp", "title": "z", "deadline_date": "2099-01-01"},
        {"deadline_id": "x", "issuing_body": "Y", "country": "GB",
         "kind": "rfp", "title": "z2", "deadline_date": "2099-01-02"},
    ]
    (tmp_path / "deadlines.yml").write_text(yaml.safe_dump({"deadlines": rows}))
    with pytest.raises(build_slots.HandshakeError) as exc:
        build_slots.load_deadlines()
    assert "duplicate" in str(exc.value).lower()


def test_load_foundation_lps_ok(patch_paths):
    _write_fixture_peer_funds(patch_paths)
    _write_fixture_foundation_lps(patch_paths)
    models, _ = build_slots.load_peer_funds()
    peer_slugs = {m.slug for m in models}
    fdns = build_slots.load_foundation_lps(peer_slugs)
    assert len(fdns) == 2
    assert all(isinstance(f, FoundationLp) for f in fdns)
    by_slug = {f.slug: f for f in fdns}
    assert by_slug["test-foundation"].foundation_type == "private"
    assert by_slug["test-foundation"].country == "US"
    assert len(by_slug["test-foundation"].known_ingo_gp_commits) == 1
    assert by_slug["test-corp-foundation"].foundation_type == "corporate"


def test_foundation_lps_missing_file_returns_empty(patch_paths):
    """If content/foundation_lps.yml doesn't exist, loader must not raise."""
    _write_fixture_peer_funds(patch_paths)
    models, _ = build_slots.load_peer_funds()
    peer_slugs = {m.slug for m in models}
    fdns = build_slots.load_foundation_lps(peer_slugs)
    assert fdns == []


def test_foundation_lps_handshake_fails_on_bad_fund_slug(patch_paths):
    _write_fixture_peer_funds(patch_paths)
    _write_fixture_foundation_lps(patch_paths, with_bad_slug=True)
    models, _ = build_slots.load_peer_funds()
    peer_slugs = {m.slug for m in models}
    with pytest.raises(build_slots.HandshakeError) as exc:
        build_slots.load_foundation_lps(peer_slugs)
    assert "ghost-fund-slug" in str(exc.value)


def test_foundation_lps_rejects_duplicate_slug(patch_paths, tmp_path):
    rows = [
        {"slug": "dup", "name": "A"},
        {"slug": "dup", "name": "B"},
    ]
    (tmp_path / "foundation_lps.yml").write_text(yaml.safe_dump({"foundations": rows}))
    with pytest.raises(build_slots.HandshakeError) as exc:
        build_slots.load_foundation_lps(set())
    assert "duplicate" in str(exc.value).lower()


def test_load_family_office_lps_ok(patch_paths):
    _write_fixture_peer_funds(patch_paths)
    _write_fixture_family_office_lps(patch_paths)
    models, _ = build_slots.load_peer_funds()
    peer_slugs = {m.slug for m in models}
    famofs = build_slots.load_family_office_lps(peer_slugs)
    assert len(famofs) == 2
    assert all(isinstance(f, FamilyOfficeLp) for f in famofs)
    by_slug = {f.slug: f for f in famofs}
    assert by_slug["test-famof"].category == "family_office"
    assert by_slug["test-famof"].invests_via_fund_lp is True
    assert by_slug["test-faith-based"].category == "faith_based"
    # Tri-state: not specified → null, not False
    assert by_slug["test-faith-based"].invests_via_fund_lp is None


def test_family_office_lps_missing_file_returns_empty(patch_paths):
    famofs = build_slots.load_family_office_lps(set())
    assert famofs == []


# --------------------------------------------------------------- build()


def test_build_end_to_end_fixture(patch_paths):
    _write_fixture_peer_funds(patch_paths)
    _write_fixture_dfi_commits(patch_paths)
    _write_fixture_deadlines(patch_paths)
    _write_fixture_foundation_lps(patch_paths)
    _write_fixture_family_office_lps(patch_paths)

    counts = build_slots.build(verbose=False)
    assert counts["peer_ingo_funds"] == 2
    assert counts["dfi_ingo_commits"] == 1
    assert counts["deadlines"] == 2  # one past entry dropped
    assert counts["foundation_lps"] == 2
    assert counts["family_office_lps"] == 2

    from pipeline import emit
    # JSON files emitted (all six)
    for name in (
        "peer_ingo_funds", "dfi_ingo_commits", "deadlines",
        "foundation_lps", "family_office_lps", "slot_meta",
    ):
        p = emit.DATA_DIR / f"{name}.json"
        assert p.exists(), f"{name}.json not emitted"
        data = json.loads(p.read_text())
        assert data is not None

    # slot_meta shape check
    slot_meta = json.loads((emit.DATA_DIR / "slot_meta.json").read_text())
    assert "country_enum" in slot_meta
    assert "slot_counts" in slot_meta
    assert slot_meta["slot_counts"]["peer_ingo_funds"] == 2
    assert slot_meta["slot_counts"]["foundation_lps"] == 2
    assert slot_meta["slot_counts"]["family_office_lps"] == 2
    assert "foundation_country" in slot_meta["country_counts"]
    assert "family_office_country" in slot_meta["country_counts"]


# --------------------------------------------------------------- integration


def test_real_content_yaml_loads():
    """The actual content/ files must validate end-to-end with no handshake
    breaks. This is the canary for hand-curated registry updates."""
    models, slugs = build_slots.load_peer_funds()
    assert len(models) >= 40, f"peer_funds.yml shrank unexpectedly: {len(models)}"
    assert len(slugs) == len(set(slugs)), "duplicate slugs in peer_funds.yml"

    cards, _ = build_slots.load_dfi_commitments(set(slugs))
    assert len(cards) >= 10, f"dfi_ingo_commitments.yml produced too few cards: {len(cards)}"

    deadlines = build_slots.load_deadlines()
    # Any rolling + future entries should survive; past one-offs drop.
    assert len(deadlines) >= 5

    fdns = build_slots.load_foundation_lps(set(slugs))
    assert len(fdns) >= 10, f"foundation_lps.yml produced too few cards: {len(fdns)}"
    fdn_slugs = [f.slug for f in fdns]
    assert len(fdn_slugs) == len(set(fdn_slugs)), "duplicate slugs in foundation_lps.yml"

    famofs = build_slots.load_family_office_lps(set(slugs))
    assert len(famofs) >= 5, f"family_office_lps.yml produced too few cards: {len(famofs)}"
    famof_slugs = [f.slug for f in famofs]
    assert len(famof_slugs) == len(set(famof_slugs)), "duplicate slugs in family_office_lps.yml"


def test_real_build_emits_all_jsons():
    counts = build_slots.build(verbose=False)
    assert counts["peer_ingo_funds"] > 0
    assert counts["dfi_ingo_commits"] > 0
    assert counts["deadlines"] > 0
    assert counts["foundation_lps"] > 0
    assert counts["family_office_lps"] > 0
    # All emitted JSONs should exist
    for name in (
        "peer_ingo_funds", "dfi_ingo_commits", "deadlines",
        "foundation_lps", "family_office_lps", "slot_meta",
    ):
        assert (DATA_DIR / f"{name}.json").exists(), f"{name}.json missing"


def test_real_build_does_not_emit_market_terms():
    """Slot 4 (market_terms.json) was retired. If this file re-appears,
    a demolition step regressed."""
    p = DATA_DIR / "market_terms.json"
    # Run build to ensure fresh state
    build_slots.build(verbose=False)
    assert not p.exists(), "market_terms.json emitted — slot 4 was killed"


def test_slot_json_countries_are_iso2_or_sentinel():
    """Country fields in emitted JSONs must be 2-letter uppercase or `INT`/`OTHER`."""
    import re
    slot_meta_countries = set()
    for name, country_field in [
        ("peer_ingo_funds.json", "parent_ingo_country"),
        ("dfi_ingo_commits.json", "country"),
        ("deadlines.json", "country"),
        ("foundation_lps.json", "country"),
        ("family_office_lps.json", "country"),
    ]:
        p = DATA_DIR / name
        if not p.exists():
            pytest.skip(f"{name} not built")
        rows = json.loads(p.read_text())
        for r in rows:
            c = r.get(country_field)
            if c is None:
                continue
            assert re.match(r"^[A-Z]{2}$", c) or c in ("INT", "OTHER"), \
                f"{name}: row `{r.get('slug') or r.get('deadline_id')}` has country={c!r}"
            slot_meta_countries.add(c)
