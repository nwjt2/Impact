"""Regression test for sync_catalogue_from_yaml's operator-edit preservation.

The contract from network/CLAUDE.md:

    The columns the script ADDS (`Fund Type`, `Pipeline Status`, `Portfolio
    Page URL`, `LP Page URL`) are operator-editable and preserved.

This was silently broken from inception until 2026-04-30 — the script
rebuilt impact_funds.csv from peer_funds.yml on every run with hard-coded
blanks for Portfolio/LP URLs and a recomputed Pipeline Status from
ACTIVE_FUND_SLUGS, wiping any operator edits whenever a sync ran for an
unrelated reason (e.g. picking up a new family-office YAML entry).

Pipeline Status carve-out: YAML / skip-list truth wins for
`wound_down` and `blocked`. Operator's CSV value wins for
`active` / `pending_onboard`.
"""
from __future__ import annotations

from network.dashboard_prep.prep_scripts.sync_catalogue_from_yaml import (
    build_impact_funds_and_skip_list,
)


def _fund(slug: str, **overrides) -> dict:
    base = {
        "slug": slug,
        "name": slug.replace("-", " ").title(),
        "status": "deployed",
        "vehicle_type": "closed_end_fund",
    }
    base.update(overrides)
    return base


def _existing(slug: str, **fields) -> dict:
    base = {"Fund Slug": slug}
    base.update(fields)
    return base


def test_preserves_lp_page_url():
    """LP Page URL set by the operator survives a re-sync."""
    doc = {"peer_funds": [_fund("not-in-active-list")]}
    existing = [_existing("not-in-active-list", **{"LP Page URL": "https://primary.source/pr"})]

    rows, _ = build_impact_funds_and_skip_list(doc, existing)

    assert rows[0]["LP Page URL"] == "https://primary.source/pr"


def test_preserves_portfolio_page_url():
    doc = {"peer_funds": [_fund("not-in-active-list")]}
    existing = [_existing("not-in-active-list", **{"Portfolio Page URL": "https://fund.com/portfolio"})]

    rows, _ = build_impact_funds_and_skip_list(doc, existing)

    assert rows[0]["Portfolio Page URL"] == "https://fund.com/portfolio"


def test_preserves_fund_type_override():
    """Operator override of Fund Type wins over the regex classifier."""
    doc = {"peer_funds": [_fund("some-fund", notes="invests directly in companies across MENA")]}
    existing = [_existing("some-fund", **{"Fund Type": "fof"})]

    rows, _ = build_impact_funds_and_skip_list(doc, existing)

    assert rows[0]["Fund Type"] == "fof"


def test_preserves_active_pipeline_status_for_non_active_slug():
    """Operator-promoted active CSV value survives even when slug isn't in
    ACTIVE_FUND_SLUGS — the original incident from 2026-04-30."""
    doc = {"peer_funds": [_fund("operator-promoted-fund")]}
    existing = [_existing("operator-promoted-fund", **{"Pipeline Status": "active"})]

    rows, _ = build_impact_funds_and_skip_list(doc, existing)

    assert rows[0]["Pipeline Status"] == "active"


def test_yaml_wound_down_overrides_csv_active():
    """Terminal state in YAML wins — operator can't keep a wound-down fund active."""
    doc = {"peer_funds": [_fund("retired-fund", status="wound_down")]}
    existing = [_existing("retired-fund", **{"Pipeline Status": "active"})]

    rows, _ = build_impact_funds_and_skip_list(doc, existing)

    assert rows[0]["Pipeline Status"] == "wound_down"


def test_new_fund_uses_default_pipeline_status():
    """Funds not in existing CSV get the rule-based default."""
    doc = {"peer_funds": [_fund("brand-new-fund")]}
    existing = []

    rows, _ = build_impact_funds_and_skip_list(doc, existing)

    assert rows[0]["Pipeline Status"] == "pending_onboard"
    assert rows[0]["LP Page URL"] == ""
    assert rows[0]["Portfolio Page URL"] == ""


def test_existing_rows_arg_is_optional():
    """Backwards-compat: callers that don't pass existing_rows still work."""
    doc = {"peer_funds": [_fund("brand-new-fund")]}

    rows, _ = build_impact_funds_and_skip_list(doc)

    assert rows[0]["Pipeline Status"] == "pending_onboard"


def test_empty_csv_string_does_not_overwrite_default():
    """Existing rows with blank columns shouldn't poison the new row."""
    doc = {"peer_funds": [_fund("some-fund")]}
    existing = [_existing("some-fund", **{"Pipeline Status": "", "LP Page URL": "", "Fund Type": ""})]

    rows, _ = build_impact_funds_and_skip_list(doc, existing)

    assert rows[0]["Pipeline Status"] == "pending_onboard"
    assert rows[0]["LP Page URL"] == ""
    assert rows[0]["Fund Type"] == "unclassified"
