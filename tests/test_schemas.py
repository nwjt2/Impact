"""Schema contract tests.

Models under test:

  - Source           (pipeline/sources.yml)
  - BriefItem        (scraper output, no score)
  - FailureRecord    (health incidents)
  - PeerIngoFund     (slot 1)
  - DfiIngoCommit    + IngoGpCommit, TicketRange, EmergingManagerFacility (slot 2)
  - Deadline         (slot 3)
"""

from datetime import date, datetime, timezone

import pytest

from pipeline.schemas import (
    BriefItem,
    Deadline,
    DfiIngoCommit,
    EmergingManagerFacility,
    FailureRecord,
    IngoGpCommit,
    PeerIngoFund,
    Source,
    TicketRange,
)


# ---- Source ---------------------------------------------------------------


def test_source_fields():
    s = Source(
        id="test",
        name="Test",
        url="https://example.com",
        type="html",
        content_type="guideline",
        expected_minimum_cadence_days=30,
        jurisdiction_tag="uk",
        validation_status="unverified",
        html_selectors={"list_item": "article.news"},
        slots=[3],
        status="active",
    )
    assert s.expected_minimum_cadence_days == 30
    assert s.jurisdiction_tag == "uk"
    assert s.validation_status == "unverified"
    assert s.slots == [3]
    assert s.status == "active"


def test_source_status_parked_allowed():
    s = Source(id="x", name="X", url="https://x", status="parked")
    assert s.status == "parked"


# ---- BriefItem (no scoring) ----------------------------------------------


def test_briefitem_minimal():
    bi = BriefItem(id="abc", source_id="s", source_name="S", title="t", url="https://x")
    assert bi.content_type == "lp_commitment"
    assert bi.summary == ""


def test_briefitem_content_type_literal():
    for ct in ("lp_commitment", "peer_close", "guideline", "regulator_bulletin"):
        bi = BriefItem(id="a", source_id="s", source_name="S", title="t",
                       url="https://x", content_type=ct)
        assert bi.content_type == ct
    with pytest.raises(Exception):
        BriefItem(id="a", source_id="s", source_name="S", title="t",
                  url="https://x", content_type="press_incident")  # retired
    with pytest.raises(Exception):
        BriefItem(id="a", source_id="s", source_name="S", title="t",
                  url="https://x", content_type="bogus")


def test_briefitem_has_no_score_fields():
    """Regression: legacy scoring fields must stay gone."""
    bi = BriefItem(id="a", source_id="s", source_name="S", title="t", url="https://x")
    d = bi.model_dump()
    for k in ("score", "subscores", "matched_tags", "matched_entity_slugs",
              "tier_if_public", "source_jurisdiction_tag",
              "matched_incident_patterns", "matched_org_slug"):
        assert k not in d, f"killed field `{k}` still present on BriefItem"


# ---- FailureRecord --------------------------------------------------------


def test_failurerecord_suspicious_silence_class():
    now = datetime.now(timezone.utc)
    r = FailureRecord(
        source_id="test-source",
        source_name="Test",
        source_url="https://x",
        first_failed_run=now,
        last_failed_run=now,
        failure_class="suspicious_silence",
    )
    assert r.failure_class == "suspicious_silence"


def test_failurerecord_minimal():
    now = datetime.now(timezone.utc)
    r = FailureRecord(
        source_id="giin-blog",
        source_name="GIIN",
        source_url="https://thegiin.org/feed/",
        first_failed_run=now,
        last_failed_run=now,
        failure_class="schema_drift",
    )
    assert r.schema_version == 1
    assert r.severity == "warn"


# ---- Fund model is retired -----------------------------------------------


def test_fund_model_gone():
    """Fund (user-config model) was removed. Importing must raise ImportError."""
    with pytest.raises(ImportError):
        from pipeline.schemas import Fund  # noqa: F401


# ---- PeerIngoFund ---------------------------------------------------------


def test_peer_ingo_fund_minimal():
    f = PeerIngoFund(slug="mercy-corps-ventures", name="Mercy Corps Ventures")
    assert f.slug == "mercy-corps-ventures"
    assert f.status == "raising"  # default
    assert f.sector_tags == []
    assert f.size_usd_m is None


def test_peer_ingo_fund_rejects_bad_status():
    with pytest.raises(Exception):
        PeerIngoFund(slug="x", name="X", status="active")  # not in enum


def test_peer_ingo_fund_accepts_programmatic_vehicle_type():
    """AKAM/NatureVest/etc are programmatic vehicles, not true funds."""
    f = PeerIngoFund(
        slug="akam",
        name="AKAM",
        vehicle_type="programmatic_not_fund",
    )
    assert f.vehicle_type == "programmatic_not_fund"


def test_peer_ingo_fund_wound_down_status():
    f = PeerIngoFund(slug="x", name="X", status="wound_down")
    assert f.status == "wound_down"


# ---- DfiIngoCommit -------------------------------------------------------


def test_ingo_gp_commit_null_amount():
    """commit_usd_m null is the norm (LPA confidentiality). Must validate."""
    c = IngoGpCommit(
        peer_fund_slug="microbuild-fund",
        peer_fund_name="MicroBuild Fund",
        parent_ingo="Habitat for Humanity",
        amount_usd_m=None,
        public_source_url="https://example.com",
    )
    assert c.amount_usd_m is None


def test_dfi_ingo_commit_minimal():
    dfi = DfiIngoCommit(
        slug="bii",
        name="British International Investment",
        country="GB",
    )
    assert dfi.lp_type == "dfi"
    assert dfi.ingo_gp_commit_count_5y == 0
    assert dfi.typical_ticket_usd_m_range is None


def test_dfi_ingo_commit_multilateral_policy_remit():
    """EIB HQ=LU but policy_remit should surface as EU."""
    dfi = DfiIngoCommit(
        slug="eib",
        name="European Investment Bank",
        country="LU",
        policy_remit="EU",
        lp_type="multilateral",
    )
    assert dfi.policy_remit == "EU"
    assert dfi.lp_type == "multilateral"


def test_ticket_range_fields():
    t = TicketRange(min=5, median=20, max=50, n=7)
    assert t.n == 7
    assert t.median == 20


def test_emerging_manager_facility():
    e = EmergingManagerFacility(
        exists=True,
        program_name="Kinetic",
        application_url="https://example.com",
    )
    assert e.exists


# ---- Deadline -------------------------------------------------------------


def test_deadline_minimal():
    d = Deadline(
        deadline_id="bii-kinetic-rolling",
        issuing_body="BII",
        country="GB",
        kind="rolling_application",
        title="BII Kinetic",
    )
    assert d.source_kind == "curated"  # default
    assert d.deadline_date is None


def test_deadline_rejects_bad_kind():
    with pytest.raises(Exception):
        Deadline(
            deadline_id="x",
            issuing_body="Y",
            kind="bogus",
            title="z",
        )


def test_outreach_import_fails():
    """outreach.py is deleted. Confirm no other module imports it."""
    with pytest.raises(ImportError):
        from pipeline.schemas import Outreach  # noqa: F401
