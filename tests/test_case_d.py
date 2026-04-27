"""Case D — suspicious_silence cadence check.

Unit tests for pipeline.health.check_suspicious_silence:

  * zero items + cadence exceeded                    → fires incident
  * zero items + under cadence                       → accumulates, no fire
  * >=1 items                                        → resets state
  * cadence=None                                     → skipped (no fire)
  * source absent from per_source_stats              → no state corruption
  * repeated zero-runs at/above cadence              → keeps firing
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.health import (
    check_suspicious_silence,
    file_incident,
    _update_meta_silence_count,
)
from pipeline.schemas import FailureRecord


def _src(
    sid: str,
    *,
    cadence: int | None = 3,
    name: str | None = None,
) -> dict:
    return {
        "id": sid,
        "name": name or f"Test {sid}",
        "url": f"https://example.invalid/{sid}/",
        "expected_minimum_cadence_days": cadence,
    }


def _stat(sid: str, rows: int) -> dict:
    return {"id": sid, "rows": rows, "status": "green"}


def _list_silence_files(open_dir: Path) -> list[Path]:
    return sorted(
        p for p in open_dir.glob("*.md")
        if "suspicious_silence" in p.read_text(encoding="utf-8", errors="replace")
    )


def test_silence_fires_when_consecutive_zero_exceeds_cadence(tmp_path):
    state = tmp_path / "source_cadence.json"
    open_dir = tmp_path / "open"
    open_dir.mkdir()

    sources = [_src("feed-a", cadence=2)]

    # Run 1: rows=0 → consecutive=1, under cadence (2).
    filed1 = check_suspicious_silence(
        sources=sources,
        per_source_stats=[_stat("feed-a", 0)],
        state_path=state,
        open_dir=open_dir,
    )
    assert filed1 == []

    # Run 2: rows=0 → consecutive=2, still not > cadence (2 > 2 is false).
    filed2 = check_suspicious_silence(
        sources=sources,
        per_source_stats=[_stat("feed-a", 0)],
        state_path=state,
        open_dir=open_dir,
    )
    assert filed2 == []

    # Run 3: rows=0 → consecutive=3 > 2 → FIRES.
    filed3 = check_suspicious_silence(
        sources=sources,
        per_source_stats=[_stat("feed-a", 0)],
        state_path=state,
        open_dir=open_dir,
    )
    assert len(filed3) == 1
    assert filed3[0].failure_class == "suspicious_silence"
    assert filed3[0].source_id == "feed-a"
    assert filed3[0].consecutive_failures == 3

    # State persisted, and the emitted state has the expected shape.
    data = json.loads(state.read_text())
    assert data["by_source"]["feed-a"]["consecutive_zero_days"] == 3
    assert data["by_source"]["feed-a"]["last_nonzero_run_date"] is None


def test_silence_resets_on_nonzero_rows(tmp_path):
    state = tmp_path / "source_cadence.json"
    open_dir = tmp_path / "open"
    open_dir.mkdir()

    sources = [_src("feed-b", cadence=2)]

    # Two zero runs.
    for _ in range(2):
        check_suspicious_silence(
            sources=sources,
            per_source_stats=[_stat("feed-b", 0)],
            state_path=state,
            open_dir=open_dir,
        )
    s = json.loads(state.read_text())
    assert s["by_source"]["feed-b"]["consecutive_zero_days"] == 2

    # Now a nonzero run resets.
    check_suspicious_silence(
        sources=sources,
        per_source_stats=[_stat("feed-b", 4)],
        state_path=state,
        open_dir=open_dir,
    )
    s = json.loads(state.read_text())
    assert s["by_source"]["feed-b"]["consecutive_zero_days"] == 0
    assert s["by_source"]["feed-b"]["last_nonzero_run_date"] is not None
    assert s["by_source"]["feed-b"]["last_item_count"] == 4


def test_silence_skips_when_cadence_is_none(tmp_path):
    """Source with expected_minimum_cadence_days=null should never fire."""
    state = tmp_path / "source_cadence.json"
    open_dir = tmp_path / "open"
    open_dir.mkdir()

    sources = [_src("feed-c", cadence=None)]

    for _ in range(10):
        filed = check_suspicious_silence(
            sources=sources,
            per_source_stats=[_stat("feed-c", 0)],
            state_path=state,
            open_dir=open_dir,
        )
        assert filed == []

    # State still tracks the zero-runs (so a later cadence config change is
    # instantly actionable), but no incident was filed.
    s = json.loads(state.read_text())
    assert s["by_source"]["feed-c"]["consecutive_zero_days"] == 10
    assert _list_silence_files(open_dir) == []


def test_silence_tolerates_absent_stats(tmp_path):
    """Source listed in sources.yml but not in this run's stats — skip."""
    state = tmp_path / "source_cadence.json"
    open_dir = tmp_path / "open"
    open_dir.mkdir()

    sources = [_src("feed-d", cadence=1)]
    # per_source_stats is empty — pipeline didn't poll this source
    filed = check_suspicious_silence(
        sources=sources,
        per_source_stats=[],
        state_path=state,
        open_dir=open_dir,
    )
    assert filed == []
    s = json.loads(state.read_text())
    # Entry exists (so last_checked is stamped) but no zero-day increment.
    assert s["by_source"]["feed-d"]["consecutive_zero_days"] == 0


def test_silence_fires_repeatedly_after_threshold(tmp_path):
    """Once past cadence, every subsequent zero-run fires a new FailureRecord.

    Filesystem-level uniqueness is not asserted here — file_incident()
    keys its path by the ISO-second of first_failed_run, which collides
    when the test invokes multiple runs within one second. In production
    the cadence check runs 1x/day so the timestamp discriminator is
    adequate. What matters is the RETURN value of check_suspicious_silence:
    every run past threshold must return a record.
    """
    state = tmp_path / "source_cadence.json"
    open_dir = tmp_path / "open"
    open_dir.mkdir()

    sources = [_src("feed-e", cadence=1)]
    returned: list[list] = []
    for _ in range(5):
        returned.append(check_suspicious_silence(
            sources=sources,
            per_source_stats=[_stat("feed-e", 0)],
            state_path=state,
            open_dir=open_dir,
        ))
    # Run 1: consec=1, 1>1 false.          → []
    # Run 2: consec=2, 2>1 TRUE.           → [rec]
    # Run 3..5: same, each returns [rec].
    assert [len(r) for r in returned] == [0, 1, 1, 1, 1]

    # consecutive_failures on each fired record should track consecutive_zero_days.
    fired = [r[0] for r in returned if r]
    assert [rec.consecutive_failures for rec in fired] == [2, 3, 4, 5]

    # At least one incident file must exist (intra-second collisions acceptable).
    assert _list_silence_files(open_dir)


def test_failure_record_accepts_suspicious_silence():
    """FailureClass must include suspicious_silence."""
    rec = FailureRecord(
        source_id="test",
        source_name="Test",
        source_url="https://example.invalid",
        first_failed_run=datetime.now(timezone.utc),
        last_failed_run=datetime.now(timezone.utc),
        failure_class="suspicious_silence",
        severity="warn",
    )
    assert rec.failure_class == "suspicious_silence"


def test_incident_front_matter_contains_cadence_fields(tmp_path):
    """The incident file written for Case D must carry the cadence evidence."""
    state = tmp_path / "source_cadence.json"
    open_dir = tmp_path / "open"
    open_dir.mkdir()

    # Monkey-patch OPEN for file_incident's default; we pass open_dir through
    # the higher-level function but file_incident itself uses module-level OPEN.
    import pipeline.health as h
    original_open = h.OPEN
    h.OPEN = open_dir
    try:
        sources = [_src("feed-f", cadence=1, name="Feed F")]
        # Fire the incident on run 2.
        check_suspicious_silence(
            sources=sources,
            per_source_stats=[_stat("feed-f", 0)],
            state_path=state,
            open_dir=open_dir,
        )
        filed = check_suspicious_silence(
            sources=sources,
            per_source_stats=[_stat("feed-f", 0)],
            state_path=state,
            open_dir=open_dir,
        )
        assert len(filed) == 1
    finally:
        h.OPEN = original_open

    files = list(open_dir.glob("*.md"))
    assert files
    text = files[0].read_text()
    assert "failure_class: suspicious_silence" in text
    assert "feed-f" in text
    assert "consecutive_zero_days: 2" in text
    assert "expected_minimum_cadence_days: 1" in text


def test_meta_update_counts_silence_incidents(tmp_path, monkeypatch):
    """_update_meta_silence_count reads meta.json, updates, writes back."""
    meta = tmp_path / "meta.json"
    open_dir = tmp_path / "open"
    open_dir.mkdir()

    meta.write_text(json.dumps({
        "suspicious_silence_count": 0,
        "status": "green",
        "open_incidents": [],
    }))

    # File two suspicious_silence incidents by hand.
    import pipeline.health as h
    monkeypatch.setattr(h, "META_JSON", meta)
    monkeypatch.setattr(h, "OPEN", open_dir)

    for sid in ("feed-g", "feed-h"):
        now = datetime.now(timezone.utc)
        file_incident(FailureRecord(
            source_id=sid,
            source_name=sid,
            source_url="https://example.invalid",
            first_failed_run=now,
            last_failed_run=now,
            failure_class="suspicious_silence",
            severity="warn",
        ))

    count = _update_meta_silence_count(open_dir=open_dir)
    assert count == 2

    data = json.loads(meta.read_text())
    assert data["suspicious_silence_count"] == 2
    assert data["status"] == "amber"  # upgraded from green
    assert len(data["open_incidents"]) == 2
