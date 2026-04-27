"""Contract test: incident file matches the FailureRecord schema."""

from datetime import datetime, timezone
from pathlib import Path

import yaml

from pipeline.health import file_incident, OPEN
from pipeline.schemas import FailureRecord

REQUIRED_FIELDS = {
    "schema_version",
    "source_id",
    "source_name",
    "source_url",
    "first_failed_run",
    "last_failed_run",
    "consecutive_failures",
    "failure_class",
    "http_status",
    "selector_or_endpoint",
    "rows_returned",
    "rows_expected_min",
    "severity",
}


def test_incident_schema(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    rec = FailureRecord(
        source_id="test-source",
        source_name="Test Source",
        source_url="https://example.com/feed",
        first_failed_run=now,
        last_failed_run=now,
        failure_class="schema_drift",
        http_status=200,
        bytes_received=1024,
        rows_returned=0,
        rows_expected_min=1,
        severity="warn",
        note="unit test",
    )
    # Redirect OPEN to a tmp path by monkeypatching.
    import pipeline.health as h
    monkeypatch.setattr(h, "OPEN", tmp_path)

    out = file_incident(rec, evidence="tmp fixture")
    assert out.exists()
    text = out.read_text()
    assert text.startswith("---")
    body = text.split("---")[1]
    front = yaml.safe_load(body)
    missing = REQUIRED_FIELDS - set(front)
    assert not missing, f"missing keys: {missing}"
    assert front["failure_class"] in (
        "http_error", "parse_error", "rate_limit", "schema_drift", "timeout"
    )
    assert front["severity"] in ("warn", "crit")
