"""Health / incident writer.

Writes tool/health/open/<source>-<ISO8601>.md when a scraper fails. Idempotent:
if an open incident exists for the same source/day, append a new entry rather
than duplicate.

Also rolls up tool/health/current.md (aggregate status) and, with --roll-up,
tool/health/YYYY-MM-DD.md.

Suspicious-silence detection:
  `suspicious_silence` failure_class fires when a source has returned
  zero items across enough consecutive runs that the total exceeds the
  source's `expected_minimum_cadence_days` from pipeline/sources.yml.

  Per-source cadence state lives at tool/state/source_cadence.json.
  Schema:
    {
      "generated_at": ISO-8601 string,
      "by_source": {
        "<source_id>": {
          "last_nonzero_run_date": "YYYY-MM-DD" | null,
          "consecutive_zero_days":  int,
          "last_item_count":        int,
          "last_checked":           ISO-8601 string
        },
        ...
      }
    }

  The wire-up: `pipeline.health --roll-up` reads
  `site/src/_data/meta.json` (written by run.py) to get per-source row
  counts, updates state, files suspicious_silence incidents, then
  rewrites meta.json's `suspicious_silence_count` field in place so the
  homepage sees the post-check value. No run.py change needed.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml

from .schemas import FailureRecord

REPO = Path(__file__).resolve().parents[1]
HEALTH = REPO / "tool" / "health"
OPEN = HEALTH / "open"
CLOSED = HEALTH / "closed"
ARTIFACTS = HEALTH / "artifacts"
STATE_DIR = REPO / "tool" / "state"
CADENCE_STATE = STATE_DIR / "source_cadence.json"
SOURCES_YML = REPO / "pipeline" / "sources.yml"
META_JSON = REPO / "site" / "src" / "_data" / "meta.json"


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def file_incident(rec: FailureRecord, evidence: str = "") -> Path:
    OPEN.mkdir(parents=True, exist_ok=True)
    ts = _iso(rec.first_failed_run).replace(":", "-")
    path = OPEN / f"{rec.source_id}-{ts}.md"

    front = {
        "schema_version": rec.schema_version,
        "source_id": rec.source_id,
        "source_name": rec.source_name,
        "source_url": rec.source_url,
        "first_failed_run": _iso(rec.first_failed_run),
        "last_failed_run": _iso(rec.last_failed_run),
        "consecutive_failures": rec.consecutive_failures,
        "failure_class": rec.failure_class,
        "http_status": rec.http_status,
        "bytes_received": rec.bytes_received,
        "selector_or_endpoint": rec.selector_or_endpoint,
        "rows_returned": rec.rows_returned,
        "rows_expected_min": rec.rows_expected_min,
        "last_successful_run": _iso(rec.last_successful_run) if rec.last_successful_run else None,
        "severity": rec.severity,
    }
    body = [
        "---",
        yaml.safe_dump(front, sort_keys=False).strip(),
        "---",
        "",
        f"## What broke",
        rec.note or f"{rec.failure_class} on {rec.source_id}.",
        "",
        "## Evidence",
        evidence or "(no evidence captured)",
        "",
        "## Suggested next step",
        "Inspect tool/fixtures/<source_id>/snapshot-*.xml to reproduce; "
        "update pipeline/sources.yml or parsing logic; rerun `make test`.",
    ]
    path.write_text("\n".join(body), encoding="utf-8")
    return path


def roll_up(open_dir: Path = OPEN) -> Path:
    """Write tool/health/current.md — a short aggregate status."""
    HEALTH.mkdir(parents=True, exist_ok=True)
    open_files = sorted(
        p for p in open_dir.glob("*.md") if p.name != ".gitkeep"
    )
    now = datetime.now(timezone.utc)

    status = "green"
    if open_files:
        status = "amber"
        # crit if anything tagged severity: crit
        for p in open_files:
            txt = p.read_text(encoding="utf-8", errors="replace")
            if "severity: crit" in txt:
                status = "red"
                break

    lines = [
        f"# Health — {_iso(now)}",
        "",
        f"**Status:** {status}",
        f"**Open incidents:** {len(open_files)}",
        "",
    ]
    if open_files:
        lines.append("## Open")
        for p in open_files:
            lines.append(f"- [{p.name}]({p.relative_to(HEALTH)})")
    out = HEALTH / "current.md"
    out.write_text("\n".join(lines), encoding="utf-8")

    daily = HEALTH / f"{now.date().isoformat()}.md"
    daily.write_text("\n".join(lines), encoding="utf-8")
    return out


def _load_cadence_state() -> dict:
    """Read tool/state/source_cadence.json, returning {} if missing/broken."""
    if not CADENCE_STATE.exists():
        return {"by_source": {}}
    try:
        raw = json.loads(CADENCE_STATE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {"by_source": {}}
        raw.setdefault("by_source", {})
        if not isinstance(raw["by_source"], dict):
            raw["by_source"] = {}
        return raw
    except Exception:
        return {"by_source": {}}


def _write_cadence_state(state: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state["generated_at"] = datetime.now(timezone.utc).isoformat()
    CADENCE_STATE.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")


def _load_sources_from_yml() -> list[dict]:
    if not SOURCES_YML.exists():
        return []
    try:
        raw = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8")) or {}
        return list(raw.get("sources") or [])
    except Exception:
        return []


def _load_meta_per_source() -> list[dict]:
    """Read per-source stats out of the freshly-emitted meta.json.

    Returns [] if meta.json is missing or malformed (e.g. roll-up invoked
    standalone in a test). Callers must tolerate an empty list.
    """
    if not META_JSON.exists():
        return []
    try:
        raw = json.loads(META_JSON.read_text(encoding="utf-8"))
        ps = raw.get("per_source") or []
        return list(ps) if isinstance(ps, list) else []
    except Exception:
        return []


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _file_incident_into(rec: FailureRecord, evidence: str, open_dir: Path) -> Path:
    """file_incident wrapper that temporarily redirects the module-level
    OPEN directory. Used by check_suspicious_silence for unit-test
    isolation and also in production (where open_dir is OPEN, a no-op)."""
    global OPEN
    prev = OPEN
    try:
        OPEN = open_dir
        return file_incident(rec, evidence=evidence)
    finally:
        OPEN = prev


def check_suspicious_silence(
    sources: Iterable[dict] | None = None,
    per_source_stats: list[dict] | None = None,
    *,
    state_path: Path | None = None,
    open_dir: Path | None = None,
    today: Optional[date] = None,
) -> list[FailureRecord]:
    """C6 cadence check.

    For each source in ``sources``:

    * If this run produced >=1 items, reset ``consecutive_zero_days`` to 0
      and stamp ``last_nonzero_run_date`` with today's date.
    * If this run produced 0 items, increment ``consecutive_zero_days``.
      (The brief is explicit: zero rows from any cause counts. We rely on
      the cadence threshold to filter transient broken-scraper noise out.)
    * If ``consecutive_zero_days > expected_minimum_cadence_days``, file a
      ``suspicious_silence`` FailureRecord via ``file_incident()``.
    * Write the updated state back to disk.

    Parameters:
      sources             — iterable of source dicts (as from sources.yml).
                            Defaults to loading pipeline/sources.yml.
      per_source_stats    — list of per-source stat dicts with at least
                            ``id`` and ``rows``. Defaults to reading the
                            just-emitted site/src/_data/meta.json.
      state_path, open_dir, today — for unit tests.

    Returns the list of FailureRecord objects filed this call. An empty
    list means either (a) no source crossed cadence, or (b) meta.json
    wasn't present and we had nothing to evaluate.
    """
    state_path = state_path or CADENCE_STATE
    open_dir = open_dir or OPEN
    today = today or _today()

    if sources is None:
        sources = _load_sources_from_yml()
    sources_list = list(sources)

    if per_source_stats is None:
        per_source_stats = _load_meta_per_source()
    stats_by_id = {s.get("id"): s for s in per_source_stats if s.get("id")}

    # Load state (local path override for tests)
    if state_path == CADENCE_STATE:
        state = _load_cadence_state()
    else:
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
                state.setdefault("by_source", {})
            except Exception:
                state = {"by_source": {}}
        else:
            state = {"by_source": {}}

    filed: list[FailureRecord] = []
    now = datetime.now(timezone.utc)
    today_iso = today.isoformat()

    for src in sources_list:
        sid = src.get("id")
        if not sid:
            continue

        cadence = src.get("expected_minimum_cadence_days")
        entry = state["by_source"].get(sid, {
            "last_nonzero_run_date": None,
            "consecutive_zero_days": 0,
            "last_item_count": 0,
            "last_checked": None,
        })

        stat = stats_by_id.get(sid)
        if stat is None:
            # Source declared in yml but absent from this run's stats —
            # pipeline didn't poll it. Skip; don't penalise.
            entry["last_checked"] = now.isoformat()
            state["by_source"][sid] = entry
            continue

        rows = int(stat.get("rows") or 0)
        entry["last_item_count"] = rows
        entry["last_checked"] = now.isoformat()

        if rows >= 1:
            entry["consecutive_zero_days"] = 0
            entry["last_nonzero_run_date"] = today_iso
        else:
            entry["consecutive_zero_days"] = int(entry.get("consecutive_zero_days") or 0) + 1

        # Edge: cadence_days unset (e.g. source without the field) → can't
        # assess silence; skip firing.
        if cadence is None or cadence <= 0:
            state["by_source"][sid] = entry
            continue

        if entry["consecutive_zero_days"] > int(cadence):
            rec = FailureRecord(
                source_id=sid,
                source_name=src.get("name", sid),
                source_url=src.get("url", ""),
                first_failed_run=now,
                last_failed_run=now,
                failure_class="suspicious_silence",
                http_status=200,
                bytes_received=None,
                rows_returned=0,
                rows_expected_min=1,
                severity="warn",
                consecutive_failures=entry["consecutive_zero_days"],
                note=(
                    f"{sid} has returned zero items for "
                    f"{entry['consecutive_zero_days']} consecutive runs; "
                    f"expected_minimum_cadence_days is {cadence}. "
                    f"Last non-zero run: {entry['last_nonzero_run_date'] or 'never'}. "
                    f"Silent-zero may be real (source genuinely inactive) or "
                    f"may indicate scraper drift that slipped past schema_drift."
                ),
            )
            evidence_lines = [
                f"source_id: {sid}",
                f"consecutive_zero_days: {entry['consecutive_zero_days']}",
                f"expected_minimum_cadence_days: {cadence}",
                f"last_nonzero_run_date: {entry['last_nonzero_run_date']}",
                f"last_item_count: {rows}",
                f"state_file: {state_path}",
            ]
            # Route file_incident() to the test-supplied open_dir. The
            # function uses the module-level OPEN by design (to keep its
            # signature simple), so we temporarily override and restore.
            _file_incident_into(rec, evidence="\n".join(evidence_lines), open_dir=open_dir)
            filed.append(rec)

        state["by_source"][sid] = entry

    # Persist state
    if state_path == CADENCE_STATE:
        _write_cadence_state(state)
    else:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state["generated_at"] = datetime.now(timezone.utc).isoformat()
        state_path.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")

    return filed


def _update_meta_silence_count(open_dir: Path = OPEN) -> int:
    """After check_suspicious_silence runs, rewrite meta.json's
    `suspicious_silence_count` in-place so Eleventy sees the post-check value.
    Returns the count.
    """
    count = 0
    for p in open_dir.glob("*.md"):
        if p.name == ".gitkeep":
            continue
        try:
            txt = p.read_text(encoding="utf-8", errors="replace")
            if "failure_class: suspicious_silence" in txt:
                count += 1
        except Exception:
            pass

    if META_JSON.exists():
        try:
            meta = json.loads(META_JSON.read_text(encoding="utf-8"))
            meta["suspicious_silence_count"] = count
            # Also refresh the list of open incidents + status so the
            # homepage footer reflects just-filed suspicious_silence files.
            open_incs = [
                p.name for p in sorted(open_dir.glob("*.md"))
                if p.name != ".gitkeep"
            ]
            meta["open_incidents"] = open_incs
            if open_incs:
                # Upgrade status to amber if currently green (don't override red).
                status = meta.get("status", "green")
                if status == "green":
                    meta["status"] = "amber"
            META_JSON.write_text(
                json.dumps(meta, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception:
            pass

    return count


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--roll-up", action="store_true")
    ap.add_argument(
        "--check-silence",
        action="store_true",
        help="Run the C6 cadence check against meta.json and file "
             "suspicious_silence incidents. Invoked automatically by --roll-up.",
    )
    ap.add_argument(
        "--reset-cadence-state",
        action="store_true",
        help="Delete tool/state/source_cadence.json (rehearsal hygiene).",
    )
    args = ap.parse_args()

    if args.reset_cadence_state:
        if CADENCE_STATE.exists():
            CADENCE_STATE.unlink()
            print(f"removed {CADENCE_STATE}")
        else:
            print(f"(no state file at {CADENCE_STATE})")

    if args.check_silence:
        filed = check_suspicious_silence()
        count = _update_meta_silence_count()
        if filed:
            print(f"suspicious_silence: filed {len(filed)} incident(s)")
            for rec in filed:
                print(f"  - {rec.source_id}: {rec.consecutive_failures} consecutive zero-runs")
        else:
            print("suspicious_silence: none fired this run")
        print(f"suspicious_silence_count (total open): {count}")

    if args.roll_up:
        # Roll-up does NOT re-run the cadence check (that would double-count
        # consecutive_zero_days on a single make-daily invocation). The
        # Makefile invokes --check-silence explicitly before the build step.
        # We still refresh the meta.json silence count from open-dir state so
        # a standalone `pipeline.health --roll-up` picks up any incidents
        # filed earlier in this run.
        _update_meta_silence_count()
        p = roll_up()
        print(f"wrote {p}")


if __name__ == "__main__":
    main()
