"""Pipeline orchestrator.

Usage:
  python -m pipeline.run                     # fixture mode (default)
  INGO_LIVE=1 python -m pipeline.run --refresh-fixtures  # live, one-shot
  INGO_REHEARSE_CASE=A python -m pipeline.run            # rehearsal A (broken)
  INGO_REHEARSE_CASE=B python -m pipeline.run            # rehearsal B (404)
  INGO_REHEARSE_CASE=C python -m pipeline.run            # rehearsal C (silent-zero)
  INGO_REHEARSE_CASE=D python -m pipeline.run            # rehearsal D (cadence)

Scrapers run against `pipeline/sources.yml` as self-heal telemetry. A future
pass will refresh `last_seen_at` on matched rows in
`content/peer_funds.yml` / `content/dfi_ingo_commitments.yml` /
`content/deadlines.yml`.

What this file does today:

  1. Iterate sources.yml (status == "active" only).
  2. For each source, dispatch to its scraper, capture per-source stats,
     file suspicious_silence / http_error / parse_error / schema_drift
     incidents to tool/health/open/ on failure.
  3. Normalize raw scraper output into BriefItem models.
  4. Emit:
       - site/src/_data/meta.json   — per-source + run metadata
       - site/src/_data/health.json — open-incident summary

The three slot JSONs are emitted by `pipeline/build_slots.py`, called
after this module by `make daily`.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from . import emit, health, normalize
from .schemas import BriefItem, FailureRecord
from .scrapers.base import FIXTURES, BaseScraper
from .scrapers.rss import RssScraper
from .scrapers.html import HtmlScraper
from .scrapers.api import ApiScraper

REPO = Path(__file__).resolve().parents[1]
SOURCES_YML = REPO / "pipeline" / "sources.yml"


def load_sources() -> list[dict]:
    raw = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8")) or {}
    out: list[dict] = []
    for s in (raw.get("sources") or []):
        if (s.get("status") or "active") == "parked":
            continue
        out.append(s)
    return out


def scraper_for(source: dict) -> BaseScraper:
    t = (source.get("type") or "rss").lower()
    if t == "rss":
        return RssScraper(source)
    if t == "html":
        return HtmlScraper(source)
    if t == "api":
        return ApiScraper(source)
    raise ValueError(f"unknown source type: {t}")


def refresh_fixtures(sources: list[dict]) -> None:
    """One-shot live fetch; save snapshot-ok.{xml,html,json} per source."""
    import httpx

    for s in sources:
        stype = (s.get("type") or "rss").lower()
        ext = {"rss": "xml", "html": "html", "api": "json"}.get(stype, "xml")
        dest = FIXTURES / s["id"] / f"snapshot-ok.{ext}"
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            contact = os.environ.get("INGO_CONTACT", "set-INGO_CONTACT-env-var@example.com")
            headers = {"User-Agent": f"FirstCloseTool {contact}"}
            if stype == "api":
                headers.update((s.get("api_params") or {}).get("headers") or {})
                params = (s.get("api_params") or {}).get("query") or {}
            else:
                params = None
            with httpx.Client(http2=True, timeout=20.0, follow_redirects=True) as c:
                r = c.get(s["url"], headers=headers, params=params)
                if r.status_code == 200 and r.content:
                    dest.write_bytes(r.content)
                    print(f"[ok]   {s['id']:30s}  bytes={len(r.content)}")
                else:
                    print(f"[fail] {s['id']:30s}  status={r.status_code}")
        except Exception as e:
            print(f"[fail] {s['id']:30s}  {e}")


def run(sources: list[dict]) -> dict[str, Any]:
    t0 = time.time()
    all_items: list[BriefItem] = []
    per_source_stats: list[dict] = []
    incidents_filed = 0
    sources_polled = 0

    for s in sources:
        sources_polled += 1
        stats = {
            "id": s["id"],
            "name": s["name"],
            "status": "green",
            "rows": 0,
            "error": None,
            "type": s.get("type", "rss"),
            "content_type": s.get("content_type", "lp_commitment"),
            "jurisdiction_tag": s.get("jurisdiction_tag"),
            "slots": s.get("slots") or [],
            "expected_minimum_cadence_days": s.get("expected_minimum_cadence_days"),
        }
        try:
            scraper = scraper_for(s)
        except Exception as e:
            stats["status"] = "red"
            stats["error"] = f"scraper init: {e!r}"
            per_source_stats.append(stats)
            continue

        t_src0 = time.time()
        try:
            fr = scraper.fetch()
            raw_items: list[dict] = []
            failure_class = None
            note = ""

            if fr.http_status >= 400:
                failure_class = "http_error"
                note = f"HTTP {fr.http_status} on {s['id']}"
            elif not fr.body:
                failure_class = "http_error"
                note = "empty body"
            else:
                try:
                    raw_items = scraper.parse(fr)
                except Exception as e:
                    failure_class = "parse_error"
                    note = f"parse: {e!r}"

            if not failure_class and len(raw_items) == 0:
                failure_class = "schema_drift"
                note = "HTTP OK but 0 items parsed (silent zero)"

            if failure_class:
                now = datetime.now(timezone.utc)
                rec = FailureRecord(
                    source_id=s["id"],
                    source_name=s["name"],
                    source_url=s["url"],
                    first_failed_run=now,
                    last_failed_run=now,
                    failure_class=failure_class,
                    http_status=fr.http_status,
                    bytes_received=len(fr.body),
                    rows_returned=0,
                    rows_expected_min=1,
                    severity="warn",
                    note=note,
                )
                health.file_incident(rec, evidence=f"fixture case: {fr.fixture_case}")
                incidents_filed += 1
                stats["status"] = "red"
                stats["error"] = note
            else:
                ct = s.get("content_type", "lp_commitment")
                for raw in raw_items:
                    bi = normalize.to_brief_item(raw, s["id"], s["name"], content_type=ct)
                    if bi is not None:
                        all_items.append(bi)
                stats["rows"] = len(raw_items)

        except Exception as e:
            now = datetime.now(timezone.utc)
            rec = FailureRecord(
                source_id=s["id"],
                source_name=s["name"],
                source_url=s["url"],
                first_failed_run=now,
                last_failed_run=now,
                failure_class="parse_error",
                note=f"unexpected: {e!r}",
            )
            health.file_incident(rec)
            incidents_filed += 1
            stats["status"] = "red"
            stats["error"] = str(e)

        stats["elapsed_ms"] = int((time.time() - t_src0) * 1000)
        per_source_stats.append(stats)

    # URL-canon dedup within this run.
    seen_urls: set[str] = set()
    deduped: list[BriefItem] = []
    for it in all_items:
        if it.url in seen_urls:
            continue
        seen_urls.add(it.url)
        deduped.append(it)

    return {
        "items": [i.model_dump(mode="json") for i in deduped],
        "per_source_stats": per_source_stats,
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "build_seconds": round(time.time() - t0, 2),
            "sources_polled": sources_polled,
            "new_items": len(deduped),
            "incidents_filed": incidents_filed,
            "last_run_timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }


def emit_all(result: dict) -> None:
    """Emit meta.json + health.json (scraper run telemetry only)."""
    # meta.json
    meta = dict(result["meta"])
    meta["per_source"] = result["per_source_stats"]
    open_dir = REPO / "tool" / "health" / "open"
    open_dir.mkdir(parents=True, exist_ok=True)
    open_incs = [p.name for p in open_dir.glob("*.md")]
    meta["open_incidents"] = open_incs

    suspicious_silence_count = 0
    for name in open_incs:
        try:
            txt = (open_dir / name).read_text(errors="replace")
            if "failure_class: suspicious_silence" in txt:
                suspicious_silence_count += 1
        except Exception:
            pass
    meta["suspicious_silence_count"] = suspicious_silence_count
    meta["status"] = (
        "red" if any("severity: crit" in (open_dir / p).read_text(encoding="utf-8") for p in open_incs)
        else ("amber" if open_incs else "green")
    )
    emit.write_json("meta", meta)

    # health.json
    health_summary: dict[str, list] = {}
    for name in open_incs:
        try:
            txt = (open_dir / name).read_text(errors="replace")
            src_line = next((l for l in txt.splitlines() if l.startswith("source_id:")), "")
            src = src_line.split(":", 1)[1].strip() if src_line else "unknown"
            cls_line = next((l for l in txt.splitlines() if l.startswith("failure_class:")), "")
            cls = cls_line.split(":", 1)[1].strip() if cls_line else "unknown"
            health_summary.setdefault(src, []).append({
                "file": name,
                "failure_class": cls,
            })
        except Exception:
            pass
    emit.write_json("health", {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "open_incident_count": len(open_incs),
        "suspicious_silence_count": suspicious_silence_count,
        "by_source": health_summary,
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh-fixtures", action="store_true",
                    help="Live-GET each source once, write snapshot-ok.*")
    ap.add_argument("--only", help="Run only this source id")
    args = ap.parse_args()

    sources = load_sources()
    if args.only:
        sources = [s for s in sources if s["id"] == args.only]

    if args.refresh_fixtures:
        refresh_fixtures(sources)
        return

    result = run(sources)
    emit_all(result)

    print(json.dumps(result["meta"], indent=2, default=str))


if __name__ == "__main__":
    main()
