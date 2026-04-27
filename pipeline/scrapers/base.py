"""BaseScraper with fixture-mode swap.

INGO_FIXTURE_MODE=1  → read tool/fixtures/<source_id>/snapshot-<case>.xml
INGO_LIVE=1          → make one live GET (used by `make fixtures-refresh`)
Default              → also fixture mode (local-first; we never auto-hit the net).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tool" / "fixtures"


@dataclass
class FetchResult:
    body: bytes
    http_status: int
    url: str
    from_fixture: bool
    fixture_case: Optional[str] = None


class BaseScraper:
    """Thin fetch layer. Subclasses (rss.py) own parsing."""

    def __init__(self, source: dict):
        self.source = source
        self.source_id = source["id"]
        self.source_name = source["name"]
        self.url = source["url"]
        self.break_token = source.get("break_token", "") or ""

    def fixture_path(self, case: str) -> Path:
        """Default fixture path. Subclasses (html, api) override the extension.

        Case naming convention:
          ok                           — snapshot-ok.xml        (nominal)
          empty                        — snapshot-empty.xml     (silent zero)
          broken                       — snapshot-broken.xml    (schema drift)
          404                          — snapshot-404.txt       (http_error)
          silent-zero-over-cadence     — snapshot-silent-zero-over-cadence.xml
                                         (file may be identical to
                                         snapshot-empty but its presence
                                         triggers the cadence check)
        """
        ext = "xml"
        if case == "404":
            ext = "txt"
        return FIXTURES / self.source_id / f"snapshot-{case}.{ext}"

    def _rehearse_case(self) -> Optional[str]:
        """Return rehearsal case for this source, or None."""
        case = os.environ.get("INGO_REHEARSE_CASE", "").strip().upper()
        if not case:
            return None
        # All sources break in rehearsal; rehearsal hits every configured source
        # with the same case to verify the incident pipeline.
        return case

    def fetch(self) -> FetchResult:
        """Fetch the source body.

        In fixture mode (default), reads tool/fixtures/<id>/snapshot-*.xml.
        Rehearsal cases:
          A = snapshot-broken (schema drift — malformed xml → parse error)
          B = snapshot-404    (http_error surrogate via empty 404 body)
          C = snapshot-empty  (valid RSS, zero items — silent zero)
        """
        live = os.environ.get("INGO_LIVE") == "1"
        if live:
            return self._fetch_live()

        rcase = self._rehearse_case()
        if rcase == "A":
            return self._read_fixture("broken")
        if rcase == "B":
            return self._read_fixture("404")
        if rcase == "C":
            return self._read_fixture("empty")
        if rcase == "D":
            # The fixture lives at snapshot-silent-zero-over-cadence.<ext>
            # and is structurally identical to snapshot-empty; what makes
            # it Case D is that health.check_suspicious_silence() reads
            # sources.yml cadence and fires a `suspicious_silence`
            # FailureRecord on top of the silent-zero. If that fixture is
            # missing we fall through to snapshot-empty so rehearsal still
            # runs.
            p = self.fixture_path("silent-zero-over-cadence")
            if p.exists():
                return self._read_fixture("silent-zero-over-cadence")
            return self._read_fixture("empty")

        # honor break_token too
        if self.break_token == "schema_drift":
            return self._read_fixture("broken")
        if self.break_token == "http_error":
            return self._read_fixture("404")
        if self.break_token == "timeout":
            return self._read_fixture("empty")

        return self._read_fixture("ok")

    def _read_fixture(self, case: str) -> FetchResult:
        p = self.fixture_path(case)
        if not p.exists():
            # Missing fixture is itself a signal — treat as http_error surrogate.
            return FetchResult(
                body=b"",
                http_status=404,
                url=str(p),
                from_fixture=True,
                fixture_case=case,
            )
        body = p.read_bytes()
        status = 404 if case == "404" or not body else 200
        return FetchResult(
            body=body,
            http_status=status,
            url=str(p),
            from_fixture=True,
            fixture_case=case,
        )

    def _fetch_live(self) -> FetchResult:
        """One-shot live GET. Used by `make fixtures-refresh`."""
        import httpx
        try:
            with httpx.Client(http2=True, timeout=20.0, follow_redirects=True) as client:
                headers = {
                    "User-Agent": "Mozilla/5.0 (compatible; ingo-first-close/0.1; +https://github.com/)",
                    "Accept": "text/html,application/xhtml+xml,application/xml,application/rss+xml,application/atom+xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                }
                r = client.get(self.url, headers=headers)
                return FetchResult(
                    body=r.content,
                    http_status=r.status_code,
                    url=str(r.url),
                    from_fixture=False,
                )
        except Exception as e:
            return FetchResult(
                body=str(e).encode(),
                http_status=599,
                url=self.url,
                from_fixture=False,
            )
