"""HTTP wrapper used by all scrapers. UA, retry, backoff, URL log, polite sleep.

Uses httpx (already in repo deps via pyproject.toml).
Per advice doc lessons 6, 13, 15, 16: identifying UA, retry with backoff,
log every URL fetched, sleep between requests, requests-first.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

import httpx

USER_AGENT = (
    "ingo-first-close-bot/0.1 "
    "(+https://github.com/Sally-Mason/Impact; "
    "research; contact via repo issues)"
)
TIMEOUT = httpx.Timeout(30.0)
DEFAULT_SLEEP = 1.5

_log = logging.getLogger("network.http")


def configure_logging(run_dir: Path | str) -> None:
    """Wire URL-fetch logging to per-run logfile. Call once per pipeline run."""
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(run_dir / "http.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _log.addHandler(fh)
    _log.setLevel(logging.INFO)


def get_html(url: str, *, sleep: float = DEFAULT_SLEEP, retries: int = 3) -> str:
    """GET a URL with retries and exponential backoff. Returns response text."""
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            r = httpx.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=TIMEOUT,
                follow_redirects=True,
            )
            _log.info("GET %s -> %s", url, r.status_code)
            r.raise_for_status()
            time.sleep(sleep)
            return r.text
        except httpx.HTTPError as e:
            last_exc = e
            wait = 2**attempt
            _log.warning(
                "GET %s failed (attempt %d/%d): %s; retrying in %ds",
                url,
                attempt + 1,
                retries,
                e,
                wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"GET {url} failed after {retries} attempts: {last_exc}")
