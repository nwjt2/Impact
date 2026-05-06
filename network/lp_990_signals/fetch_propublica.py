"""Fetch ProPublica Nonprofit Explorer organization records for confirmed EINs.

Reads:  network/lp_990_signals/ein_registry.csv  (only Confidence=confirmed rows)
Writes: network/lp_990_signals/individual_lp_990s/run_<N>/<slug>.json
        (raw API response, gitignored per network/**/individual_*/run_*)
        network/lp_990_signals/lp_990_fetch_history.csv  (audit log, tracked)

Per advice doc lessons 6, 9, 13:
  - Identifying User-Agent on every fetch (network.utils.http.USER_AGENT).
  - Failures land in network/docs/discovery_skip_list.csv with a reason.
  - Use httpx (already in deps) rather than urllib for retry/backoff.

Per CLAUDE.md: this is NOT a per-fund custom scraper (lesson 11 territory).
It's a single uniform API client for one well-known free data source.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import date
from pathlib import Path

import httpx
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import read_rows, write_rows  # noqa: E402
from network.utils.http import USER_AGENT  # noqa: E402

REGISTRY = REPO_ROOT / "network" / "lp_990_signals" / "ein_registry.csv"
SISTER_TRUSTS = REPO_ROOT / "network" / "lp_990_signals" / "sister_trusts.yml"
INDIVIDUAL_DIR = REPO_ROOT / "network" / "lp_990_signals" / "individual_lp_990s"
TRUST_DIR = REPO_ROOT / "network" / "lp_990_signals" / "individual_lp_trusts"
HISTORY = REPO_ROOT / "network" / "lp_990_signals" / "lp_990_fetch_history.csv"
RUN_STATE = REPO_ROOT / "network" / "lp_990_signals" / "state.json"
SKIP_LIST = REPO_ROOT / "network" / "docs" / "discovery_skip_list.csv"

PROPUBLICA_BASE = "https://projects.propublica.org/nonprofits/api/v2"
DEFAULT_SLEEP = 1.0
TIMEOUT = httpx.Timeout(30.0)

HISTORY_HEADERS = [
    "Run Number",
    "LP Slug",
    "EIN",
    "Entity Type",
    "Status",
    "Filings With Data",
    "Latest Filing Year",
    "Source URL",
    "Source Date",
]


def load_sister_trusts() -> list[dict]:
    if not SISTER_TRUSTS.exists():
        return []
    with SISTER_TRUSTS.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("sister_trusts") or []


def next_run_number() -> int:
    if RUN_STATE.exists():
        st = json.loads(RUN_STATE.read_text(encoding="utf-8"))
        return int(st.get("last_run", 0)) + 1
    return 1


def write_run_state(n: int) -> None:
    RUN_STATE.write_text(json.dumps({"last_run": n, "updated_at": date.today().isoformat()}, indent=2), encoding="utf-8")


def fetch_org(ein: str) -> dict:
    url = f"{PROPUBLICA_BASE}/organizations/{ein}.json"
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT) as c:
        r = c.get(url)
        r.raise_for_status()
        return r.json()


def append_skip_list_entry(slug: str, reason: str) -> None:
    """Append-only skip list per lesson 9. Schema: Slug,Source URL,Reason,Skipped At."""
    row = {
        "Slug": slug,
        "Source URL": f"{PROPUBLICA_BASE}/organizations/<EIN>.json",
        "Reason": reason,
        "Skipped At": date.today().isoformat(),
    }
    headers = ["Slug", "Source URL", "Reason", "Skipped At"]
    SKIP_LIST.parent.mkdir(parents=True, exist_ok=True)
    file_existed = SKIP_LIST.exists()
    with SKIP_LIST.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        if not file_existed:
            w.writeheader()
        w.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="cap to first N (0 = all)")
    parser.add_argument("--only-slug", default=None, help="fetch only this single LP slug (debug)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    rows = read_rows(REGISTRY)
    confirmed = [r for r in rows if r.get("Confidence") == "confirmed" and r.get("EIN")]
    if args.only_slug:
        confirmed = [r for r in confirmed if r["LP Slug"] == args.only_slug]
    if args.limit:
        confirmed = confirmed[: args.limit]

    if not confirmed:
        print("no confirmed EINs to fetch")
        return

    run_n = next_run_number()
    out_dir = INDIVIDUAL_DIR / f"run_{run_n}"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"run {run_n}: fetching {len(confirmed)} orgs from ProPublica…")
    if args.dry_run:
        for r in confirmed[:5]:
            print(f"  would fetch {r['LP Slug']:40s} EIN={r['EIN']}")
        print(f"  …+{len(confirmed)-5} more" if len(confirmed) > 5 else "")
        return

    today = date.today().isoformat()
    history_rows: list[dict] = []
    ok = 0
    failed = 0

    def fetch_one(slug: str, ein: str, target_dir: Path, entity_type: str, idx_label: str) -> None:
        nonlocal ok, failed
        url = f"{PROPUBLICA_BASE}/organizations/{ein}.json"
        try:
            payload = fetch_org(ein)
            (target_dir / f"{slug}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
            fwd = payload.get("filings_with_data", []) or []
            latest_yr = fwd[0].get("tax_prd_yr") if fwd else None
            history_rows.append({
                "Run Number": run_n,
                "LP Slug": slug,
                "EIN": ein,
                "Entity Type": entity_type,
                "Status": "ok",
                "Filings With Data": len(fwd),
                "Latest Filing Year": latest_yr or "",
                "Source URL": url,
                "Source Date": today,
            })
            ok += 1
            print(f"  {idx_label} ok    {slug:40s} [{entity_type}] EIN={ein:>10s}  {len(fwd)} filings, latest {latest_yr}")
        except httpx.HTTPStatusError as e:
            failed += 1
            reason = f"propublica_http_{e.response.status_code}"
            history_rows.append({
                "Run Number": run_n, "LP Slug": slug, "EIN": ein, "Entity Type": entity_type,
                "Status": reason, "Filings With Data": "", "Latest Filing Year": "",
                "Source URL": url, "Source Date": today,
            })
            append_skip_list_entry(slug, reason)
            print(f"  {idx_label} FAIL  {slug:40s} [{entity_type}] EIN={ein:>10s}  {reason}")
        except Exception as exc:
            failed += 1
            history_rows.append({
                "Run Number": run_n, "LP Slug": slug, "EIN": ein, "Entity Type": entity_type,
                "Status": "error", "Filings With Data": "", "Latest Filing Year": "",
                "Source URL": url, "Source Date": today,
            })
            print(f"  {idx_label} ERROR {slug:40s} [{entity_type}] EIN={ein:>10s}  {exc!r}")
        time.sleep(DEFAULT_SLEEP)

    for i, r in enumerate(confirmed, 1):
        fetch_one(r["LP Slug"], r["EIN"], out_dir, "foundation", f"[{i:2d}/{len(confirmed)}]")

    # Sister-trust fetch pass — corpus-holding entities for foundations whose
    # operating shell doesn't reflect the real AUM. Cached per parent slug.
    trusts = load_sister_trusts()
    parent_slugs_in_run = {r["LP Slug"] for r in confirmed}
    relevant = [t for t in trusts if t.get("parent_slug") in parent_slugs_in_run and t.get("trust_ein")]
    if relevant:
        trust_out_dir = TRUST_DIR / f"run_{run_n}"
        trust_out_dir.mkdir(parents=True, exist_ok=True)
        print(f"\nfetching {len(relevant)} sister-trust filings…")
        for j, t in enumerate(relevant, 1):
            fetch_one(t["parent_slug"], str(t["trust_ein"]), trust_out_dir, "trust", f"[T {j}/{len(relevant)}]")

    # Append to history (this file IS tracked, supports audit across runs).
    existing = read_rows(HISTORY)
    existing.extend(history_rows)
    write_rows(HISTORY, HISTORY_HEADERS, existing)

    write_run_state(run_n)
    print(f"\nrun {run_n}: ok={ok}, failed={failed}, written to {out_dir.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
