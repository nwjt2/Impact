"""Combine ProPublica per-LP JSONs into a single sizing CSV for the match page.

Reads:  network/lp_990_signals/individual_lp_990s/run_<N>/*.json  (latest run by state.json)
Writes: network/dashboard_prep/lp_990_signals.csv

For each foundation, surfaces:
  - Latest filing year (staleness indicator)
  - Total assets at end of year (AUM proxy)
  - Grants paid + qualifying distributions (annual giving size)
  - Total charitable expenses
  - Aggregate officer compensation (no names — that lives in the 990 PDF, not the API)
  - ProPublica org-page URL (for the card link)

This is metadata enrichment, not a per-fund scraper. Per advice doc lesson 1
every row carries `Scraping Method Used`; per honest-null discipline we leave
fields blank where ProPublica had no data.

Form-type handling: all current targets are 990-PF (formtype=2) but the script
also tries 990 field names so a future public-charity LP wouldn't silently miss.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

import yaml  # noqa: E402

from network.utils.csv_io import read_rows, write_rows  # noqa: E402

REGISTRY = REPO_ROOT / "network" / "lp_990_signals" / "ein_registry.csv"
SISTER_TRUSTS = REPO_ROOT / "network" / "lp_990_signals" / "sister_trusts.yml"
INDIVIDUAL_DIR = REPO_ROOT / "network" / "lp_990_signals" / "individual_lp_990s"
TRUST_DIR = REPO_ROOT / "network" / "lp_990_signals" / "individual_lp_trusts"
RUN_STATE = REPO_ROOT / "network" / "lp_990_signals" / "state.json"
OUT_CSV = REPO_ROOT / "network" / "dashboard_prep" / "lp_990_signals.csv"

PROPUBLICA_ORG_URL = "https://projects.propublica.org/nonprofits/organizations/{ein}"

OUT_HEADERS = [
    "LP Slug",
    "EIN",
    "Resolved Name",
    "Form Type",
    "Latest Filing Year",
    "Total Assets USD",
    "Annual Grants Paid USD",
    "Annual Distributions USD",
    "Total Charitable Expenses USD",
    "Officer Compensation Aggregate USD",
    "Sister Trust",
    "Sister Trust EIN",
    "Sister Trust Total Assets USD",
    "Sister Trust Source URL",
    "Source URL",
    "Source Date",
    "Scraping Method Used",
]

SCRAPING_METHOD = "lp_990_signals.combine_lp_990_signals"


def latest_run_dir() -> Path:
    if RUN_STATE.exists():
        st = json.loads(RUN_STATE.read_text(encoding="utf-8"))
        n = int(st.get("last_run", 0))
        return INDIVIDUAL_DIR / f"run_{n}"
    # fall back to highest-numbered dir
    candidates = sorted(INDIVIDUAL_DIR.glob("run_*"), key=lambda p: int(p.name.split("_")[1]))
    if not candidates:
        raise SystemExit(f"no run dir under {INDIVIDUAL_DIR}")
    return candidates[-1]


def first_filing(payload: dict) -> dict | None:
    fwd = payload.get("filings_with_data") or []
    return fwd[0] if fwd else None


def pick(filing: dict, *keys: str) -> int | str:
    """Return the first non-zero numeric field in keys, else ''.

    Zeros are treated as missing because ProPublica often returns 0 for
    fields the filing didn't populate (e.g. qlfydistribtot stays 0 while
    distribamt carries the real number). 0 is never a meaningful sizing
    value for an active foundation.
    """
    for k in keys:
        v = filing.get(k)
        if v is not None and v != "" and v != 0:
            return v
    return ""


def extract_row(slug: str, registry_row: dict, payload: dict, trust: dict | None = None,
                trust_payload: dict | None = None) -> dict:
    """Extract sizing row for one foundation. If a sister-trust mapping and its
    payload are passed, apply the configured consolidation method.
    """
    f = first_filing(payload)
    ein = registry_row.get("EIN") or ""
    base = {
        "LP Slug": slug,
        "EIN": ein,
        "Resolved Name": registry_row.get("Resolved Name") or "",
        "Form Type": "",
        "Latest Filing Year": "",
        "Total Assets USD": "",
        "Annual Grants Paid USD": "",
        "Annual Distributions USD": "",
        "Total Charitable Expenses USD": "",
        "Officer Compensation Aggregate USD": "",
        "Sister Trust": "",
        "Sister Trust EIN": "",
        "Sister Trust Total Assets USD": "",
        "Sister Trust Source URL": "",
        "Source URL": PROPUBLICA_ORG_URL.format(ein=ein) if ein else "",
        "Source Date": date.today().isoformat(),
        "Scraping Method Used": SCRAPING_METHOD,
    }
    if not f:
        return base
    formtype = f.get("formtype")
    base["Form Type"] = {0: "990", 1: "990-EZ", 2: "990-PF", 5: "990-T"}.get(formtype, str(formtype))
    base["Latest Filing Year"] = f.get("tax_prd_yr") or ""
    parent_assets = pick(f, "totassetsend", "fairmrktvaleoy", "fairmrktvalamt")
    base["Total Assets USD"] = parent_assets
    # 990-PF: contrpdpbks is grants paid line; 990: grntspdtoindiv / grntsincomadminexpns. Try both.
    base["Annual Grants Paid USD"] = pick(f, "contrpdpbks", "grntspdtoindiv", "grntsamt")
    base["Annual Distributions USD"] = pick(f, "qlfydistribtot", "distribamt")
    base["Total Charitable Expenses USD"] = pick(f, "totexpnsexempt", "totfuncexpns")
    base["Officer Compensation Aggregate USD"] = pick(f, "compofficers", "compnsatncurrofcr")

    # Sister-trust consolidation. Two methods:
    #   add  — sum trust.totassetsend into parent.totassetsend (Kellogg case)
    #   note — leave parent.totassetsend unchanged; record the relationship
    #          (BMGF case where Foundation 990 already reflects the corpus)
    if trust:
        trust_ein = str(trust.get("trust_ein") or "")
        base["Sister Trust"] = trust.get("trust_name") or ""
        base["Sister Trust EIN"] = trust_ein
        base["Sister Trust Source URL"] = PROPUBLICA_ORG_URL.format(ein=trust_ein) if trust_ein else ""
        if trust_payload:
            tf = first_filing(trust_payload)
            if tf:
                trust_assets = pick(tf, "totassetsend", "fairmrktvaleoy", "fairmrktvalamt")
                base["Sister Trust Total Assets USD"] = trust_assets
                method = trust.get("consolidate") or "note"
                if method == "add" and isinstance(trust_assets, (int, float)) and isinstance(parent_assets, (int, float)):
                    base["Total Assets USD"] = parent_assets + trust_assets
                elif method == "add" and isinstance(trust_assets, (int, float)):
                    # parent had no assets value; just take trust's
                    base["Total Assets USD"] = trust_assets
                # method == "note" → leave parent_assets as-is
    return base


def load_sister_trust_map() -> dict[str, dict]:
    """parent_slug → trust mapping dict."""
    if not SISTER_TRUSTS.exists():
        return {}
    with SISTER_TRUSTS.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {t["parent_slug"]: t for t in (data.get("sister_trusts") or []) if t.get("parent_slug")}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", default=None, help="explicit run dir (default: latest)")
    args = parser.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else latest_run_dir()
    if not run_dir.exists():
        raise SystemExit(f"run dir does not exist: {run_dir}")
    # Sister-trust JSONs cached at TRUST_DIR/run_<N>/<parent_slug>.json under
    # the same run number as the parent fetch.
    trust_dir = TRUST_DIR / run_dir.name

    registry = {r["LP Slug"]: r for r in read_rows(REGISTRY) if r.get("Confidence") == "confirmed"}
    trusts = load_sister_trust_map()

    rows: list[dict] = []
    missing_filing = 0
    consolidated = 0
    for slug, reg_row in registry.items():
        path = run_dir / f"{slug}.json"
        if not path.exists():
            print(f"  WARN: {slug} confirmed in registry but no fetched JSON at {path.name} — skipping")
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        trust = trusts.get(slug)
        trust_payload = None
        if trust:
            tpath = trust_dir / f"{slug}.json"
            if tpath.exists():
                trust_payload = json.loads(tpath.read_text(encoding="utf-8"))
                consolidated += 1
            else:
                print(f"  WARN: {slug} has sister-trust mapping but no trust JSON at {tpath.name} — using parent only")
        row = extract_row(slug, reg_row, payload, trust=trust, trust_payload=trust_payload)
        if not row["Latest Filing Year"]:
            missing_filing += 1
        rows.append(row)

    write_rows(OUT_CSV, OUT_HEADERS, rows)
    print(f"wrote {len(rows)} rows to {OUT_CSV.relative_to(REPO_ROOT)}")
    if consolidated:
        print(f"  ({consolidated} row(s) had sister-trust data merged in)")
    if missing_filing:
        print(f"  ({missing_filing} foundations had filings_with_data empty — they appear with blanks per honest-null)")


if __name__ == "__main__":
    main()
