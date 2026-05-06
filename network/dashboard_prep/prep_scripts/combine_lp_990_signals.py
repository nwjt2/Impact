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

from network.utils.csv_io import read_rows, write_rows  # noqa: E402

REGISTRY = REPO_ROOT / "network" / "lp_990_signals" / "ein_registry.csv"
INDIVIDUAL_DIR = REPO_ROOT / "network" / "lp_990_signals" / "individual_lp_990s"
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


def extract_row(slug: str, registry_row: dict, payload: dict) -> dict:
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
        "Source URL": PROPUBLICA_ORG_URL.format(ein=ein) if ein else "",
        "Source Date": date.today().isoformat(),
        "Scraping Method Used": SCRAPING_METHOD,
    }
    if not f:
        return base
    formtype = f.get("formtype")
    base["Form Type"] = {0: "990", 1: "990-EZ", 2: "990-PF", 5: "990-T"}.get(formtype, str(formtype))
    base["Latest Filing Year"] = f.get("tax_prd_yr") or ""
    base["Total Assets USD"] = pick(f, "totassetsend", "fairmrktvaleoy", "fairmrktvalamt")
    # 990-PF: contrpdpbks is grants paid line; 990: grntspdtoindiv / grntsincomadminexpns. Try both.
    base["Annual Grants Paid USD"] = pick(f, "contrpdpbks", "grntspdtoindiv", "grntsamt")
    base["Annual Distributions USD"] = pick(f, "qlfydistribtot", "distribamt")
    base["Total Charitable Expenses USD"] = pick(f, "totexpnsexempt", "totfuncexpns")
    base["Officer Compensation Aggregate USD"] = pick(f, "compofficers", "compnsatncurrofcr")
    return base


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", default=None, help="explicit run dir (default: latest)")
    args = parser.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else latest_run_dir()
    if not run_dir.exists():
        raise SystemExit(f"run dir does not exist: {run_dir}")

    registry = {r["LP Slug"]: r for r in read_rows(REGISTRY) if r.get("Confidence") == "confirmed"}

    rows: list[dict] = []
    missing_filing = 0
    for slug, reg_row in registry.items():
        path = run_dir / f"{slug}.json"
        if not path.exists():
            print(f"  WARN: {slug} confirmed in registry but no fetched JSON at {path.name} — skipping")
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        row = extract_row(slug, reg_row, payload)
        if not row["Latest Filing Year"]:
            missing_filing += 1
        rows.append(row)

    write_rows(OUT_CSV, OUT_HEADERS, rows)
    print(f"wrote {len(rows)} rows to {OUT_CSV.relative_to(REPO_ROOT)}")
    if missing_filing:
        print(f"  ({missing_filing} foundations had filings_with_data empty — they appear with blanks per honest-null)")


if __name__ == "__main__":
    main()
