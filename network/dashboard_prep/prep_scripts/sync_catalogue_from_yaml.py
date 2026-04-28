"""Seed network/catalogue/ CSVs from existing YAML files.

Reads:
  content/peer_funds.yml          -> impact_funds.csv + ingos.csv
  pipeline/entities.yml           -> investors.csv (target_lps section)

Writes:
  network/catalogue/impact_funds.csv
  network/catalogue/ingos.csv
  network/catalogue/investors.csv
  network/docs/discovery_skip_list.csv (appended for excluded vehicles)

Seeding rules (decided 2026-04-28):
  - vehicle_type == 'programmatic_not_fund' -> EXCLUDE, skip-list reason
    "programmatic, not a fund vehicle"
  - status == 'defunct' -> EXCLUDE, skip-list reason "defunct"
  - status == 'wound_down' -> SEED with pipeline_status='wound_down'
  - First batch (acumen-h2r-amplify, mercy-corps-ventures, root-capital)
    -> pipeline_status='active'
  - All others -> pipeline_status='pending_onboard'

  - fund_type defaults to 'unclassified'. Set to 'fof' or 'direct' only when
    YAML notes contain unambiguous language. Most stay unclassified until
    portfolio is scraped (advice doc lesson 39: don't model perfectly in v1).
"""
from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
PEER_FUNDS_YML = REPO_ROOT / "content" / "peer_funds.yml"
ENTITIES_YML = REPO_ROOT / "pipeline" / "entities.yml"
CATALOGUE_DIR = REPO_ROOT / "network" / "catalogue"
SKIP_LIST_CSV = REPO_ROOT / "network" / "docs" / "discovery_skip_list.csv"

sys.path.insert(0, str(REPO_ROOT))
from network.utils.csv_io import write_rows  # noqa: E402
from network.utils.slugify import slugify  # noqa: E402

ACTIVE_FUND_SLUGS = {
    "acumen-h2r-amplify",
    "mercy-corps-ventures",
    "acumen-kawisafi",
    # AgDevCo added 2026-04-28 as a clean-disclosure fallback after Acumen
    # was found to be Cloudflare-walled. Non-INGO comparable.
    "agdevco-smallholder-development-unit",
    # Kiva Refugee Investment Fund added 2026-04-28. The portfolio scraper
    # hits Kiva's umbrella /partners page (225 MFIs). KRIF is one of several
    # Kiva vehicles; per-fund attribution is a future enrichment.
    "kiva-refugee-investment-fund",
    # ACDI/VOCA's AV Ventures family — three funds with per-fund attribution
    # on av-ventures.com/our_funds/. Single bespoke scraper, three CSVs.
    "av-ventures-ink-kenya",
    "av-ventures-ghana",
    "av-frontiers-caif",
}

IMPACT_FUNDS_HEADERS = [
    "Fund Name",
    "Fund Slug",
    "INGO Slug",
    "Website",
    "Founded Year",
    "AUM (USD M)",
    "Thesis Tags",
    "Status",
    "Portfolio Page URL",
    "LP Page URL",
    "Fund Type",
    "Pipeline Status",
    "Notes",
]

INGOS_HEADERS = [
    "INGO Name",
    "INGO Slug",
    "HQ Country",
    "Website",
    "Status",
    "Notes",
]

INVESTORS_HEADERS = [
    "Investor Name",
    "Investor Slug",
    "Investor Type",
    "Impact Focus",
    "Website",
    "HQ Country",
    "AUM Bucket",
    "Status",
    "Notes",
]

LP_TYPE_TO_INVESTOR_TYPE = {
    "dfi": "dfi",
    "foundation": "foundation",
    "family_office": "family-office",
    "corp_impact": "other",
}

_FOF_HINTS_RE = re.compile(
    r"\b(fund of funds|fund-of-funds|invests in.*funds|underlying funds)\b",
    flags=re.IGNORECASE,
)
_DIRECT_HINTS_RE = re.compile(
    r"\b(\d+\s+portfolio companies|direct equity into|invests directly in (companies|operators|enterprises))\b",
    flags=re.IGNORECASE,
)


def classify_fund_type(notes: str | None) -> str:
    """Return 'fof' / 'direct' / 'unclassified' from free-text notes only.

    Conservative: only returns a non-default classification when notes contain
    unambiguous language. Everything else stays unclassified until scraped.
    """
    if not notes:
        return "unclassified"
    if _FOF_HINTS_RE.search(notes):
        return "fof"
    if _DIRECT_HINTS_RE.search(notes):
        return "direct"
    return "unclassified"


def derive_pipeline_status(
    fund: dict, blocked_slugs: set[str]
) -> tuple[str | None, str | None]:
    """Return (pipeline_status, skip_reason). pipeline_status=None means EXCLUDE.

    blocked_slugs comes from the skip list (entries that aren't outright
    excluded but shouldn't be scraped this run — e.g., Cloudflare-walled).
    """
    vtype = (fund.get("vehicle_type") or "").lower()
    status = (fund.get("status") or "").lower()
    slug = fund["slug"]

    if vtype == "programmatic_not_fund":
        return None, "programmatic, not a fund vehicle"
    if status == "defunct":
        return None, "defunct"
    if status == "wound_down":
        return "wound_down", None
    if slug in blocked_slugs:
        return "blocked", None
    if slug in ACTIVE_FUND_SLUGS:
        return "active", None
    return "pending_onboard", None


# Skip-list reasons that block scraping but aren't fatal exclusions.
# Reasons in this set keep the fund in impact_funds.csv with pipeline_status=blocked.
_BLOCKING_REASON_FRAGMENTS = (
    "cloudflare",
    "needs playwright",
)


def load_blocked_slugs() -> set[str]:
    """Read the skip list and return slugs whose Reason is a non-fatal block."""
    if not SKIP_LIST_CSV.exists():
        return set()
    import csv

    blocked = set()
    with SKIP_LIST_CSV.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("Entity Type") != "impact_fund":
                continue
            reason = (row.get("Reason") or "").lower()
            if any(frag in reason for frag in _BLOCKING_REASON_FRAGMENTS):
                blocked.add(row["Entity Slug"])
    return blocked


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_impact_funds_and_skip_list(
    peer_funds_doc: dict,
) -> tuple[list[dict], list[dict]]:
    rows = []
    skip = []
    today = date.today().isoformat()
    blocked = load_blocked_slugs()
    for fund in peer_funds_doc.get("peer_funds", []):
        pipeline_status, skip_reason = derive_pipeline_status(fund, blocked)
        if pipeline_status is None:
            skip.append(
                {
                    "Entity Slug": fund["slug"],
                    "Entity Type": "impact_fund",
                    "Reason": skip_reason,
                    "Reconsider After": "",
                    "Date Added": today,
                }
            )
            continue

        ingo = fund.get("parent_ingo")
        ingo_slug = slugify(ingo) if ingo else ""

        sector_tags = fund.get("sector_tags") or []
        thesis_tags = "|".join(sector_tags + (fund.get("geo_tags") or []))

        rows.append(
            {
                "Fund Name": fund.get("name") or "",
                "Fund Slug": fund["slug"],
                "INGO Slug": ingo_slug,
                "Website": fund.get("public_source_url") or "",
                "Founded Year": fund.get("vintage") or "",
                "AUM (USD M)": _format_aum(fund.get("size_usd_m")),
                "Thesis Tags": thesis_tags,
                "Status": fund.get("status") or "unknown",
                "Portfolio Page URL": "",  # to be filled by /scrape-add-fund
                "LP Page URL": "",  # to be filled by /scrape-add-fund
                "Fund Type": classify_fund_type(fund.get("notes")),
                "Pipeline Status": pipeline_status,
                "Notes": _short_notes(fund.get("notes")),
            }
        )
    return rows, skip


def build_ingos(peer_funds_doc: dict) -> list[dict]:
    seen: dict[str, dict] = {}
    for fund in peer_funds_doc.get("peer_funds", []):
        ingo = fund.get("parent_ingo")
        if not ingo:
            continue
        ingo_slug = slugify(ingo)
        if ingo_slug in seen:
            continue
        seen[ingo_slug] = {
            "INGO Name": ingo,
            "INGO Slug": ingo_slug,
            "HQ Country": fund.get("parent_ingo_country") or "",
            "Website": "",
            "Status": "active",
            "Notes": "",
        }
    return list(seen.values())


def build_investors(entities_doc: dict) -> list[dict]:
    rows = []
    for lp in entities_doc.get("target_lps", []):
        name = lp["name"]
        rows.append(
            {
                "Investor Name": name,
                "Investor Slug": slugify(name),
                "Investor Type": LP_TYPE_TO_INVESTOR_TYPE.get(
                    lp.get("lp_type"), "other"
                ),
                "Impact Focus": "unknown",
                "Website": lp.get("public_newsroom_url") or "",
                "HQ Country": "",
                "AUM Bucket": "unknown",
                "Status": "active",
                "Notes": "",
            }
        )
    return rows


def _format_aum(size_usd_m) -> str:
    if size_usd_m is None or size_usd_m == "TBD":
        return ""
    try:
        return str(int(size_usd_m))
    except (TypeError, ValueError):
        return str(size_usd_m)


def _short_notes(notes: str | None) -> str:
    if not notes:
        return ""
    text = " ".join(notes.split())
    if len(text) > 280:
        return text[:277] + "..."
    return text


def append_skip_list(skip_rows: list[dict]) -> int:
    """Append skip-list rows, preserving existing entries. Returns count of NEW rows."""
    existing = []
    if SKIP_LIST_CSV.exists():
        import csv

        with SKIP_LIST_CSV.open("r", encoding="utf-8", newline="") as f:
            existing = list(csv.DictReader(f))
    seen = {(r["Entity Slug"], r["Entity Type"]) for r in existing}
    new = [r for r in skip_rows if (r["Entity Slug"], r["Entity Type"]) not in seen]
    write_rows(
        SKIP_LIST_CSV,
        ["Entity Slug", "Entity Type", "Reason", "Reconsider After", "Date Added"],
        existing + new,
    )
    return len(new)


def main() -> None:
    print(f"Reading {PEER_FUNDS_YML}")
    peer_funds_doc = load_yaml(PEER_FUNDS_YML)
    print(f"Reading {ENTITIES_YML}")
    entities_doc = load_yaml(ENTITIES_YML)

    impact_funds, skip = build_impact_funds_and_skip_list(peer_funds_doc)
    ingos = build_ingos(peer_funds_doc)
    investors = build_investors(entities_doc)

    write_rows(CATALOGUE_DIR / "impact_funds.csv", IMPACT_FUNDS_HEADERS, impact_funds)
    write_rows(CATALOGUE_DIR / "ingos.csv", INGOS_HEADERS, ingos)
    write_rows(CATALOGUE_DIR / "investors.csv", INVESTORS_HEADERS, investors)

    new_skips = append_skip_list(skip) if skip else 0

    print(f"  impact_funds.csv: {len(impact_funds)} rows")
    print(f"  ingos.csv:        {len(ingos)} rows")
    print(f"  investors.csv:    {len(investors)} rows")
    print(f"  skip list:        +{new_skips} new (of {len(skip)} excluded)")

    by_status: dict[str, int] = {}
    for r in impact_funds:
        by_status[r["Pipeline Status"]] = by_status.get(r["Pipeline Status"], 0) + 1
    print(f"  pipeline_status breakdown: {by_status}")

    by_type: dict[str, int] = {}
    for r in impact_funds:
        by_type[r["Fund Type"]] = by_type.get(r["Fund Type"], 0) + 1
    print(f"  fund_type breakdown:       {by_type}")


if __name__ == "__main__":
    main()
