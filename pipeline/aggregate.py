"""Derive impact-area aggregations from the slot-1/slot-2 models.

Pure function. No I/O, no scrape. Re-uses the already-loaded peer-fund
and DFI-card models so the source-of-truth stays the YAML registries.

One row per `sector_tag` observed across:
  - peer_funds.yml -> peer_funds[*].sector_tags  (precedents)
  - dfi_ingo_commitments.yml -> commitments[*].sector_tags via fund join

Each row carries:
  - peer_fund_count / dfi_count           (all-time totals)
  - dfi_count_active_3y                   (DFIs with last commit >= today - 3y)
  - peer_funds[]  / dfis[]                (lightweight rows for drill-down)

Honesty: peer-fund "precedents" filter mirrors slot 1 emit (parent_ingo
not null AND vehicle_type != "programmatic_not_fund"). Non-INGO
comparables are referents in the registry; they are not precedents an
INGO GP would cite when raising.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from typing import Any


SECTOR_LABELS: dict[str, str] = {
    "fi": "Financial Inclusion",
    "agri": "Agriculture",
    "energy": "Energy access",
    "climate": "Climate",
    "gender": "Gender lens",
    "gender_lens": "Gender lens",
    "health": "Health",
    "edu": "Education",
    "smb": "SMB / SME",
    "sgb": "Small & Growing Business",
    "wash": "Water & Sanitation",
    "water": "Water & Sanitation",
    "housing": "Housing",
    "humanitarian": "Humanitarian",
    "forestry": "Forestry",
    "generalist": "Generalist",
    "infra": "Infrastructure",
    "resilience": "Resilience",
    "fragile_states": "Fragile States",
    "nature": "Nature",
    "conservation": "Conservation",
    "fintech": "Fintech",
    "msme": "MSME",
    "manufacturing": "Manufacturing",
    "jobs": "Jobs",
    "tech_for_good": "Tech for Good",
}

ACTIVE_WINDOW_YEARS = 3


def _label(slug: str) -> str:
    return SECTOR_LABELS.get(slug, slug.replace("_", " ").title())


def _is_precedent(fund) -> bool:
    """Mirror slot-1 emit filter: real INGO-sponsored fund vehicles only."""
    if not getattr(fund, "parent_ingo", None):
        return False
    if getattr(fund, "vehicle_type", None) == "programmatic_not_fund":
        return False
    return True


def build_impact_areas(
    peer_models: list,
    dfi_cards: list,
    raw_commits: list[dict[str, Any]],
    today: date,
) -> list[dict[str, Any]]:
    """Return sorted list of impact-area rows for site/src/_data/impact_areas.json.

    Rows are sorted by (peer_fund_count + dfi_count) descending — the
    "what's funded" axis the homepage chart sorts on.
    """
    cutoff = today - timedelta(days=ACTIVE_WINDOW_YEARS * 365 + 1)

    precedents = [f for f in peer_models if _is_precedent(f)]
    fund_by_slug = {f.slug: f for f in peer_models}

    # ---- collect sectors mentioned anywhere ---------------------------------
    sectors: set[str] = set()
    for f in precedents:
        for s in f.sector_tags or []:
            sectors.add(s)
    for c in raw_commits:
        # Only count commits that resolve to an INGO-sponsored fund precedent.
        fs = c.get("fund_slug")
        fund = fund_by_slug.get(fs) if fs else None
        if not fund or not _is_precedent(fund):
            continue
        for s in (c.get("sector_tags") or fund.sector_tags or []):
            sectors.add(s)

    # ---- index DFIs by slug for fast lookup ---------------------------------
    dfi_by_slug = {d.slug: d for d in dfi_cards}

    rows: list[dict[str, Any]] = []
    for sector in sorted(sectors):
        # peer funds touching this sector
        funds_in = [f for f in precedents if sector in (f.sector_tags or [])]

        # commits touching this sector (sector via commit row, fall back to fund)
        commits_in: list[dict[str, Any]] = []
        for c in raw_commits:
            fs = c.get("fund_slug")
            fund = fund_by_slug.get(fs) if fs else None
            if not fund or not _is_precedent(fund):
                continue
            tags = c.get("sector_tags") or fund.sector_tags or []
            if sector in tags:
                commits_in.append(c)

        # collapse commits to per-DFI counts within this sector
        dfi_counter: Counter[str] = Counter()
        dfi_last_date: dict[str, date] = {}
        for c in commits_in:
            slug = c.get("dfi_slug")
            if not slug:
                continue
            dfi_counter[slug] += 1
            cd = _parse_date(c.get("commit_date"))
            if cd and (slug not in dfi_last_date or cd > dfi_last_date[slug]):
                dfi_last_date[slug] = cd

        dfi_rows: list[dict[str, Any]] = []
        for slug, n in dfi_counter.most_common():
            card = dfi_by_slug.get(slug)
            last = dfi_last_date.get(slug)
            is_active = last is not None and last >= cutoff
            dfi_rows.append({
                "slug": slug,
                "name": card.name if card else slug,
                "country": card.country if card else None,
                "policy_remit": card.policy_remit if card else None,
                "commit_count": n,
                "last_commit_date": last.isoformat() if last else None,
                "is_active_3y": is_active,
            })

        fund_rows: list[dict[str, Any]] = []
        for f in sorted(
            funds_in,
            key=lambda x: (x.first_close_date or date(1900, 1, 1), x.vintage or 0),
            reverse=True,
        ):
            fund_rows.append({
                "slug": f.slug,
                "name": f.name,
                "parent_ingo": f.parent_ingo,
                "parent_ingo_country": f.parent_ingo_country,
                "vintage": f.vintage,
                "first_close_date": f.first_close_date.isoformat() if f.first_close_date else None,
                "size_usd_m": f.size_usd_m,
                "status": f.status,
                "public_source_url": getattr(f, "public_source_url", None),
            })

        peer_count = len(fund_rows)
        dfi_count = len(dfi_rows)
        dfi_count_active = sum(1 for d in dfi_rows if d["is_active_3y"])

        if peer_count == 0 and dfi_count == 0:
            # Sector tag exists in the registry but has zero precedents and
            # zero commits after filtering — drop rather than render an empty bar.
            continue

        rows.append({
            "slug": sector,
            "label": _label(sector),
            "peer_fund_count": peer_count,
            "dfi_count": dfi_count,
            "dfi_count_active_3y": dfi_count_active,
            "total_count": peer_count + dfi_count,
            "total_count_active_3y": peer_count + dfi_count_active,
            "peer_funds": fund_rows,
            "dfis": dfi_rows,
        })

    rows.sort(key=lambda r: (r["total_count"], r["peer_fund_count"]), reverse=True)
    return rows


def _parse_date(v: Any) -> date | None:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None
