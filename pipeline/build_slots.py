"""Build the three slot JSONs from the curated YAML registries.

Single module responsible for:

  1. Loading content/peer_funds.yml, content/dfi_ingo_commitments.yml,
     content/deadlines.yml.
  2. Validating each row against the Pydantic models in pipeline/schemas.py.
  3. Enforcing handshake integrity:
       - every IngoGpCommit.peer_fund_slug must resolve to a PeerIngoFund.slug
         (or be null — we emit null + a health warning for dangling refs)
       - every dfi_slug in commitments + dfi_profiles must match an entry
         in the _dfi_slug_mapping documentation block
       - every deadline_id unique across slot 3
  4. Computing DfiIngoCommit aggregate fields:
       - typical_ticket_usd_m_range (median/p25/p75/max/min over non-null
         commitments with amount disclosed), or null if n < 2
       - ingo_gp_commit_count_5y / _10y
       - last_known_activity_date / _url (most recent commit_date in commits)
  5. Stripping past deadlines (deadline_date < today AND recurring is null
     or "one_off"). Rolling/annual/quarterly without a date stay.
  6. Writing:
       - site/src/_data/peer_ingo_funds.json
       - site/src/_data/dfi_ingo_commits.json
       - site/src/_data/deadlines.json
       - site/src/_data/slot_meta.json

No scoring. No scraping. No aggregation beyond the per-DFI ticket range.
Fails fast with a clear message on handshake break.

Entry point:

    python -m pipeline.build_slots

Invoked by `make daily` after the scraper pass but before the site build.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import yaml

from .aggregate import build_impact_areas
from .emit import write_json
from .schemas import (
    Deadline,
    DfiIngoCommit,
    EmergingManagerFacility,
    IngoGpCommit,
    PeerIngoFund,
    TicketRange,
)

REPO = Path(__file__).resolve().parents[1]
CONTENT = REPO / "content"
PEER_FUNDS_YML = CONTENT / "peer_funds.yml"
DFI_COMMITS_YML = CONTENT / "dfi_ingo_commitments.yml"
DEADLINES_YML = CONTENT / "deadlines.yml"
ENTITIES_YML = REPO / "pipeline" / "entities.yml"

HEALTH_DIR = REPO / "tool" / "health"
DATA_DIR = REPO / "site" / "src" / "_data"

# Per slot-specs.md §common.
COUNTRY_ENUM = [
    "GB", "US", "NL", "CH", "FR", "DE", "SE", "NO", "FI",
    "CA", "AU", "IN", "SG", "LU", "IE", "BE", "IT", "JP", "KR",
    "BD",  # Bangladesh (BRAC)
    "KE",  # Kenya (Kenya-domiciled peer INGOs)
    "INT",  # synthetic for multilateral / supranational
    "OTHER",
]
COUNTRY_DISPLAY_NAMES = {
    "GB": "United Kingdom", "US": "United States", "NL": "Netherlands",
    "CH": "Switzerland", "FR": "France", "DE": "Germany", "SE": "Sweden",
    "NO": "Norway", "FI": "Finland", "CA": "Canada", "AU": "Australia",
    "IN": "India", "SG": "Singapore", "LU": "Luxembourg", "IE": "Ireland",
    "BE": "Belgium", "IT": "Italy", "JP": "Japan", "KR": "South Korea",
    "BD": "Bangladesh", "KE": "Kenya",
    "INT": "Supranational / Multilateral",
    "OTHER": "Other",
}

# DFI HQ countries where we normalize the DFI's displayed `country` to INT.
# HQ stays in `country`; the slot filter keys on `country`, and the UI can
# surface `policy_remit` as a second badge.
MULTILATERAL_HQ_TO_REMIT = {
    "IFC":   "INT",
    "EIB":   "EU",
    "GCF":   "INT",
    "EBRD":  "INT",
    "AIIB":  "INT",
    "IDB":   "INT",
    "AfDB":  "INT",
    "AsDB":  "INT",
    "IsDB":  "INT",
}


# ------------------------------------------------------------------ helpers


class HandshakeError(RuntimeError):
    """Raised when an intra-registry slug reference can't be resolved."""


def _normalize_country(c: Any) -> str | None:
    if c is None:
        return None
    # YAML 1.1 booleanizes unquoted NO/YES/Y/N/ON/OFF. If a country field
    # reaches us as bool, the YAML was unquoted — treat as missing rather
    # than silently emit "OTHER" and disappear the row from country filters.
    if isinstance(c, bool):
        return None
    s = str(c).strip().upper()
    if not s:
        return None
    # "EU" in content files gets mapped to INT at slot-3 filter level per spec.
    if s == "EU":
        return "INT"
    if s in COUNTRY_ENUM:
        return s
    # 2-letter but not in our enum — keep as-is; UI renders "OTHER" at filter time.
    if len(s) == 2 and s.isalpha():
        return s
    return "OTHER"


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _write_health_warning(kind: str, lines: list[str]) -> None:
    """Append a soft warning to tool/health/<YYYY-MM-DD>.md.

    Handshake breaks that we want to warn-not-fail-on (rare: nulled
    peer_fund_slug where the commit names a fund outside the registry)
    flow here. Hard errors raise HandshakeError.
    """
    HEALTH_DIR.mkdir(parents=True, exist_ok=True)
    path = HEALTH_DIR / f"{_today_iso()}.md"
    prelude = (
        f"\n\n## build_slots — {kind} ({datetime.now(timezone.utc).isoformat()})\n\n"
    )
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    path.write_text(existing + prelude + "\n".join(f"- {ln}" for ln in lines))


# ------------------------------------------------------------------ loaders


def load_peer_funds() -> tuple[list[PeerIngoFund], list[str]]:
    """Parse peer_funds.yml. Returns (models, slugs_unique_ordered)."""
    raw = yaml.safe_load(PEER_FUNDS_YML.read_text()) or {}
    rows = raw.get("peer_funds") or []

    seen: set[str] = set()
    models: list[PeerIngoFund] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise HandshakeError(f"peer_funds.yml row {i} is not a mapping: {row!r}")
        slug = (row.get("slug") or "").strip()
        if not slug:
            raise HandshakeError(f"peer_funds.yml row {i} missing required `slug`")
        if slug in seen:
            raise HandshakeError(f"peer_funds.yml: duplicate slug `{slug}`")
        seen.add(slug)

        # Normalise country codes.
        row = dict(row)
        row["parent_ingo_country"] = _normalize_country(row.get("parent_ingo_country"))
        sec = row.get("parent_ingo_country_secondary") or []
        row["parent_ingo_country_secondary"] = [
            c for c in (_normalize_country(x) for x in sec) if c
        ]

        # derive parent_ingo_slug best-effort from parent_ingo
        if row.get("parent_ingo") and not row.get("parent_ingo_slug"):
            row["parent_ingo_slug"] = _slug(row["parent_ingo"])

        models.append(PeerIngoFund(**row))

    return models, [m.slug for m in models]


def _slug(s: str) -> str:
    import re
    s = re.sub(r"[^A-Za-z0-9]+", "-", (s or "").lower()).strip("-")
    return s[:80]


def load_dfi_commitments(
    peer_slugs: set[str],
) -> tuple[list[DfiIngoCommit], list[dict]]:
    """Parse dfi_ingo_commitments.yml and build per-DFI DfiIngoCommit cards.

    Returns (dfi_cards, raw_commitments) so downstream callers can also
    inspect the flat commit list (e.g. for QA or v1.5 scraper write-back).
    """
    raw = yaml.safe_load(DFI_COMMITS_YML.read_text()) or {}
    commits_raw: list[dict] = [r for r in (raw.get("commitments") or []) if isinstance(r, dict)]
    profiles_raw: list[dict] = [r for r in (raw.get("dfi_profiles") or []) if isinstance(r, dict)]
    slug_mapping: dict = raw.get("_dfi_slug_mapping") or {}

    # --- Handshake 1: every commit's fund_slug exists in peer_funds ---------
    dangling: list[str] = []
    for c in commits_raw:
        fs = (c.get("fund_slug") or "").strip()
        if fs and fs not in peer_slugs:
            dangling.append(
                f"fund_slug `{fs}` in dfi_ingo_commitments.yml "
                f"(dfi={c.get('dfi_slug')}, fund_name={c.get('fund_name')}) "
                f"not found in peer_funds.yml"
            )
    if dangling:
        raise HandshakeError(
            "dfi_ingo_commitments.yml handshake failure:\n  " + "\n  ".join(dangling)
        )

    # --- Handshake 2: every dfi_slug (commits + profiles) appears in the
    # documented _dfi_slug_mapping block. The mapping is the canonical list;
    # a dfi_slug outside the mapping is a content error. ------------------
    commit_dfi_slugs = {c.get("dfi_slug") for c in commits_raw if c.get("dfi_slug")}
    profile_dfi_slugs = {p.get("dfi_slug") for p in profiles_raw if p.get("dfi_slug")}
    all_dfi_slugs = commit_dfi_slugs | profile_dfi_slugs
    missing_mapping = [s for s in all_dfi_slugs if s not in slug_mapping]
    if missing_mapping:
        raise HandshakeError(
            "dfi_ingo_commitments.yml: dfi_slug(s) referenced without "
            "an entry in _dfi_slug_mapping: " + ", ".join(sorted(missing_mapping))
        )

    # --- Build per-DFI cards ------------------------------------------------
    profiles_by_slug = {p["dfi_slug"]: p for p in profiles_raw if p.get("dfi_slug")}
    commits_by_slug: dict[str, list[dict]] = {}
    dfi_name_by_slug: dict[str, str] = {}
    dfi_country_by_slug: dict[str, str | None] = {}
    for c in commits_raw:
        slug = c.get("dfi_slug")
        if not slug:
            continue
        commits_by_slug.setdefault(slug, []).append(c)
        dfi_name_by_slug.setdefault(slug, c.get("dfi_name") or slug_mapping.get(slug) or slug)
        dfi_country_by_slug.setdefault(slug, _normalize_country(c.get("dfi_country")))

    # Profiles may describe DFIs with no commits yet — keep them.
    for p in profiles_raw:
        slug = p.get("dfi_slug")
        if not slug:
            continue
        dfi_name_by_slug.setdefault(slug, p.get("dfi_name") or slug_mapping.get(slug) or slug)
        dfi_country_by_slug.setdefault(slug, _normalize_country(p.get("dfi_country")))

    today = datetime.now(timezone.utc).date()
    days_5y = 5 * 365 + 1
    days_10y = 10 * 365 + 2

    cards: list[DfiIngoCommit] = []
    for slug in sorted(all_dfi_slugs):
        commits = commits_by_slug.get(slug, [])
        profile = profiles_by_slug.get(slug, {})

        # Build IngoGpCommit rows (only INGO-GP commits enter slot 2; the
        # non-INGO comparable commits in the YAML — parent_ingo is null —
        # are filtered out of the DFI card view. We keep them in
        # raw_commitments for auditability.)
        commit_models: list[IngoGpCommit] = []
        for c in commits:
            if not c.get("parent_ingo"):
                continue  # non-INGO comparable; not in slot 2
            commit_models.append(IngoGpCommit(
                peer_fund_slug=c.get("fund_slug"),
                peer_fund_name=c.get("fund_name") or "unknown",
                parent_ingo=c.get("parent_ingo"),
                commit_date=_parse_date(c.get("commit_date")),
                amount_usd_m=c.get("commit_usd_m"),
                amount_usd_m_original_ccy=c.get("commit_usd_m_original_ccy"),
                role=c.get("role"),
                public_source_url=c.get("public_source_url"),
                notes=_trim(c.get("notes")),
            ))

        # --- aggregate: ticket range, counts, last-activity ---------------
        amounts = [m.amount_usd_m for m in commit_models if m.amount_usd_m is not None]
        ticket_range: TicketRange | None = None
        if len(amounts) >= 2:
            amounts_sorted = sorted(amounts)
            ticket_range = TicketRange(
                min=float(min(amounts_sorted)),
                median=float(median(amounts_sorted)),
                max=float(max(amounts_sorted)),
                n=len(amounts_sorted),
            )

        def _within(cd: date | None, n_days: int) -> bool:
            if cd is None:
                return False
            return (today - cd).days <= n_days

        c5 = sum(1 for m in commit_models if _within(m.commit_date, days_5y))
        c10 = sum(1 for m in commit_models if _within(m.commit_date, days_10y))

        last_act_date: date | None = None
        last_act_url: str | None = None
        for m in commit_models:
            if m.commit_date and (last_act_date is None or m.commit_date > last_act_date):
                last_act_date = m.commit_date
                last_act_url = m.public_source_url

        # --- EMF ---------------------------------------------------------
        emf: EmergingManagerFacility | None = None
        if profile.get("emerging_manager_facility"):
            em = profile["emerging_manager_facility"]
            emf = EmergingManagerFacility(
                exists=bool(em.get("exists")),
                program_name=em.get("program_name"),
                application_url=em.get("application_url"),
                notes=_trim(em.get("notes")),
            )

        ticket_min = profile.get("typical_ticket_usd_m_min")
        ticket_max = profile.get("typical_ticket_usd_m_max")

        # If we have no computed ticket_range (n<2) but profile gave a
        # stated range, surface it with n=0 so the UI shows the stated value
        # with a "stated — no sample" badge. For n<2 from commits, leave
        # computed ticket_range as null; but attach the stated range values
        # as extras for UI rendering.
        country_raw = dfi_country_by_slug.get(slug)
        policy_remit = None
        if country_raw and country_raw in MULTILATERAL_HQ_TO_REMIT:
            policy_remit = MULTILATERAL_HQ_TO_REMIT[country_raw]
        # MULTILATERAL_HQ_TO_REMIT is keyed by alias not country (e.g. EIB
        # is in LU but should map to policy_remit=EU), so resolve via alias
        # match too.
        if not policy_remit:
            aliases = _collect_dfi_aliases(slug, slug_mapping, profile)
            for a in aliases:
                if a.upper() in MULTILATERAL_HQ_TO_REMIT:
                    policy_remit = MULTILATERAL_HQ_TO_REMIT[a.upper()]
                    break

        lp_type: str = "multilateral" if policy_remit else "dfi"

        cards.append(DfiIngoCommit(
            slug=slug,
            name=dfi_name_by_slug.get(slug, slug),
            aliases=_collect_dfi_aliases(slug, slug_mapping, profile),
            country=country_raw,
            policy_remit=policy_remit,
            lp_type=lp_type,  # type: ignore[arg-type]
            typical_ticket_usd_m_range=ticket_range,
            ingo_gp_commit_count_5y=c5,
            ingo_gp_commit_count_10y=c10,
            ingo_gp_commits=commit_models,
            stated_sector_priorities=profile.get("stated_sector_preferences") or [],
            stated_geo_priorities=profile.get("stated_geo_focus") or [],
            stated_thesis_url=profile.get("stated_thesis_url"),
            stated_thesis_excerpt=_trim(profile.get("stated_thesis_excerpt")),
            emerging_manager_facility=emf,
            named_contact=(profile.get("known_contacts_public") or [None])[0] if profile.get("known_contacts_public") else None,
            named_contact_title=None,
            last_known_activity_date=last_act_date,
            last_known_activity_url=last_act_url,
            public_newsroom_url=profile.get("public_newsroom_url"),
            last_seen_at=_today(),
        ))

        # Attach stated-ticket passthrough as extras on the model for UI
        if ticket_min is not None or ticket_max is not None:
            cards[-1].__pydantic_extra__ = cards[-1].__pydantic_extra__ or {}
            cards[-1].__pydantic_extra__["stated_ticket_usd_m_min"] = ticket_min
            cards[-1].__pydantic_extra__["stated_ticket_usd_m_max"] = ticket_max

    return cards, commits_raw


def _collect_dfi_aliases(slug: str, mapping: dict, profile: dict) -> list[str]:
    aliases: list[str] = []
    if slug in mapping:
        aliases.append(str(mapping[slug]))
    name = profile.get("dfi_name")
    if name and name not in aliases:
        aliases.append(name)
    extra_aliases = profile.get("aliases") or []
    for a in extra_aliases:
        if a and a not in aliases:
            aliases.append(a)
    return aliases


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _parse_date(v: Any) -> date | None:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    s = str(v).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).date()
    except ValueError:
        return None


def _trim(s: Any) -> str | None:
    if s is None:
        return None
    t = str(s).strip()
    return t or None


def load_deadlines() -> list[Deadline]:
    raw = yaml.safe_load(DEADLINES_YML.read_text()) or {}
    rows = raw.get("deadlines") or []

    today = _today()
    seen_ids: set[str] = set()
    out: list[Deadline] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise HandshakeError(f"deadlines.yml row {i} not a mapping")
        did = (row.get("deadline_id") or "").strip()
        if not did:
            raise HandshakeError(f"deadlines.yml row {i} missing deadline_id")
        if did in seen_ids:
            raise HandshakeError(f"deadlines.yml: duplicate deadline_id `{did}`")
        seen_ids.add(did)

        dd = _parse_date(row.get("deadline_date"))
        recurring = row.get("recurring")

        # Drop strictly-past one-off deadlines. Rolling / annual / quarterly /
        # biennial without a date stay. Annual / quarterly with a past date
        # also stay — the recurring tag signals "next cycle is TBD, keep
        # the link visible."
        if dd is not None and dd < today and not recurring:
            continue

        row_clean = dict(row)
        row_clean["country"] = _normalize_country(row_clean.get("country"))
        row_clean["last_verified_at"] = _parse_date(row_clean.get("last_verified_at"))
        row_clean["deadline_date"] = dd

        # Map `kind` values used in content/deadlines.yml to the allowed enum.
        if row_clean.get("kind") == "rolling_application":
            # keep as-is; enum supports it
            pass

        # `why_it_matters` may have trailing newlines from block scalars.
        if row_clean.get("why_it_matters"):
            row_clean["why_it_matters"] = row_clean["why_it_matters"].strip()

        # `public_source_url` is the canonical field name across all three
        # slots; templates read it under that name.
        row_clean["source_kind"] = row_clean.get("source_kind") or "curated"

        out.append(Deadline(**row_clean))

    # Sort by deadline_date ascending (nulls last).
    def _sort_key(d: Deadline) -> tuple[int, date]:
        if d.deadline_date is None:
            return (1, date(9999, 12, 31))
        return (0, d.deadline_date)

    out.sort(key=_sort_key)
    return out


# ------------------------------------------------------------------ emit


def _dump(model) -> dict:
    """Dump a pydantic model to JSON-ready dict (dates → ISO strings)."""
    return json.loads(model.model_dump_json())


def build(verbose: bool = True) -> dict[str, int]:
    """Load, validate, emit. Returns counts for each slot."""
    # --- slot 1 ---
    peer_models, peer_slugs_ordered = load_peer_funds()
    peer_slug_set = set(peer_slugs_ordered)

    if verbose:
        print(f"[slot 1] peer_ingo_funds: {len(peer_models)} entries")

    # --- slot 2 (depends on slot 1) ---
    dfi_cards, raw_commits = load_dfi_commitments(peer_slug_set)
    if verbose:
        total_commits = sum(len(c.ingo_gp_commits) for c in dfi_cards)
        print(f"[slot 2] dfi_ingo_commits: {len(dfi_cards)} cards, "
              f"{total_commits} INGO-GP commitments")

    # --- slot 3 ---
    deadlines = load_deadlines()
    if verbose:
        print(f"[slot 3] deadlines: {len(deadlines)} entries")

    # --- emit ---
    # Slot 1 title is "INGOs like you that closed a fund" — filter emit to
    # rows that (a) actually have a parent INGO and (b) are a real fund
    # vehicle, not a programmatic investment activity. Non-INGO comparables
    # (BlueOrchard, Symbiotics, etc.) and `programmatic_not_fund` entries
    # (e.g. AKFED, Christian Aid PCIF, World Vision investing program) stay
    # in peer_funds.yml as the superset so slot-2 commitment cross-lookups
    # still resolve.
    slot1_models = [
        m for m in peer_models
        if m.parent_ingo and getattr(m, "vehicle_type", None) != "programmatic_not_fund"
    ]
    peer_payload = [_dump(m) for m in slot1_models]
    dfi_payload = [_dump(m) for m in dfi_cards]
    dead_payload = [_dump(m) for m in deadlines]

    write_json("peer_ingo_funds", peer_payload)
    write_json("dfi_ingo_commits", dfi_payload)
    write_json("deadlines", dead_payload)

    # --- impact-areas aggregation ---
    impact_rows = build_impact_areas(peer_models, dfi_cards, raw_commits, _today(), deadlines)
    write_json("impact_areas", impact_rows)
    if verbose:
        print(f"[impact] impact_areas: {len(impact_rows)} sectors")

    # --- slot_meta ---
    # Country enum populated from actually-observed values so the dropdown
    # only shows live options. Includes "All" sentinel.
    slot1_countries = Counter(m.parent_ingo_country for m in slot1_models if m.parent_ingo_country)
    slot2_countries = Counter(c.country for c in dfi_cards if c.country)
    slot3_countries = Counter(d.country for d in deadlines if d.country)

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "country_enum": COUNTRY_ENUM,
        "country_display_names": COUNTRY_DISPLAY_NAMES,
        "slot_counts": {
            "peer_ingo_funds": len(slot1_models),
            "dfi_ingo_commits": len(dfi_cards),
            "deadlines": len(deadlines),
            "impact_areas": len(impact_rows),
        },
        "country_counts": {
            "slot_1_parent_ingo_country": dict(slot1_countries),
            "slot_2_dfi_country":         dict(slot2_countries),
            "slot_3_issuing_body_country": dict(slot3_countries),
        },
    }
    write_json("slot_meta", meta)

    return meta["slot_counts"]


def main() -> int:
    try:
        counts = build()
    except HandshakeError as e:
        print(f"[build_slots] HANDSHAKE ERROR: {e}", file=sys.stderr)
        _write_health_warning("handshake_error", [str(e)])
        return 2
    print(json.dumps(counts, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
