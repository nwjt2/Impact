"""Seed network/catalogue/ CSVs from existing YAML files.

Reads:
  content/peer_funds.yml          -> impact_funds.csv + ingos.csv
  content/family_office_lps.yml   -> investors.csv (canonical for the 27 curated
                                     family-office / faith-based / philanthropy-
                                     LLC / DAF entries that drive the
                                     /family-offices/ page).
  pipeline/entities.yml           -> investors.csv (target_lps section, lower
                                     priority than family_office_lps.yml).

Writes:
  network/catalogue/impact_funds.csv
  network/catalogue/ingos.csv
  network/catalogue/investors.csv
  network/docs/discovery_skip_list.csv (appended for excluded vehicles)

investors.csv merge rules (re-runnable, preserves scraper-discovered rows):
  1. family_office_lps.yml entries seed first; their YAML category → CSV
     investor_type via CATEGORY_TO_INVESTOR_TYPE.
  2. entities.yml target_lps fill in the rest. Slugs canonicalized through
     investor_aliases.csv so e.g. entities.yml's "Tides" (slug `tides`) does
     not duplicate family_office_lps.yml's `tides-foundation`.
  3. Existing rows in investors.csv whose canonical slug isn't seeded by
     either YAML are preserved verbatim (these are scraper-discovered LPs
     that came in via combine_fund_lps.py / combine_portco_investors.py).

LP commitments curated in family_office_lps.yml's known_ingo_gp_commits are
bridged into fund_lps.csv by a separate post-combine step,
inject_yaml_family_office_commits.py — same pattern as
inject_yaml_dfi_commitments.py for the DFI side.

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
FAMILY_OFFICE_LPS_YML = REPO_ROOT / "content" / "family_office_lps.yml"
CATALOGUE_DIR = REPO_ROOT / "network" / "catalogue"
INVESTORS_CSV = CATALOGUE_DIR / "investors.csv"
SKIP_LIST_CSV = REPO_ROOT / "network" / "docs" / "discovery_skip_list.csv"

sys.path.insert(0, str(REPO_ROOT))
from network.utils.aliases import canonicalize_investor_slug  # noqa: E402
from network.utils.csv_io import read_rows, write_rows  # noqa: E402
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
    # Activated 2026-04-28. LP rosters scraped from primary press releases.
    # FEFISOL II from EIB launch PR; WaterEquity GAF IV from Newswire final
    # close; WWB Fund II from Women's World Banking insights post.
    "fefisol-ii",
    "waterequity-waci4",
    "wwb-capital-partners-fund-ii",
    # Activated 2026-04-28. Portfolio scraped from /portfolio/ archive page.
    "andgreen-fund",
    # Activated 2026-04-28 (second batch). All four scrapers follow already-
    # established patterns:
    #   - Kampani: portfolio + LP scrapers from kampani.org (Webflow site
    #     with clean <h3> portcos and a named-href shareholder grid).
    #   - WWB Fund I: portfolio scraper sharing the same multi-fund WWB
    #     asset-management page that also serves Fund II portcos.
    #   - WCIF3: LP scraper from WaterEquity's PR Newswire first-close release.
    #   - ALEG: LP scraper from LAVCA's coverage of the August 2020 final
    #     close (acumen.org itself is Cloudflare-walled).
    "kampani",
    "wwb-capital-partners-fund",
    "watercredit-investment-fund-3",
    "aleg-acumen-latam-early-growth",
    # Activated 2026-04-28 (third batch).
    #   - CI Ventures: portfolio scraper from conservation.org/ci-ventures
    #     (Sanity/Next.js page with a clean <a href="...ci-ventures/<slug>">
    #     anchor + nested <span class="button-text">NAME</span> structure;
    #     name field uses zero-width Unicode obfuscation that we strip).
    #   - Truvalu Business Booster Fund: portfolio scraper from truvalu-group.com
    #     /portfolio/ (paginated WordPress, 30 entries across 3 pages — mix of
    #     portcos and program/case-study posts as Truvalu groups them).
    #   - CARE-SheTrades: small sponsor LP scraper (3 named anchors: ITC,
    #     CARE Enterprises, Bamboo CP) from Bamboo's April 2020 ITC-joins PR.
    #   - Oxfam SEIIF: small sponsor LP scraper (3 named launch partners:
    #     Oxfam, City of London Corporation, Symbiotics) from Third Sector's
    #     October 2012 launch article.
    "ci-ventures",
    "truvalu-business-booster-fund",
    "care-shetrades",
    "oxfam-seiif",
    # Activated 2026-04-29 (non-INGO batch — all 9 are non-INGO peer funds).
    # 4 portfolio scrapers + 5 LP scrapers.
    #   - goodwell-umunthu-ii: portfolio scraper from goodwell.nl/portfolio/
    #     (16 portcos; per-portco detail-page <title> resolves curated names
    #     including the ABC/Oradian rebrand). Umbrella attribution.
    #   - creation-investments-iv: portfolio scraper from creationinvestments
    #     .com/portfolio/ (23 portcos; anchor-text label "<Country> | <Sector>"
    #     filters; company names derived from outbound hostname). Umbrella.
    #   - ifc-aip: portfolio scraper from ifcamc.org/portfolio?page=0..8
    #     (135 entries across 9 paginated pages; Tailwind <article> blocks
    #     with <h3>NAME + GEOGRAPHY/INDUSTRY spans; outbound URLs in
    #     commented-out <a href> wrappers).
    #   - grassroots-business-fund: portfolio scraper from gbfund.org/impact
    #     case-study section (7 portcos in <h4> headings).
    #   - bamboo-bloc-smart-africa: LP scraper from TWO Bamboo PRs
    #     (Feb 2021 launch + Sep 2019 DRC/Tunisia commitment); 4 named
    #     government LPs. Multi-source LP scraper (deviation from FEFISOL
    #     single-source pattern; per-row Source URL is schema-supported).
    #   - iix-wlb-series: LP scraper from WLB6 launch PR on wlb.iixglobal.com
    #     (15 named LPs; corporate/DFI/foundation mix).
    #   - aavishkaar-india-vi: LP scraper from PR Newswire 2017 first-close
    #     PR (4 named anchor LPs: SIDBI, CDC, Munjal Family Office, TIAA).
    #   - incofin-agrif: LP scraper from TWO Incofin PRs (Apr 2017 + Jan 2020);
    #     17 named LPs (DFIs, pension funds, Belgian retail co-ops, family
    #     offices). Multi-source LP scraper. Largest non-INGO LP roster.
    #   - ecoenterprises-iii: LP scraper from THREE primary DFI/MDB project
    #     pages (EIB + Common Fund for Commodities + 2X Challenge); 3 named
    #     LPs. Multi-source LP scraper — required because the fund itself
    #     publishes no LP roster, so each LP's own institution discloses.
    "goodwell-umunthu-ii",
    "creation-investments-iv",
    "ifc-aip",
    "grassroots-business-fund",
    "bamboo-bloc-smart-africa",
    "iix-wlb-series",
    "aavishkaar-india-vi",
    "incofin-agrif",
    "ecoenterprises-iii",
    # Activated 2026-04-29 (Acumen umbrella reroute).
    #   - acumen-capital-partners: cross-fund roll-up of Acumen-managed
    #     direct investments (160 companies at acumen.org/companies). The
    #     YAML marks this `vehicle_type: programmatic_not_fund` because it's
    #     not a discrete LPA-fund — the discrete Acumen funds (Kawisafi,
    #     ARAF, ALEG, H2R) live under their own slugs. We force-allow it
    #     into impact_funds.csv so catalyst attribution counts Acumen-
    #     overlapping portcos as INGO-funded rather than externally
    #     co-invested. See `derive_pipeline_status` below for the override.
    "acumen-capital-partners",
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

# family_office_lps.yml uses a finer-grained 4-way taxonomy than the network's
# 6 archetypes. faith-based investors and DAF hosts function as charitable-
# capital pools — both fold to `foundation` in the network view.
CATEGORY_TO_INVESTOR_TYPE = {
    "family_office": "family-office",
    "philanthropy_llc": "family-office",
    "faith_based": "foundation",
    "daf": "foundation",
    "hnwi_collective": "family-office",
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

    # ACTIVE_FUND_SLUGS overrides programmatic_not_fund exclusion. A handful
    # of YAML entries (e.g. Acumen's umbrella roll-up) are correctly tagged
    # as programmatic for the brief's comparable-fund framing, but we still
    # want them in the network catalogue so their portfolios attribute as
    # INGO-fund investments.
    if slug in ACTIVE_FUND_SLUGS and vtype == "programmatic_not_fund":
        return "active", None
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
    existing_rows: list[dict] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Build impact_funds.csv rows from peer_funds.yml, preserving operator edits.

    Per network/CLAUDE.md, four columns are operator-editable and must survive
    re-runs: `Portfolio Page URL`, `LP Page URL`, `Fund Type`, `Pipeline Status`.
    For each existing slug we keep the operator's value where present.

    `Pipeline Status` has a wrinkle: YAML / skip-list truth wins for terminal
    or operational states (`wound_down`, `blocked`); operator's CSV value
    wins for `active` / `pending_onboard` (so demoting a fund in the CSV to
    pause scraping doesn't get reverted on every sync).
    """
    rows = []
    skip = []
    today = date.today().isoformat()
    blocked = load_blocked_slugs()
    by_slug = {r.get("Fund Slug"): r for r in (existing_rows or []) if r.get("Fund Slug")}
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

        existing = by_slug.get(fund["slug"], {})

        if pipeline_status not in ("wound_down", "blocked"):
            existing_status = (existing.get("Pipeline Status") or "").strip()
            if existing_status:
                pipeline_status = existing_status

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
                "Portfolio Page URL": existing.get("Portfolio Page URL") or "",
                "LP Page URL": existing.get("LP Page URL") or "",
                "Fund Type": existing.get("Fund Type") or classify_fund_type(fund.get("notes")),
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


def _row_from_family_office(row: dict) -> dict:
    category = (row.get("category") or "").strip()
    return {
        "Investor Name": row.get("name") or row["slug"],
        "Investor Slug": row["slug"],
        "Investor Type": CATEGORY_TO_INVESTOR_TYPE.get(category, "other"),
        "Impact Focus": "unknown",
        "Website": row.get("public_newsroom_url") or "",
        "HQ Country": row.get("country") or "",
        "AUM Bucket": "unknown",
        "Status": "active",
        "Notes": "",
    }


def _row_from_target_lp(lp: dict) -> dict:
    name = lp["name"]
    return {
        "Investor Name": name,
        "Investor Slug": slugify(name),
        "Investor Type": LP_TYPE_TO_INVESTOR_TYPE.get(lp.get("lp_type"), "other"),
        "Impact Focus": "unknown",
        "Website": lp.get("public_newsroom_url") or "",
        "HQ Country": "",
        "AUM Bucket": "unknown",
        "Status": "active",
        "Notes": "",
    }


def build_investors(
    entities_doc: dict,
    family_office_doc: dict,
    existing_rows: list[dict],
) -> tuple[list[dict], dict[str, int]]:
    """Merge investors.csv from three sources, by canonical slug.

    Priority (highest first):
      1. family_office_lps.yml — explicit slug, category → investor_type.
      2. entities.yml target_lps — slugify(name), canonicalized via aliases.
      3. existing investors.csv rows for slugs not seeded by 1 or 2 (these
         are scraper-discovered LPs we want to preserve across re-runs).

    Returns (rows, counts) where counts breaks down by source.
    """
    by_slug: dict[str, dict] = {}
    counts = {"family_office_yml": 0, "entities_yml": 0, "preserved": 0}

    # Pass 1: family_office_lps.yml — canonical for the curated 27.
    for fo in (family_office_doc.get("family_offices") or []):
        slug = (fo.get("slug") or "").strip()
        if not slug or slug in by_slug:
            continue
        by_slug[slug] = _row_from_family_office(fo)
        counts["family_office_yml"] += 1

    # Pass 2: entities.yml target_lps. Canonicalize slug so e.g. "Tides"
    # (slug `tides`) collapses into `tides-foundation` if the alias map
    # says so, preventing duplicate rows for the same entity.
    for lp in entities_doc.get("target_lps", []):
        raw_slug = slugify(lp["name"])
        canonical = canonicalize_investor_slug(raw_slug)
        if canonical in by_slug:
            continue
        row = _row_from_target_lp(lp)
        row["Investor Slug"] = canonical
        by_slug[canonical] = row
        counts["entities_yml"] += 1

    # Pass 3: preserve scraper-discovered rows whose slug is unknown to YAML.
    # These came in via combine_fund_lps.py / combine_portco_investors.py;
    # we don't want sync to wipe them.
    for r in existing_rows:
        slug = (r.get("Investor Slug") or "").strip()
        if not slug or slug in by_slug:
            continue
        canonical = canonicalize_investor_slug(slug)
        if canonical != slug:
            # Existing row uses a deprecated slug; the canonical form is
            # already (or about to be) seeded — drop the deprecated row.
            continue
        by_slug[canonical] = {h: r.get(h, "") for h in INVESTORS_HEADERS}
        counts["preserved"] += 1

    return list(by_slug.values()), counts


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
    print(f"Reading {FAMILY_OFFICE_LPS_YML}")
    family_office_doc = (
        load_yaml(FAMILY_OFFICE_LPS_YML) if FAMILY_OFFICE_LPS_YML.exists() else {}
    )

    existing_funds = read_rows(CATALOGUE_DIR / "impact_funds.csv")
    impact_funds, skip = build_impact_funds_and_skip_list(peer_funds_doc, existing_funds)
    ingos = build_ingos(peer_funds_doc)
    existing_investors = read_rows(INVESTORS_CSV)
    investors, inv_counts = build_investors(
        entities_doc, family_office_doc, existing_investors
    )

    write_rows(CATALOGUE_DIR / "impact_funds.csv", IMPACT_FUNDS_HEADERS, impact_funds)
    write_rows(CATALOGUE_DIR / "ingos.csv", INGOS_HEADERS, ingos)
    write_rows(INVESTORS_CSV, INVESTORS_HEADERS, investors)

    new_skips = append_skip_list(skip) if skip else 0

    print(f"  impact_funds.csv: {len(impact_funds)} rows")
    print(f"  ingos.csv:        {len(ingos)} rows")
    print(
        f"  investors.csv:    {len(investors)} rows "
        f"({inv_counts['family_office_yml']} from family_office_lps.yml, "
        f"{inv_counts['entities_yml']} from entities.yml, "
        f"{inv_counts['preserved']} preserved discoveries)"
    )
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
