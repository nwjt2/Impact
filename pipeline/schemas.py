"""Pydantic schemas for the INGO first-close tool.

The tool is a reference surface for INGOs on the first-close journey. Zero
user state. Three public slot JSONs:

  - PeerIngoFund     — slot 1 card shape (peer INGO fund registry)
  - DfiIngoCommit    — slot 2 card shape (DFI profile + committed peer funds)
  - DfiProfile       — nested within DfiIngoCommit
  - Deadline         — slot 3 card shape

Infrastructure models:
  - Source           — pipeline/sources.yml row
  - BriefItem        — one normalized scraper item; used by the self-heal
                       pipeline (tests, rehearsals, future write-back).
                       No user-visible page consumes these.
  - FailureRecord    — tool/health/open/*.md front-matter
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


# ---- Source ---------------------------------------------------------------

ContentType = Literal[
    "lp_commitment",
    "peer_close",
    "guideline",
    "regulator_bulletin",
]


class Source(BaseModel):
    """One entry from pipeline/sources.yml.

    `slots` is an informational tag indicating which of the three slots
    (1/2/3) this source feeds. The `status` field (`active` | `parked`)
    lets us keep a dropped source's config in-tree for re-activation
    without the scraper picking it up.
    """

    model_config = ConfigDict(extra="allow")

    id: str
    name: str
    url: str
    type: Literal["rss", "html", "api"] = "rss"
    content_type: ContentType = "lp_commitment"
    expected_minimum_cadence_days: Optional[int] = None
    jurisdiction_tag: Optional[str] = None
    break_token: str = ""
    validation_status: Literal["verified", "unverified"] = "verified"

    slots: list[int] = Field(default_factory=list)
    status: Literal["active", "parked"] = "active"

    html_selectors: Optional[dict] = None
    api_params: Optional[dict] = None


# ---- BriefItem (scraper output) -------------------------------------------


class BriefItem(BaseModel):
    """One normalized scraper entry.

    Scrapers emit these as self-heal telemetry and for the future
    slot-matcher that will bump `last_seen_at` on registry rows. No
    user-visible page renders BriefItems.
    """

    model_config = ConfigDict(extra="allow")

    id: str  # stable: hash of (source_id, url)
    source_id: str
    source_name: str
    title: str
    url: str
    published_at: Optional[datetime] = None
    summary: str = ""
    content_type: ContentType = "lp_commitment"
    body_excerpt: str = ""


# ---- FailureRecord --------------------------------------------------------

FailureClass = Literal[
    "http_error",
    "parse_error",
    "rate_limit",
    "schema_drift",
    "timeout",
    "suspicious_silence",  # C6
]


class FailureRecord(BaseModel):
    """Incident record written to tool/health/open/."""

    model_config = ConfigDict(extra="allow")

    schema_version: int = 1
    source_id: str
    source_name: str
    source_url: str
    first_failed_run: datetime
    last_failed_run: datetime
    consecutive_failures: int = 1
    failure_class: FailureClass
    http_status: Optional[int] = None
    bytes_received: Optional[int] = None
    selector_or_endpoint: Optional[str] = None
    rows_returned: int = 0
    rows_expected_min: int = 1
    last_successful_run: Optional[datetime] = None
    severity: Literal["warn", "crit"] = "warn"
    note: str = ""


# ---- Slot 1: Peer INGO Fund ----------------------------------------------

FundStatus = Literal["raising", "deployed", "wound_down"]
VehicleType = Literal[
    "closed_end_fund", "evergreen", "blended", "DIB", "listed_bond",
    # `programmatic_not_fund` surfaces AKAM / NatureVest / Root Capital-style
    # vehicles that are not true fund structures.
    "programmatic_not_fund",
]

# Tranche types for the blended-finance capital stack. Ordered from least
# risky (top of stack, first paid) to most risky (bottom, last paid). The
# `grant` value is for cases where a grant arm sits *inside* the vehicle's
# capitalization, distinct from a parallel TA facility on the side.
CapitalStackTrancheType = Literal[
    "senior_debt", "junior_debt", "mezzanine",
    "equity", "first_loss_equity",
    "guarantee", "grant",
]


class CapitalStackTranche(BaseModel):
    """One tranche in a blended-finance fund's capital stack.

    Sourcing discipline: every tranche should carry a source_url. The
    source_quote is optional but encouraged where the public document
    uses a memorable phrasing — it lets the site cite the disclosure
    verbatim instead of paraphrasing.
    """

    model_config = ConfigDict(extra="allow")

    tranche: CapitalStackTrancheType
    provider: Optional[str] = None       # free-text (LP names sometimes carry parentheticals)
    provider_slug: Optional[str] = None  # optional handshake → LP registry slug
    size_usd_m: Optional[float] = None
    source_url: Optional[str] = None
    source_quote: Optional[str] = None


class ParallelTAFacility(BaseModel):
    """Technical-assistance / grant sidecar that sits alongside a fund.

    Distinct from a `grant` tranche inside the capital stack: the TA
    facility is parallel capital, not a layer of the investible vehicle.

    `exists` semantics:
      • True   — public record names a TA facility
      • False  — public record explicitly says no TA facility
      • None   — not in public record (the common case; honest-null)
    """

    model_config = ConfigDict(extra="allow")

    exists: Optional[bool] = None
    size_usd_m: Optional[float] = None
    funder: Optional[str] = None
    source_url: Optional[str] = None


class BlendedFinanceStructure(BaseModel):
    """Slot 1 — per-fund blended-finance disclosure block.

    Hit-rate reality (per the 4-fund test):
      • capital_stack          ~80% of funds have ≥1 layer publicly disclosed
      • parallel_ta_facility   ~30%
      • vehicle_legal_form     ~50%
      • instruments_offered    ~70%
    Everything is optional. Honest-null discipline: if a layer or tranche
    is not in the public record, leave it out — never invent.
    """

    model_config = ConfigDict(extra="allow")

    capital_stack: list[CapitalStackTranche] = Field(default_factory=list)
    parallel_ta_facility: Optional[ParallelTAFacility] = None
    vehicle_legal_form: Optional[str] = None  # free-text: "Delaware LP", "Luxembourg SCSp", "limited liability company"
    instruments_offered: list[str] = Field(default_factory=list)
    structure_notes: Optional[str] = None
    structure_source_urls: list[str] = Field(default_factory=list)
    # Optional eyebrow label, rendered above the block on the peer-fund card.
    # Populate only when the structure refers to a sub-vehicle distinct from
    # the platform name on the card (e.g. MCV's RFF Fund II inside the
    # platform-level card). Leave null when the BF block describes the same
    # entity as the card heading.
    scope_label: Optional[str] = None


# ---- Fund-KPIs block (per-fund impact reporting disclosure) ---------------
#
# Curated for ~10–15 exemplar INGO-sponsored funds spanning the
# disclosure-quality spectrum. Renders inline on the peer fund card and
# powers a separate /kpis/ gallery page.
#
# The 3-fund pilot established that fund-LEVEL attribution is the
# exception, not the rule — most INGO funds publish at manager-portfolio
# or parent-org level. The `attribution_level` field on each metric
# captures this, and the block-level `scope` field flags the whole entry.

FundKpiScope = Literal[
    "fund",                     # KPIs are fund-attributed end to end
    "manager_portfolio_proxy",  # only manager-portfolio aggregates published; used as proxy
    "parent_org_proxy",         # only parent-INGO or 501(c)(3) aggregates published
]
FundKpiDisclosureQuality = Literal[
    "comprehensive",        # multi-pathway, current, well-documented (e.g. Root Capital)
    "partial",              # fund-specific snapshot only at close, plus portfolio aggregates
    "scattered_milestones", # disclosure across blog posts / case studies / press milestones
    "placeholder_only",     # public surface shows zeros / no current numbers
]
FundKpiReportFormat = Literal[
    "pdf",
    "html",
    "dashboard",
    "annual_report_section",
    "scattered_milestone_disclosure",
]
FundKpiAttributionLevel = Literal[
    "fund_attributed",        # this fund's investments produced this number
    "pro_rated",              # pro-rated share of FI/portco outcomes
    "manager_portfolio",      # aggregate across manager's funds
    "franchise_fi_aggregate", # totals at portfolio FIs (clients, AUM at FI level)
    "parent_org_aggregate",   # parent-INGO or 501(c)(3) aggregate
]
FundKpiMetricType = Literal[
    "realized",            # actual outcome to date
    "target",              # target set at close / in IPS
    "longitudinal_outcome",# multi-year cohort follow-up
    "cohort_outcome",      # one-time cohort study
    "survey_outcome",      # client survey (e.g. 60 Decibels)
]


class FundKpiMetric(BaseModel):
    """One KPI as published in a fund's impact report.

    Verbatim source quote is mandatory in practice (the curated process
    requires it). The schema itself leaves source_quote optional only so
    that placeholder-only / narrative-only metrics can be captured with
    value=None and source_quote describing the absence.
    """

    model_config = ConfigDict(extra="allow")

    metric_name: str                                 # verbatim — never paraphrased
    tags: list[str] = Field(default_factory=list)   # informal cross-cut: gender, climate, agri, ...
    value: Optional[float] = None                    # null if narrative-only
    value_text: Optional[str] = None                 # verbatim composite values ("46% pre → 8% post")
    unit: Optional[str] = None                       # free-text — "people", "%", "MT CO2e", "USD m"
    cumulative_since: Optional[int] = None           # year; null = single-period metric
    period_end: Optional[str] = None                 # YYYY or YYYY-MM-DD or null
    iris_plus_id: Optional[str] = None
    attribution_level: FundKpiAttributionLevel
    metric_type: FundKpiMetricType = "realized"
    source_url: str
    source_quote: Optional[str] = None               # verbatim, ≤200 chars
    caveat: Optional[str] = None                     # ≤2 sentences — methodology gaps, scope notes


class FundKpis(BaseModel):
    """Slot 1 — per-fund impact-KPI disclosure block.

    Curated for exemplar funds only (~10–15 of the 51 INGO-sponsored peer
    funds, plus a small number of non-INGO comparables). Funds
    without this block render normally; funds with it render an additional
    `Impact KPIs` section on their card and appear on the /kpis/ gallery.

    Honest-null discipline:
      • iris_plus_id is null on most metrics — even IRIS+-aligned
        managers rarely print the codes alongside KPIs
      • attribution_level is the most important field; without it the
        artifact misleads readers about what a number actually represents
    """

    model_config = ConfigDict(extra="allow")

    scope: FundKpiScope
    disclosure_quality: FundKpiDisclosureQuality
    report_year: Optional[int] = None
    report_url: Optional[str] = None
    report_format: Optional[FundKpiReportFormat] = None
    reporting_framework_tags: list[str] = Field(default_factory=list)
    metrics: list[FundKpiMetric] = Field(default_factory=list)
    structure_notes: Optional[str] = None
    structure_source_urls: list[str] = Field(default_factory=list)
    # Optional eyebrow label, rendered above the KPI block on the peer-fund
    # card. Populate when the KPIs cover a different entity than the card's
    # blended-finance structure block (e.g. platform-lifetime aggregates next
    # to a single sub-vehicle's structure). Leave null when both blocks
    # describe the same entity.
    scope_label: Optional[str] = None


class PeerIngoFund(BaseModel):
    """Slot 1: one peer INGO fund card.

    Data flows from content/peer_funds.yml (hand-curated). Scrapers only
    refresh `last_seen_at`; they do NOT synthesize new entries.
    """

    model_config = ConfigDict(extra="allow")

    slug: str
    name: str
    manager: Optional[str] = None
    parent_ingo: Optional[str] = None
    parent_ingo_slug: Optional[str] = None
    parent_ingo_country: Optional[str] = None  # ISO-2 uppercase or null
    parent_ingo_country_secondary: list[str] = Field(default_factory=list)
    vintage: Optional[int] = None
    first_close_date: Optional[date] = None
    first_close_date_month_only: bool = False
    final_close_date: Optional[date] = None
    size_usd_m: Optional[float] = None
    size_usd_m_original_ccy: Optional[str] = None
    sector_tags: list[str] = Field(default_factory=list)
    geo_tags: list[str] = Field(default_factory=list)
    vehicle_type: Optional[VehicleType] = None
    anchor_lp: Optional[str] = None
    anchor_lp_slug: Optional[str] = None
    named_lps: list[str] = Field(default_factory=list)
    timeline_days_announce_to_first_close: Optional[int] = None
    mgmt_fee_bps: Optional[int] = None
    carry_pct: Optional[float] = None
    hurdle_pct: Optional[float] = None
    gp_commit_pct: Optional[float] = None
    investment_period_years: Optional[float] = None
    sub_advisor: Optional[str] = None
    placement_agent: Optional[str] = None
    blended_finance_structure: Optional[BlendedFinanceStructure] = None
    fund_kpis: Optional[FundKpis] = None
    public_source_url: Optional[str] = None
    status: FundStatus = "raising"
    notes: Optional[str] = None
    last_seen_at: Optional[date] = None
    validation_status: Literal["verified", "unverified"] = "verified"


# ---- Slot 2: DFI → INGO-GP commitments -----------------------------------

CommitRole = Literal["anchor", "co_lead", "participant"]
LpType = Literal["dfi", "multilateral", "foundation", "other"]


class IngoGpCommit(BaseModel):
    """One commitment row nested inside DfiIngoCommit.ingo_gp_commits."""

    model_config = ConfigDict(extra="allow")

    peer_fund_slug: Optional[str] = None  # handshake → PeerIngoFund.slug
    peer_fund_name: str
    parent_ingo: Optional[str] = None
    commit_date: Optional[date] = None
    commit_date_month_only: bool = False
    amount_usd_m: Optional[float] = None
    amount_usd_m_original_ccy: Optional[str] = None
    role: Optional[CommitRole] = None
    public_source_url: Optional[str] = None
    notes: Optional[str] = None


class TicketRange(BaseModel):
    model_config = ConfigDict(extra="allow")
    min: float
    median: float
    max: float
    n: int


class EmergingManagerFacility(BaseModel):
    model_config = ConfigDict(extra="allow")
    exists: bool = False
    program_name: Optional[str] = None
    application_url: Optional[str] = None
    notes: Optional[str] = None


class DfiIngoCommit(BaseModel):
    """Slot 2: one DFI card with its INGO-GP commitment history.

    Flows from content/dfi_ingo_commitments.yml. Typical-ticket range is
    computed at emit time from the commitments list.
    """

    model_config = ConfigDict(extra="allow")

    slug: str
    name: str
    aliases: list[str] = Field(default_factory=list)
    country: Optional[str] = None  # ISO-2 HQ country, or null if unresolved
    policy_remit: Optional[str] = None  # "EU", "INT", etc. — filter hint
    lp_type: LpType = "dfi"
    typical_ticket_usd_m_range: Optional[TicketRange] = None
    ingo_gp_commit_count_5y: int = 0
    ingo_gp_commit_count_10y: int = 0
    ingo_gp_commits: list[IngoGpCommit] = Field(default_factory=list)
    stated_sector_priorities: list[str] = Field(default_factory=list)
    stated_geo_priorities: list[str] = Field(default_factory=list)
    stated_thesis_url: Optional[str] = None
    stated_thesis_excerpt: Optional[str] = None
    emerging_manager_facility: Optional[EmergingManagerFacility] = None
    named_contact: Optional[str] = None
    named_contact_title: Optional[str] = None
    last_known_activity_date: Optional[date] = None
    last_known_activity_url: Optional[str] = None
    public_newsroom_url: Optional[str] = None
    last_seen_at: Optional[date] = None
    status: Literal["active", "defunct"] = "active"
    defunct_since: Optional[date] = None
    defunct_note: Optional[str] = None


# ---- Foundation LPs (separate page, parallel to slot 2) ------------------

FoundationType = Literal[
    "private",       # private grantmaking foundation (Ford, MacArthur)
    "corporate",     # corporate foundation (Visa, Citi, JPMorgan Chase)
    "community",     # community foundation
    "operating",     # operating foundation that runs programs directly
    "public_charity", # 501(c)(3) public charity
    "supporting_org", # supporting org or DAF host
    "philanthropy",  # umbrella for newer-form vehicles (Bloomberg, Bezos Earth Fund)
]
FoundationVehicle = Literal["grant", "pri", "mri", "fund_lp", "guarantee", "loan"]


class FoundationProgram(BaseModel):
    """Foundation PRI / MRI / fund-LP program block.

    Same shape as EmergingManagerFacility for consistency. `exists=null`
    permitted (we treat exists==False with a "could not be confirmed" note
    as the explicit honest-null pattern).
    """
    model_config = ConfigDict(extra="allow")
    exists: bool = False
    program_name: Optional[str] = None
    application_url: Optional[str] = None
    notes: Optional[str] = None


class FoundationLp(BaseModel):
    """One foundation LP card.

    Flows from content/foundation_lps.yml. Sectors / geos / AUM / vehicle
    types follow honest-null discipline — only confirmed via primary
    sources (foundation site, 990s, annual reports).
    """

    model_config = ConfigDict(extra="allow")

    slug: str
    name: str
    aliases: list[str] = Field(default_factory=list)
    country: Optional[str] = None
    foundation_type: Optional[FoundationType] = None
    aum_usd_m: Optional[float] = None
    aum_usd_m_year: Optional[int] = None
    stated_priority_themes: list[str] = Field(default_factory=list)
    stated_geo_focus: list[str] = Field(default_factory=list)
    stated_thesis_url: Optional[str] = None
    stated_thesis_excerpt: Optional[str] = None
    pri_program: Optional[FoundationProgram] = None
    mri_program: Optional[FoundationProgram] = None
    typical_check_usd_m_min: Optional[float] = None
    typical_check_usd_m_max: Optional[float] = None
    known_ingo_gp_commits: list[IngoGpCommit] = Field(default_factory=list)
    public_newsroom_url: Optional[str] = None
    last_seen_at: Optional[date] = None


# ---- Family-Office / Faith-Based / DAF LPs -------------------------------

FamilyOfficeCategory = Literal[
    "family_office",     # single- or multi-family
    "faith_based",       # religious-mission asset manager
    "daf",               # donor-advised fund host
    "philanthropy_llc",  # 501(c)(4) or LLC vehicle (CZI, Yield Giving)
    "hnwi_collective",   # named individual or pooled HNWI vehicle
]


class FamilyOfficeLp(BaseModel):
    """One family-office / faith-based / DAF LP card.

    Flows from content/family_office_lps.yml. Smaller, faster check-writers
    than DFIs; often catalytic for INGO-GP first close. Honest-null for
    AUM and named-anchor history where not publicly disclosed.
    """

    model_config = ConfigDict(extra="allow")

    slug: str
    name: str
    aliases: list[str] = Field(default_factory=list)
    country: Optional[str] = None
    category: Optional[FamilyOfficeCategory] = None
    aum_usd_m: Optional[float] = None
    aum_usd_m_year: Optional[int] = None
    stated_priority_themes: list[str] = Field(default_factory=list)
    stated_geo_focus: list[str] = Field(default_factory=list)
    stated_thesis_url: Optional[str] = None
    stated_thesis_excerpt: Optional[str] = None
    typical_check_usd_m_min: Optional[float] = None
    typical_check_usd_m_max: Optional[float] = None
    invests_via_fund_lp: Optional[bool] = None  # null = not confirmed
    invests_via_direct: Optional[bool] = None
    invests_via_grants: Optional[bool] = None
    known_ingo_gp_commits: list[IngoGpCommit] = Field(default_factory=list)
    public_newsroom_url: Optional[str] = None
    last_seen_at: Optional[date] = None


# ---- Slot 3: Deadlines ----------------------------------------------------

DeadlineKind = Literal[
    "rfp",
    "open_call",
    "rolling_application",
    "board_meeting",
    "template_revision",
    "regulator_filing",
    "conference_deadline",
]
Recurring = Literal["rolling", "annual", "biennial", "quarterly", "one_off"]


class Deadline(BaseModel):
    """Slot 3: one deadline card.

    Flows from content/deadlines.yml. Deadlines with a hard `deadline_date`
    strictly earlier than today drop out at emit time; rolling / annual /
    quarterly entries without a fixed date always emit.
    """

    model_config = ConfigDict(extra="allow")

    deadline_id: str
    issuing_body: str
    country: Optional[str] = None  # ISO-2 (or "EU", "INT") of issuing body
    kind: DeadlineKind
    title: str
    deadline_date: Optional[date] = None
    recurring: Optional[Recurring] = None
    next_occurrence: Optional[date] = None  # computed or curated best-guess of next cycle start
    why_it_matters: Optional[str] = None
    public_source_url: Optional[str] = None
    last_verified_at: Optional[date] = None
    source_kind: Literal["scraped", "curated"] = "curated"
