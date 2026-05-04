"""Investor archetype classifier — single source of truth.

Used by both combine_fund_lps.py and combine_portco_investors.py to assign an
``Investor Type`` to newly-discovered investors. Existing rows in
investors.csv are preserved verbatim — operator hand-edits never get blown
away. This classifier only fires for slugs that appear for the first time.

Archetype enum (12 values):

    dfi               National / multilateral development finance institution
                      that LPs into impact funds.
    foundation        Endowment / philanthropic grantmaker (private,
                      corporate, community, religious, public charity).
    family-office     Single- or multi-family office, philanthropy LLC,
                      faith-based or HNWI collective.
    vc                Conventional venture / growth fund (impact-curious or
                      impact-aligned but not impact-dedicated).
    government        Sovereign / state government acting directly as an LP
                      (NOT via a development-cooperation arm — see
                      bilateral-donor).
    bank              Retail / commercial / cooperative bank acting as
                      treasury or impact LP.
    asset-manager     Dedicated impact asset manager, MIV (microfinance
                      investment vehicle), or fund-of-funds.
    pension-fund      Public or corporate retirement fund.
    corporate         Operating corporate (consumer / industrial / pharma /
                      tech) using P&L or treasury for an impact LP commit,
                      typically tied to a supply-chain or brand initiative.
    bilateral-donor   Bilateral-aid / development-cooperation agency
                      (USAID, BMZ, DFAT, Swiss Dev Coop). Usually a govt
                      department but operates as a fund LP via aid budget.
    cooperative-ngo   NGO / co-operative / faith-based organisation acting
                      as an LP into a peer fund (the Belgian agri-coop and
                      Dutch fair-trade cluster, INGOs LP'ing other INGOs'
                      vehicles).
    other             Doesn't fit; awaiting operator review.

The classifier is heuristic. Order matters — most-specific tests first.
Specific-name overrides take precedence over pattern matches.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Specific-name overrides. Keys are lowercased exact-or-substring matches;
# values are the archetype. Add an entry here when the heuristic gets it
# wrong for a known LP.
#
# Source-of-truth lives here, not on investors.csv directly, so re-runs that
# discover a previously-unknown variant of the name still classify correctly.
# ---------------------------------------------------------------------------
_NAME_OVERRIDES: dict[str, str] = {
    # ---- DFIs that don't match the "development finance" substring ----
    "british international investment": "dfi",
    "european investment bank": "dfi",
    "global environment facility": "dfi",
    "international finance corporation": "dfi",
    "kfw": "dfi",
    "sida": "dfi",
    "u.s. international development finance corporation": "dfi",
    "overseas private investment corporation": "dfi",
    "idb lab": "dfi",
    "idb invest": "dfi",
    "common fund for commodities": "dfi",
    "fsd africa investments": "dfi",
    "bancóldex": "dfi",
    "bancoldex": "dfi",
    "dutch good growth fund": "dfi",
    "global health investment corporation": "dfi",
    "swedfund": "dfi",
    "norfund": "dfi",
    "finnfund": "dfi",
    "proparco": "dfi",
    "fmo": "dfi",
    "bio-invest": "dfi",
    "bii": "dfi",
    "afd": "dfi",

    # ---- Banks ----
    "anz": "bank",
    "bank of america": "bank",
    "bnp paribas": "bank",
    "rabobank": "bank",
    "standard chartered": "bank",
    "banca etica": "bank",
    "vdk bank": "bank",
    "crédit coopératif": "bank",
    "credit cooperatif": "bank",
    "citigroup": "bank",
    "citi": "bank",
    "hsbc": "bank",
    "barclays": "bank",
    "deutsche bank": "bank",
    "ubs ag": "bank",
    "ubs group": "bank",
    "ubs wealth": "bank",
    "ubs optimus foundation": "foundation",  # UBS philanthropy arm — disambiguates from UBS bank
    "credit suisse": "bank",
    "jpmorgan": "bank",
    "goldman sachs": "bank",

    # ---- Asset managers / MIVs ----
    "symbiotics": "asset-manager",
    "triple jump": "asset-manager",
    "incofin investment management": "asset-manager",
    "incofin": "asset-manager",
    "nuveen": "asset-manager",
    "axa investment managers": "asset-manager",
    "impax asset management": "asset-manager",
    "tiaa": "asset-manager",
    "impactassets": "asset-manager",
    "impactassets inc.": "asset-manager",
    "wire group": "asset-manager",
    "invest in visions": "asset-manager",
    "capshift": "asset-manager",
    "blueorchard": "asset-manager",
    "responsability": "asset-manager",
    "developing world markets": "asset-manager",
    "alterfin": "asset-manager",
    "sidi": "asset-manager",
    "oikocredit": "asset-manager",
    "etimos": "asset-manager",

    # ---- Pension funds ----
    "apg": "pension-fund",
    "kbc pensioenfonds": "pension-fund",
    "spf": "pension-fund",
    "spov": "pension-fund",
    "calpers": "pension-fund",
    "calstrs": "pension-fund",
    "abp": "pension-fund",
    "pggm": "pension-fund",

    # ---- Corporates ----
    "dupont": "corporate",
    "ecolab": "corporate",
    "gap inc.": "corporate",
    "gap inc": "corporate",
    "mastercard": "corporate",
    "merck & co., inc.": "corporate",
    "merck": "corporate",
    "metlife foundation": "foundation",  # Corporate-foundation arm of MetLife
    "metlife": "corporate",
    "niagara bottling": "corporate",
    "paypal": "corporate",
    "prudential financial": "corporate",
    "reckitt": "corporate",
    "royal philips": "corporate",
    "philips": "corporate",
    "starbucks": "corporate",
    "the mills fabrica": "corporate",
    "mills fabrica": "corporate",
    "unilever": "corporate",
    "nestle": "corporate",
    "nestlé": "corporate",
    "danone": "corporate",
    "ikea": "corporate",
    "patagonia": "corporate",

    # ---- Bilateral / development-cooperation donors ----
    "usaid": "bilateral-donor",
    "u.s. agency for international development": "bilateral-donor",
    "swiss agency for development and cooperation": "bilateral-donor",
    "global affairs canada": "bilateral-donor",
    "european union": "bilateral-donor",
    "german federal ministry for economic cooperation and development": "bilateral-donor",
    "bmz": "bilateral-donor",
    "australian department of foreign affairs and trade": "bilateral-donor",
    "dfat": "bilateral-donor",
    "international trade centre": "bilateral-donor",
    "fcdo": "bilateral-donor",
    "norad": "bilateral-donor",

    # ---- Cooperative / NGO LPs ----
    "boerenbond": "cooperative-ngo",
    "broederlijk delen": "cooperative-ngo",
    "maatschappij voor roerend bezit van de boerenbond": "cooperative-ngo",
    "mrbb": "cooperative-ngo",
    "oxfam": "cooperative-ngo",
    "oxfam-solidarité belgium": "cooperative-ngo",
    "oxfam-solidarite belgium": "cooperative-ngo",
    "rikolto": "cooperative-ngo",
    "solidaridad": "cooperative-ngo",
    "sos faim luxembourg": "cooperative-ngo",
    "sos faim": "cooperative-ngo",
    "trias": "cooperative-ngo",
    "meda": "cooperative-ngo",
    "louvain coopération": "cooperative-ngo",
    "louvain cooperation": "cooperative-ngo",
    "ku leuven": "cooperative-ngo",
    "acv-csc metea": "cooperative-ngo",
    "uniting financial services": "cooperative-ngo",
    "ufs": "cooperative-ngo",
    "care enterprises": "cooperative-ngo",
    "chemonics international": "cooperative-ngo",
    "pathfinder": "cooperative-ngo",
    "clac": "cooperative-ngo",
    "coordinadora latinoamericana y del caribe de pequeños productores": "cooperative-ngo",

    # ---- Family offices missing from the registry today ----
    "alvarium": "family-office",
    "dreilinden": "family-office",
    "ferd": "family-office",
    "korys": "family-office",
    "legatum": "family-office",
    "sobrato philanthropies": "family-office",
    "the todd and anne mccormack fund": "family-office",
    "todd and anne mccormack fund": "family-office",
    "volksvermogen": "family-office",
    "fundación bancolombia": "foundation",
    "fundacion bancolombia": "foundation",
    "fundación sura": "foundation",
    "fundacion sura": "foundation",
    "fundación wwb colombia": "foundation",
    "fundacion wwb colombia": "foundation",
    "mercantil colpatria": "corporate",
    "ferd sosial entreprenører": "family-office",

    # Endowment-backed municipal authorities — their LP activity is via
    # philanthropic arms (City Bridge Foundation in this case).
    "city of london corporation": "foundation",

    # ---- VCs / specific-name fund managers (some scraped as LPs because
    # they LP into other funds via fund-of-funds vehicles) ----
    "bamboo capital partners": "asset-manager",  # Reclassified — Bamboo is an EM impact AM, not a conventional VC
}


# ---------------------------------------------------------------------------
# Pattern-based heuristics. Run after specific-name overrides.
# ---------------------------------------------------------------------------

_GOVT_AGENCIES = ("ministry", "department of")  # bilateral-donor caught above

_BANK_TOKENS = (
    " bank",
    "banque",
    "banca",
    "sparkasse",
    "raiffeisen",
)

_ASSET_MGR_TOKENS = (
    "investment management",
    "investment managers",
    "asset management",
    "asset managers",
    "capital management",
    "fund managers",
    "fund management",
    "wealth management",
)

_PENSION_TOKENS = (
    "pension",
    "pensioenfonds",
    "retirement system",
    "retirement fund",
)

_CORPORATE_SUFFIXES = (
    " inc.",
    " inc",
    " corp.",
    " corp",
    " corporation",
    " co., ltd",
    " plc",
    " ag",
    " s.a.",
)

_VC_SUFFIXES = (" ventures", " capital", " partners", " angel", " holdings")
_WELL_KNOWN_VC = (
    "andreessen horowitz", "a16z", "sequoia", "tiger global",
    "y combinator", "kindred", "variant", "sv angel", "coinbase",
    "lowercarbon", "floating point",
)


def classify_investor_type(name: str) -> str:
    """Best-guess investor type. Heuristic. Operator review can refine.

    Order: specific-name overrides → governments / DFIs / family-offices →
    banks / asset managers / pension funds → corporates → VCs → foundations
    → fallback "other".
    """
    if not name:
        return "other"
    n = name.lower().strip()

    # 1. Specific-name overrides (highest priority — handle edge cases).
    # Length-descending so longer matches win: "ubs optimus foundation"
    # beats a generic "ubs" → bank override.
    if n in _NAME_OVERRIDES:
        return _NAME_OVERRIDES[n]
    for needle in sorted(_NAME_OVERRIDES, key=len, reverse=True):
        if needle in n:
            return _NAME_OVERRIDES[needle]

    # 2. Family offices first — "Family Foundation" should not be classified
    # as plain foundation.
    if "family" in n and ("foundation" in n or "fund" in n or "office" in n):
        return "family-office"

    # 3. Sovereign government (acting directly, not via aid agency)
    if n.startswith("government of"):
        return "government"

    # 4. Bilateral / development-cooperation agencies (substring catch for
    # ones not in overrides)
    if "agency for international development" in n:
        return "bilateral-donor"
    if "ministry for economic cooperation" in n:
        return "bilateral-donor"
    if "agency for development" in n:
        return "bilateral-donor"

    # 5. Catch-all for state agencies that didn't match bilateral-donor —
    # treat as government.
    if any(w in n for w in _GOVT_AGENCIES):
        return "government"

    # 6. DFIs by canonical phrase
    if "development finance" in n or " dfi " in f" {n} ":
        return "dfi"
    if n.endswith(" development bank") or " development bank" in n:
        # Heuristic: "<X> Development Bank" usually a DFI (KfW, ADB, AfDB, etc.)
        return "dfi"

    # 7. Pension funds (before banks — some pension funds end in "Bank"
    # but the pension token is more specific).
    if any(t in n for t in _PENSION_TOKENS):
        return "pension-fund"

    # 8. Asset managers (before banks — "X Capital Management" is an AM)
    if any(t in n for t in _ASSET_MGR_TOKENS):
        return "asset-manager"

    # 9. Banks
    if any(t in n for t in _BANK_TOKENS):
        return "bank"

    # 10. VC / equity funds — pattern: ends in Ventures / Capital / Partners
    if any(n.endswith(s) for s in _VC_SUFFIXES):
        return "vc"
    if any(w in n for w in _WELL_KNOWN_VC):
        return "vc"

    # 11. Foundations / religious / charitable
    if "foundation" in n or "philanthropy" in n or "philanthrophy" in n:
        return "foundation"
    if "missionary" in n or "sister" in n or "religious" in n:
        return "foundation"
    if "investment services" in n or "charitable" in n:
        return "foundation"

    # 12. Corporates by legal-suffix tail
    if any(n.endswith(s) for s in _CORPORATE_SUFFIXES):
        return "corporate"

    # 13. Wealth advisors → other
    if "advisors" in n or "advisor" in n:
        return "other"

    return "other"


# Stable iterable of canonical archetypes — useful for ARCHETYPE_ORDER on
# the homepage and for tests that enumerate types.
ARCHETYPES: tuple[str, ...] = (
    "dfi",
    "foundation",
    "family-office",
    "asset-manager",
    "bank",
    "pension-fund",
    "corporate",
    "bilateral-donor",
    "government",
    "cooperative-ngo",
    "vc",
    "other",
)
