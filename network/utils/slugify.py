"""Single canonical slugify. All scrapers and prep scripts MUST use this."""
import re
import unicodedata

_SUFFIX_RE = re.compile(
    r"\b(inc|incorporated|ltd|limited|llp|llc|plc|sa|ag|gmbh|"
    r"capital|partners|holdings|group|company|co)\b\.?",
    flags=re.IGNORECASE,
)
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_DASHES = re.compile(r"-+")


def slugify(name: str, *, strip_suffixes: bool = False) -> str:
    """Slugify a name into kebab-case.

    `strip_suffixes` is for *match* keys when deduping e.g. "Acme Capital" vs
    "Acme Capital Partners". Leave False for the canonical slug stored in the
    catalogue — existing peer_funds.yml slugs preserve suffixes like "ventures".
    """
    if name is None:
        raise ValueError("slugify(None) — caller must provide a name")
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    n = n.lower().strip()
    if strip_suffixes:
        n = _SUFFIX_RE.sub(" ", n)
    n = _NON_ALNUM.sub("-", n)
    n = _DASHES.sub("-", n).strip("-")
    if not n:
        raise ValueError(f"slugify produced empty slug for {name!r}")
    return n


def match_key(name: str) -> str:
    """Stable dedup key — strips legal suffixes."""
    return slugify(name, strip_suffixes=True)
