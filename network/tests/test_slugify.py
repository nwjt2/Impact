"""Tests for the canonical slugify."""
from __future__ import annotations

import pytest

from network.utils.slugify import match_key, slugify


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Mercy Corps Ventures", "mercy-corps-ventures"),
        ("Acumen H2R — Amplify", "acumen-h2r-amplify"),
        ("Acumen Hardest-to-Reach (H2R) — Amplify", "acumen-hardest-to-reach-h2r-amplify"),
        ("British International Investment", "british-international-investment"),
        ("Société Générale", "societe-generale"),
        ("  trailing  spaces  ", "trailing-spaces"),
        ("ALL CAPS", "all-caps"),
    ],
)
def test_slugify_canonical(name: str, expected: str) -> None:
    assert slugify(name) == expected


def test_slugify_rejects_empty() -> None:
    with pytest.raises(ValueError):
        slugify("")


def test_slugify_rejects_none() -> None:
    with pytest.raises(ValueError):
        slugify(None)  # type: ignore[arg-type]


def test_match_key_strips_legal_suffixes() -> None:
    assert match_key("Acme Capital") == match_key("Acme Capital Partners")
    assert match_key("Acme Inc.") == match_key("Acme")
    assert match_key("Acme LLC") == match_key("Acme Limited")
