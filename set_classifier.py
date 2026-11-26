"""
set_classifier.py
=================
Shared classifier primitives for distinguishing numbered Pokémon singles
from sealed products on a PriceCharting listing.

Extracted from import_30th_celebration.py so both the one-shot import
script and the new onboard_new_set.py pipeline share one source of
truth for the sealed-keyword list. Behaviour is byte-for-byte identical
to the version previously inlined in import_30th_celebration.py.
"""

import re


CARD_NUMBER_RE = re.compile(r"#(\w+)")


SEALED_KEYWORDS = [
    "booster", "pack", "box", "blister", "bundle",
    "collection", "tin", "etb", "elite trainer",
    "build & battle", "build and battle", "case", "display", "deck",
]


_SEALED_PATTERNS = [
    re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE) for kw in SEALED_KEYWORDS
]


def extract_card_number(product_name: str):
    """Return the raw card number token (e.g. "129", "XY-P") if the
    product name ends in / contains a "#NN" marker, else None."""
    if not product_name:
        return None
    m = CARD_NUMBER_RE.search(product_name)
    return m.group(1) if m else None


def is_sealed_by_name(product_name: str) -> bool:
    """Word-boundary keyword match. Only consulted for rows that do NOT
    have a #NN suffix (any product with a card number is a card, not a
    sealed product). Mirrors import_30th_celebration.is_sealed_by_name."""
    if not product_name:
        return False
    for pat in _SEALED_PATTERNS:
        if pat.search(product_name):
            return True
    return False


BRACKET_VARIANT_RE = re.compile(r"\[[^\]]+\]")


def has_bracket_variant(product_name: str) -> bool:
    """True when the product name carries a bracketed variant tag such
    as "[Reverse Holo]" or "[1st Edition]". Used for reporting only."""
    if not product_name:
        return False
    return BRACKET_VARIANT_RE.search(product_name) is not None
