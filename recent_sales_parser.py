"""
recent_sales_parser.py — Block 4A-S1
====================================

Standalone parser for PriceCharting "recent completed sales" tables.

This module is intentionally **not** imported by the production scraper
(pokeprices_scraper_v8.py) and writes nothing to Supabase. It exists so
parsing logic can be developed and exhaustively tested against saved HTML
fixtures before any production wiring is added in a later block.

Public surface
--------------
    parse_recent_sales(html, *, page_url, provider_card_id,
                       internal_card_slug, import_run_id=None)
        -> RecentSalesParseResult

The parser never raises on malformed HTML. Failures are reported as a
typed result with `parse_status` set to one of:

    "ok"                  — fully parsed (may include quarantined rows)
    "no_section"          — page had no completed-auctions block at all
    "unsupported_layout"  — language inferred non-English, OR every known
                            selector signature was absent
    "failed"              — defensive bucket; only emitted when a parser
                            invariant breaks (no rows recovered AND an
                            unhandled error occurred)

Authoritative findings encoded here (from Block 4A-S audit):
- Recent sales live in the same response already fetched for current prices.
- Each grade section is its own ``<div class="completed-auctions-XXX">``
  containing a ``<table class="hoverable-rows sortable">``.
- Marketplace anchor classes seen: js-ebay-completed-sale,
  js-tcgplayer-completed-sale, js-ha-completed-sale, js-pwcc-completed-sale.
- Best Offer rows use ``title="best offer accepted price"`` (final, bold)
  and ``title="best offer list price"`` (crossed out).
- Japanese pages use a different layout with none of these anchors.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, field, asdict
from typing import Any
from urllib.parse import urlsplit, urlunsplit

# bs4 is already a runtime dependency of the production scraper workflow
# (see .github/workflows/nightly-scrape.yml). We use lxml-free html.parser
# for portability.
from bs4 import BeautifulSoup, Tag

# ────────────────────────────────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────────────────────────────────

RECENT_SALES_PARSER_VERSION = "recent_sales_parser@v1"
SCHEMA_VERSION = 1
SOURCE_ATTRIBUTION = "Source: PriceCharting recent sale"
DISPLAY_CURRENCY = "USD"

# Confidence cap for strong-identity rows
CONF_STRONG = 100
CONF_FALLBACK = 80
CONF_PARTIAL = 60

# Bracket label canonicalisation
_BRACKET_LABEL_TO_MARKETPLACE = {
    "ebay": "ebay",
    "tcgplayer": "tcgplayer",
    "heritageauctions": "heritage_auctions",
    "heritage auctions": "heritage_auctions",
    "heritage": "heritage_auctions",
    "pwcc": "pwcc",
}

# Anchor-class to marketplace
_ANCHOR_CLASS_TO_MARKETPLACE = {
    "js-ebay-completed-sale": "ebay",
    "js-tcgplayer-completed-sale": "tcgplayer",
    "js-ha-completed-sale": "heritage_auctions",
    "js-pwcc-completed-sale": "pwcc",
}

# Trusted hosts per marketplace — for cross-checks and country inference
_HOST_TO_MARKETPLACE = (
    (re.compile(r"(^|\.)ebay\.[a-z.]+$", re.I), "ebay"),
    (re.compile(r"(^|\.)tcgplayer\.com$", re.I), "tcgplayer"),
    (re.compile(r"(^|\.)ha\.com$", re.I), "heritage_auctions"),
    (re.compile(r"(^|\.)pwccmarketplace\.com$", re.I), "pwcc"),
    (re.compile(r"(^|\.)pwcc\.com$", re.I), "pwcc"),
)

# Observed eBay host suffixes → ISO country (best-effort, NEVER guessed when
# the host is bare ``ebay.com`` — that is recorded as 'unknown' country)
_EBAY_HOST_TO_COUNTRY = {
    "ebay.co.uk": "GB",
    "ebay.de": "DE",
    "ebay.fr": "FR",
    "ebay.it": "IT",
    "ebay.es": "ES",
    "ebay.com.au": "AU",
    "ebay.ca": "CA",
    "ebay.ie": "IE",
    "ebay.at": "AT",
    "ebay.ch": "CH",
    "ebay.be": "BE",
    "ebay.nl": "NL",
    "ebay.pl": "PL",
    "ebay.com.hk": "HK",
    "ebay.com.sg": "SG",
    "ebay.com.my": "MY",
    "ebay.ph": "PH",
    "ebay.com.tw": "TW",
}

# Section-class → section info (canonical map). Confidence reflects how
# unambiguous the grade attribution is on the PriceCharting UI: ungraded =
# 100, sections whose tab text is literally a brand-named grade (e.g.
# "PSA 10", "CGC 10") = 100, sections whose tab text is only "Grade N"
# without naming the company = 80.
@dataclass(frozen=True)
class SectionInfo:
    observed_section: str
    grading_company: str | None
    grade: str
    raw_or_graded: str  # "raw" | "graded"
    confidence: int


# Keys are the section-class suffix (the string after ``completed-auctions-``).
SECTION_MAP: dict[str, SectionInfo] = {
    "used":               SectionInfo("completed-auctions-used", None, "Ungraded", "raw", 100),
    "cib":                SectionInfo("completed-auctions-cib", None, "Grade 7", "graded", 80),
    "new":                SectionInfo("completed-auctions-new", None, "Grade 8", "graded", 80),
    "graded":             SectionInfo("completed-auctions-graded", None, "Grade 9", "graded", 80),
    "box-only":           SectionInfo("completed-auctions-box-only", None, "Grade 9.5", "graded", 80),
    "manual-only":        SectionInfo("completed-auctions-manual-only", "PSA", "PSA 10", "graded", 100),
    "loose-and-manual":   SectionInfo("completed-auctions-loose-and-manual", None, "Grade 1", "graded", 80),
    "box-and-manual":     SectionInfo("completed-auctions-box-and-manual", None, "Grade 2", "graded", 80),
    "grade-three":        SectionInfo("completed-auctions-grade-three", None, "Grade 3", "graded", 80),
    "grade-four":         SectionInfo("completed-auctions-grade-four", None, "Grade 4", "graded", 80),
    "grade-five":         SectionInfo("completed-auctions-grade-five", None, "Grade 5", "graded", 80),
    "grade-six":          SectionInfo("completed-auctions-grade-six", None, "Grade 6", "graded", 80),
    "loose-and-box":      SectionInfo("completed-auctions-loose-and-box", "BGS", "BGS 10", "graded", 100),
    "grade-seventeen":    SectionInfo("completed-auctions-grade-seventeen", "CGC", "CGC 10", "graded", 100),
    "grade-eighteen":     SectionInfo("completed-auctions-grade-eighteen", "SGC", "SGC 10", "graded", 100),
    "grade-nineteen":     SectionInfo("completed-auctions-grade-nineteen", "CGC", "CGC 10 Pristine", "graded", 100),
    "grade-twenty":       SectionInfo("completed-auctions-grade-twenty", "BGS", "BGS 10 Black", "graded", 100),
    "grade-twenty-one":   SectionInfo("completed-auctions-grade-twenty-one", "TAG", "TAG 10", "graded", 100),
    "grade-twenty-two":   SectionInfo("completed-auctions-grade-twenty-two", "ACE", "ACE 10", "graded", 100),
}


# ────────────────────────────────────────────────────────────────────────────
# Result types
# ────────────────────────────────────────────────────────────────────────────

@dataclass
class SaleRow:
    """One parsed sale. All values are primitive / JSON-friendly."""
    schema_version: int
    provider: str
    provider_card_id: str
    internal_card_slug: str
    pricecharting_url: str
    observed_section: str
    sale_date: str | None  # YYYY-MM-DD, None only on rejected rows
    marketplace_source: str
    marketplace_country: str | None
    listing_title: str | None
    sale_price_cents: int | None
    original_price_cents: int | None
    display_currency: str
    source_currency: str | None
    grading_company: str | None
    grade: str | None
    condition_text: str | None
    condition_bucket: str
    listing_url: str | None
    marketplace_item_id: str | None
    best_offer_status: str
    language: str
    first_edition_status: str
    variant_text: str | None
    provider_sale_key: str | None
    raw_hash: str
    parser_version: str
    parse_confidence: int
    parse_status: str
    rejection_reason: str | None
    anomaly_flags: list[str] = field(default_factory=list)
    source_attribution: str = SOURCE_ATTRIBUTION
    import_run_id: str | None = None
    # Limited debug payload — populated only for quarantined / rejected rows.
    raw_metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RecentSalesParseResult:
    rows: list[SaleRow]
    parse_status: str  # "ok" | "no_section" | "unsupported_layout" | "failed"
    section_detected: bool
    section_count: int
    row_count: int
    quarantined_count: int
    rejected_count: int
    layout_signature: str
    parser_version: str
    warnings: list[str] = field(default_factory=list)
    language: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["rows"] = [r.to_dict() if isinstance(r, SaleRow) else r for r in self.rows]
        return d


# ────────────────────────────────────────────────────────────────────────────
# Top-level entry point
# ────────────────────────────────────────────────────────────────────────────

def parse_recent_sales(
    html: str,
    *,
    page_url: str,
    provider_card_id: str,
    internal_card_slug: str,
    import_run_id: str | None = None,
    expected_card_number: str | None = None,
) -> RecentSalesParseResult:
    """
    Parse a PriceCharting card page's HTML for recent completed sales.

    The parser never raises on malformed HTML. Use the returned
    ``parse_status`` to decide whether to consume ``rows``.

    ``expected_card_number`` is optional. When supplied (e.g. ``"4"`` for
    Charizard #4/102), rows whose title carries a ``#NN`` or ``NN/NNN`` that
    disagrees are flagged ``wrong_card_number`` and quarantined. When
    omitted, no card-number cross-check is performed — the parser does not
    sniff card numbers from the ``internal_card_slug`` because that string
    in this repo is a PriceCharting *product* id, not a card number.

    Sealed status is intentionally NOT inferred here (see module docstring
    and docs/recent-sales-parser.md). If a downstream caller needs it,
    derive it from the ``cards`` table.
    """
    warnings: list[str] = []
    language = _infer_language(page_url)

    # Japanese pages use a different layout. We do not attempt to parse
    # English selectors against them; we surface this as a typed status.
    if language == "ja":
        sig = _compute_layout_signature_for_unsupported(html, language=language)
        return RecentSalesParseResult(
            rows=[],
            parse_status="unsupported_layout",
            section_detected=False,
            section_count=0,
            row_count=0,
            quarantined_count=0,
            rejected_count=0,
            layout_signature=sig,
            parser_version=RECENT_SALES_PARSER_VERSION,
            warnings=["language=ja; PriceCharting JP pages use an unsupported layout"],
            language=language,
        )

    try:
        soup = BeautifulSoup(html or "", "html.parser")
    except Exception as e:  # pragma: no cover — bs4 with html.parser is robust
        return RecentSalesParseResult(
            rows=[],
            parse_status="failed",
            section_detected=False,
            section_count=0,
            row_count=0,
            quarantined_count=0,
            rejected_count=0,
            layout_signature="",
            parser_version=RECENT_SALES_PARSER_VERSION,
            warnings=[f"bs4 parse error: {e!r}"],
            language=language,
        )

    layout_signature = _compute_layout_signature(soup, language=language)

    # Find every <div class="completed-auctions-XXX"> that actually contains
    # a sortable rows-table. The class set on the div is exactly one of the
    # section slugs in SECTION_MAP plus possibly other helper classes.
    section_divs = _find_section_divs(soup)

    if not section_divs:
        return RecentSalesParseResult(
            rows=[],
            parse_status="no_section",
            section_detected=False,
            section_count=0,
            row_count=0,
            quarantined_count=0,
            rejected_count=0,
            layout_signature=layout_signature,
            parser_version=RECENT_SALES_PARSER_VERSION,
            warnings=warnings,
            language=language,
        )

    canonical_url = _strip_url(page_url) or page_url
    rows: list[SaleRow] = []
    quarantined = 0
    rejected = 0
    seen_keys: set[str] = set()

    for div in section_divs:
        section_slug, section_info = _classify_section(div)
        if section_info is None:
            warnings.append(f"unknown_section_class: {section_slug}")
            # Still attempt to parse rows so the operator sees the data; mark
            # all such rows as quarantined.
        table = div.find("table", class_=lambda c: c and "hoverable-rows" in c and "sortable" in c)
        if not isinstance(table, Tag):
            continue
        tbody = table.find("tbody")
        if not isinstance(tbody, Tag):
            continue
        for tr in tbody.find_all("tr", recursive=False):
            if not isinstance(tr, Tag):
                continue
            row = _parse_row(
                tr=tr,
                section_slug=section_slug,
                section_info=section_info,
                provider_card_id=provider_card_id,
                internal_card_slug=internal_card_slug,
                pricecharting_url=canonical_url,
                language=language,
                import_run_id=import_run_id,
                expected_card_number=expected_card_number,
            )
            if row is None:
                continue
            # In-batch dedup — strong keys collapse re-emitted rows; fallback
            # hash keys collapse exact-duplicate rows within the same response.
            dedup_key = row.provider_sale_key or row.raw_hash
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            if row.parse_status == "quarantined":
                quarantined += 1
            elif row.parse_status == "rejected":
                rejected += 1
            rows.append(row)

    return RecentSalesParseResult(
        rows=rows,
        parse_status="ok",
        section_detected=True,
        section_count=len(section_divs),
        row_count=len(rows),
        quarantined_count=quarantined,
        rejected_count=rejected,
        layout_signature=layout_signature,
        parser_version=RECENT_SALES_PARSER_VERSION,
        warnings=warnings,
        language=language,
    )


# ────────────────────────────────────────────────────────────────────────────
# Section detection
# ────────────────────────────────────────────────────────────────────────────

_SECTION_CLASS_RE = re.compile(r"^completed-auctions-(?P<slug>[a-z0-9\-]+)$")


def _find_section_divs(soup: BeautifulSoup) -> list[Tag]:
    """Return every wrapper div that holds a recent-sales table."""
    out: list[Tag] = []
    for div in soup.find_all("div", class_=True):
        if not isinstance(div, Tag):
            continue
        classes = div.get("class") or []
        # We want a wrapper whose class is *exactly* a section slug, not the
        # tab UI elements (which carry additional "tab available" classes).
        # The wrapper div on PriceCharting today has the section class as the
        # *only* class.
        section_classes = [c for c in classes if _SECTION_CLASS_RE.match(c)]
        if not section_classes:
            continue
        # Skip the tab elements: they live inside <div id="tab-bar"> and
        # carry "tab" among their classes.
        if "tab" in classes:
            continue
        # The wrapper must actually contain the sales table.
        table = div.find("table", class_=lambda c: c and "hoverable-rows" in c and "sortable" in c)
        if table is None:
            continue
        out.append(div)
    return out


def _classify_section(div: Tag) -> tuple[str, SectionInfo | None]:
    classes = div.get("class") or []
    for c in classes:
        m = _SECTION_CLASS_RE.match(c)
        if not m:
            continue
        slug = m.group("slug")
        return slug, SECTION_MAP.get(slug)
    return ("", None)


# ────────────────────────────────────────────────────────────────────────────
# Row parsing
# ────────────────────────────────────────────────────────────────────────────

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_PRICE_RE = re.compile(r"\$\s*(-?[\d,]+\.\d{2}|-?[\d,]+)")
_BRACKET_LABEL_RE = re.compile(r"\[([^\]]+)\]")
_EBAY_TR_ID_RE = re.compile(r"^ebay-(\d+)$", re.I)
_HA_TR_ID_RE = re.compile(r"^ha-([0-9a-zA-Z_\-]+)$")
_PWCC_TR_ID_RE = re.compile(r"^pwcc-([0-9a-zA-Z_\-]+)$", re.I)
_TCG_TR_ID_RE = re.compile(r"^tcgplayer-([0-9a-zA-Z_\-]+)$", re.I)
_EBAY_URL_ITEM_RE = re.compile(r"/itm/(?:[^/?#]*?-)?(\d{6,})(?:[/?#]|$)")

_LOT_KEYWORDS = (
    "lot of", "bundle of", "x10", "x20", " repack", " mystery",
    "collection lot", "binder full",
)
_PROXY_KEYWORDS = (
    "proxy", "replica", "fake ", " fake", "custom card", "custom print",
    "metal card", "metal pokemon", "gold metal", "gold plated",
)
# Characters that are unambiguously non-Latin script
_NON_LATIN_RE = re.compile(r"[぀-ヿ一-鿿㐀-䶿가-힯]")

_CONDITION_TOKENS = (
    ("near_mint",          (" nm ", " nm/", "near mint", "near-mint")),
    ("lightly_played",     (" lp ", "lightly played", "light play")),
    ("moderately_played",  (" mp ", "moderately played")),
    ("heavily_played",     (" hp ", "heavily played", "heavy play")),
    ("damaged",            ("damaged", " dmg ", "poor condition", "creased", "water damage")),
)

_FIRST_ED_RE = re.compile(r"\b(1st edition|first edition|1st ed\.?)\b", re.I)
_UNLIMITED_RE = re.compile(r"\bunlimited\b", re.I)
_SHADOWLESS_RE = re.compile(r"\bshadowless\b", re.I)


def _parse_row(
    *,
    tr: Tag,
    section_slug: str,
    section_info: SectionInfo | None,
    provider_card_id: str,
    internal_card_slug: str,
    pricecharting_url: str,
    language: str,
    import_run_id: str | None,
    expected_card_number: str | None = None,
) -> SaleRow | None:
    """
    Parse one ``<tr>`` from a sales-table tbody.

    Returns ``None`` when the row is not a sale row at all (e.g. unrelated
    nested table content). For sale-like rows that are malformed, returns
    a row with parse_status='quarantined' or 'rejected'.
    """
    anomaly_flags: list[str] = []
    rejection_reason: str | None = None
    raw_metadata: dict[str, Any] | None = None

    # ── Date
    date_td = tr.find("td", class_="date")
    if not isinstance(date_td, Tag):
        # Skip header rows / decorative rows that aren't sale rows
        return None
    sale_date_raw = date_td.get_text(strip=True)
    if not _DATE_RE.match(sale_date_raw):
        # Date is structurally required — reject.
        return _make_rejected(
            tr=tr, section_slug=section_slug, section_info=section_info,
            provider_card_id=provider_card_id, internal_card_slug=internal_card_slug,
            pricecharting_url=pricecharting_url, language=language,
            import_run_id=import_run_id, reason="malformed_date",
            sale_date_raw=sale_date_raw,
        )
    sale_date = sale_date_raw

    # ── Title + URL + marketplace anchor
    title_td = tr.find("td", class_="title")
    title_anchor: Tag | None = None
    raw_label_text = ""
    if isinstance(title_td, Tag):
        title_anchor = title_td.find("a")
        if not isinstance(title_anchor, Tag):
            title_anchor = None
        raw_label_text = title_td.get_text(" ", strip=True)

    raw_href = (title_anchor.get("href") if title_anchor else None) or None
    listing_title = (title_anchor.get_text(strip=True) if title_anchor else None) or None

    # ── Marketplace detection (3 signals, with cross-check)
    anchor_class_marketplace: str | None = None
    if title_anchor is not None:
        for c in (title_anchor.get("class") or []):
            if c in _ANCHOR_CLASS_TO_MARKETPLACE:
                anchor_class_marketplace = _ANCHOR_CLASS_TO_MARKETPLACE[c]
                break

    bracket_marketplace: str | None = None
    bracket_match = _BRACKET_LABEL_RE.search(raw_label_text)
    if bracket_match:
        bracket_marketplace = _BRACKET_LABEL_TO_MARKETPLACE.get(
            bracket_match.group(1).strip().lower()
        )

    host_marketplace, host = _host_marketplace(raw_href)

    # Decide canonical marketplace + detect conflict
    signals = [s for s in (anchor_class_marketplace, bracket_marketplace, host_marketplace) if s]
    distinct_signals = set(signals)
    marketplace_source = "unknown"
    conflicting = False
    if not signals:
        marketplace_source = "unknown"
    elif len(distinct_signals) == 1:
        marketplace_source = next(iter(distinct_signals))
    else:
        # Conflict — quarantine after we extract the rest.
        conflicting = True
        # Prefer the anchor-class signal (the most structurally-bound one).
        marketplace_source = anchor_class_marketplace or bracket_marketplace or host_marketplace or "unknown"

    # ── Listing URL normalisation
    listing_url, url_rejection = _normalise_listing_url(raw_href)
    if url_rejection == "unsafe_scheme":
        return _make_rejected(
            tr=tr, section_slug=section_slug, section_info=section_info,
            provider_card_id=provider_card_id, internal_card_slug=internal_card_slug,
            pricecharting_url=pricecharting_url, language=language,
            import_run_id=import_run_id, reason="unsafe_url",
            raw_href=raw_href,
        )

    # ── Marketplace item ID
    marketplace_item_id = _extract_item_id(
        tr=tr, marketplace=marketplace_source, raw_href=raw_href,
    )

    # ── Marketplace country
    marketplace_country = _marketplace_country(marketplace_source, host)

    # ── Prices
    price_result = _extract_prices(tr)
    if price_result.error == "missing_final":
        return _make_rejected(
            tr=tr, section_slug=section_slug, section_info=section_info,
            provider_card_id=provider_card_id, internal_card_slug=internal_card_slug,
            pricecharting_url=pricecharting_url, language=language,
            import_run_id=import_run_id, reason="missing_final_price",
        )
    if price_result.error == "invalid_final":
        return _make_rejected(
            tr=tr, section_slug=section_slug, section_info=section_info,
            provider_card_id=provider_card_id, internal_card_slug=internal_card_slug,
            pricecharting_url=pricecharting_url, language=language,
            import_run_id=import_run_id, reason="invalid_final_price",
            sale_price_raw=price_result.raw_text,
        )

    sale_price_cents = price_result.sale_price_cents
    original_price_cents = price_result.original_price_cents
    best_offer_status = price_result.best_offer_status

    # ── Title-driven quarantine signals
    title_lower = (listing_title or "").lower()
    if listing_title is None:
        anomaly_flags.append("missing_title")
    if any(k in title_lower for k in _LOT_KEYWORDS):
        anomaly_flags.append("lot_or_bundle")
    if any(k in title_lower for k in _PROXY_KEYWORDS):
        anomaly_flags.append("proxy_or_reprint")
    if language == "en" and listing_title and _NON_LATIN_RE.search(listing_title):
        anomaly_flags.append("wrong_language")
    if expected_card_number and _check_wrong_card_number(expected_card_number, listing_title):
        anomaly_flags.append("wrong_card_number")

    if conflicting:
        anomaly_flags.append("conflicting_marketplace")
        raw_metadata = {
            "anchor_class_marketplace": anchor_class_marketplace,
            "bracket_marketplace": bracket_marketplace,
            "host_marketplace": host_marketplace,
            "raw_href": raw_href,
            "raw_label_text": raw_label_text,
        }

    # ── Condition / variant parsing
    condition_text, condition_bucket = _classify_condition(
        title_lower=title_lower, section_info=section_info,
    )
    first_edition_status = _first_edition_status(title_lower)
    variant_text = _variant_text(listing_title or "")

    # ── Section resolution
    if section_info is None:
        # Unknown section — keep the row visible but quarantine.
        observed_section = f"completed-auctions-{section_slug}" if section_slug else "completed-auctions-unknown"
        grading_company = None
        grade = None
        anomaly_flags.append("unknown_section")
    else:
        observed_section = section_info.observed_section
        grading_company = section_info.grading_company
        grade = section_info.grade

    # ── Identity
    normalised_title = _normalise_title(listing_title or "")
    if marketplace_item_id and marketplace_source != "unknown":
        provider_sale_key = _sha256(
            "pricecharting", provider_card_id, marketplace_source,
            marketplace_item_id, observed_section,
        )
        parse_confidence = CONF_STRONG
    else:
        provider_sale_key = _sha256(
            "pricecharting", provider_card_id, sale_date, marketplace_source,
            observed_section, str(sale_price_cents), normalised_title,
        )
        parse_confidence = CONF_FALLBACK

    raw_hash = _sha256(
        "pricecharting", provider_card_id, observed_section,
        sale_date, marketplace_source, marketplace_country or "",
        str(sale_price_cents), str(original_price_cents) if original_price_cents else "",
        marketplace_item_id or "", listing_url or "", listing_title or "",
        best_offer_status, first_edition_status, variant_text or "",
        condition_text or "", condition_bucket,
        " ".join(sorted(anomaly_flags)),
    )

    # ── Quality classification
    quarantine_reasons: list[str] = []
    if marketplace_source == "unknown":
        quarantine_reasons.append("missing_marketplace")
    if section_info is None:
        quarantine_reasons.append("unknown_section_class")
    if anomaly_flags:
        # Anomaly flags that classify-as-quarantine
        for flag in (
            "lot_or_bundle", "proxy_or_reprint", "wrong_language",
            "wrong_card_number", "conflicting_marketplace", "missing_title",
        ):
            if flag in anomaly_flags:
                quarantine_reasons.append(flag)

    if quarantine_reasons:
        parse_status = "quarantined"
        rejection_reason = ",".join(sorted(set(quarantine_reasons)))
        parse_confidence = min(parse_confidence, CONF_PARTIAL)
        if raw_metadata is None:
            raw_metadata = {
                "raw_href": raw_href,
                "raw_label_text": raw_label_text,
            }
    else:
        parse_status = "ok"

    return SaleRow(
        schema_version=SCHEMA_VERSION,
        provider="pricecharting",
        provider_card_id=provider_card_id,
        internal_card_slug=internal_card_slug,
        pricecharting_url=pricecharting_url,
        observed_section=observed_section,
        sale_date=sale_date,
        marketplace_source=marketplace_source,
        marketplace_country=marketplace_country,
        listing_title=listing_title,
        sale_price_cents=sale_price_cents,
        original_price_cents=original_price_cents,
        display_currency=DISPLAY_CURRENCY,
        source_currency=None,
        grading_company=grading_company,
        grade=grade,
        condition_text=condition_text,
        condition_bucket=condition_bucket,
        listing_url=listing_url,
        marketplace_item_id=marketplace_item_id,
        best_offer_status=best_offer_status,
        language=language,
        first_edition_status=first_edition_status,
        variant_text=variant_text,
        provider_sale_key=provider_sale_key,
        raw_hash=raw_hash,
        parser_version=RECENT_SALES_PARSER_VERSION,
        parse_confidence=parse_confidence,
        parse_status=parse_status,
        rejection_reason=rejection_reason,
        anomaly_flags=anomaly_flags,
        source_attribution=SOURCE_ATTRIBUTION,
        import_run_id=import_run_id,
        raw_metadata=raw_metadata,
    )


def _make_rejected(
    *,
    tr: Tag,
    section_slug: str,
    section_info: SectionInfo | None,
    provider_card_id: str,
    internal_card_slug: str,
    pricecharting_url: str,
    language: str,
    import_run_id: str | None,
    reason: str,
    **debug: Any,
) -> SaleRow:
    """Build a typed rejected SaleRow for an unparseable input row."""
    observed_section = (
        section_info.observed_section if section_info
        else (f"completed-auctions-{section_slug}" if section_slug else "completed-auctions-unknown")
    )
    raw_hash = _sha256("rejected", provider_card_id, observed_section, reason, str(debug))
    return SaleRow(
        schema_version=SCHEMA_VERSION,
        provider="pricecharting",
        provider_card_id=provider_card_id,
        internal_card_slug=internal_card_slug,
        pricecharting_url=pricecharting_url,
        observed_section=observed_section,
        sale_date=None,
        marketplace_source="unknown",
        marketplace_country=None,
        listing_title=None,
        sale_price_cents=None,
        original_price_cents=None,
        display_currency=DISPLAY_CURRENCY,
        source_currency=None,
        grading_company=section_info.grading_company if section_info else None,
        grade=section_info.grade if section_info else None,
        condition_text=None,
        condition_bucket="unknown",
        listing_url=None,
        marketplace_item_id=None,
        best_offer_status="unknown",
        language=language,
        first_edition_status="unknown",
        variant_text=None,
        provider_sale_key=None,
        raw_hash=raw_hash,
        parser_version=RECENT_SALES_PARSER_VERSION,
        parse_confidence=0,
        parse_status="rejected",
        rejection_reason=reason,
        anomaly_flags=[],
        source_attribution=SOURCE_ATTRIBUTION,
        import_run_id=import_run_id,
        raw_metadata={"reason": reason, **debug},
    )


# ────────────────────────────────────────────────────────────────────────────
# Price extraction
# ────────────────────────────────────────────────────────────────────────────

@dataclass
class _PriceParse:
    sale_price_cents: int | None
    original_price_cents: int | None
    best_offer_status: str  # "not_best_offer" | "accepted" | "unknown"
    raw_text: str | None
    error: str | None


def _extract_prices(tr: Tag) -> _PriceParse:
    """
    Extract final / optional list price out of the row.

    Strategy (driven by Block 4A-S audit):

    1. Look for ``<span class="js-price" title="best offer accepted price">``
       inside any ``<td>`` of this row. If present, that is the FINAL price
       and ``best_offer_status='accepted'``.
    2. The list price (crossed-out) comes from ``title="best offer list price"``.
    3. Otherwise: the final price is the first ``<span class="js-price">``
       inside ``<td class="numeric">`` that is not also class
       ``listed-price-inline``. ``original_price_cents`` stays None.
    """
    accepted_span: Tag | None = None
    list_span: Tag | None = None
    plain_span: Tag | None = None

    # Limit scope: only price cells.
    numeric_tds = [
        td for td in tr.find_all("td", recursive=False)
        if isinstance(td, Tag) and "numeric" in (td.get("class") or [])
    ]
    for td in numeric_tds:
        for span in td.find_all("span", class_="js-price"):
            if not isinstance(span, Tag):
                continue
            title = (span.get("title") or "").strip().lower()
            classes = span.get("class") or []
            if title == "best offer accepted price":
                accepted_span = accepted_span or span
            elif title == "best offer list price":
                list_span = list_span or span
            elif "listed-price-inline" in classes:
                # inline list price (shown next to the accepted price); same
                # semantics as title="best offer list price".
                list_span = list_span or span
            else:
                plain_span = plain_span or span

    raw_text: str | None = None
    if accepted_span is not None:
        final_cents, raw_text = _to_cents(accepted_span.get_text(strip=True))
        list_cents: int | None = None
        if list_span is not None:
            list_cents, _ = _to_cents(list_span.get_text(strip=True))
        if final_cents is None:
            return _PriceParse(None, None, "unknown", raw_text, "invalid_final")
        if final_cents <= 0:
            return _PriceParse(None, None, "unknown", raw_text, "invalid_final")
        if list_cents is not None and list_cents <= 0:
            list_cents = None
        return _PriceParse(final_cents, list_cents, "accepted", raw_text, None)

    if plain_span is not None:
        final_cents, raw_text = _to_cents(plain_span.get_text(strip=True))
        if final_cents is None:
            return _PriceParse(None, None, "unknown", raw_text, "invalid_final")
        if final_cents <= 0:
            return _PriceParse(None, None, "unknown", raw_text, "invalid_final")
        return _PriceParse(final_cents, None, "not_best_offer", raw_text, None)

    return _PriceParse(None, None, "unknown", None, "missing_final")


def _to_cents(text: str) -> tuple[int | None, str | None]:
    if not text:
        return None, None
    raw = text.strip()
    m = _PRICE_RE.search(raw)
    if not m:
        return None, raw
    digits = m.group(1).replace(",", "")
    try:
        value = float(digits)
    except ValueError:
        return None, raw
    return round(value * 100), raw


# ────────────────────────────────────────────────────────────────────────────
# Marketplace / URL / item-id helpers
# ────────────────────────────────────────────────────────────────────────────

def _host_marketplace(raw_href: str | None) -> tuple[str | None, str | None]:
    if not raw_href:
        return None, None
    try:
        parts = urlsplit(raw_href)
    except ValueError:
        return None, None
    host = (parts.hostname or "").lower()
    if not host:
        return None, None
    for pattern, marketplace in _HOST_TO_MARKETPLACE:
        if pattern.search(host):
            return marketplace, host
    return None, host


def _marketplace_country(marketplace: str, host: str | None) -> str | None:
    """
    Best-effort marketplace country.

    NEVER guess US from a bare ``ebay.com`` host — the audit explicitly calls
    this out as unreliable (PC localises affiliate links by geo). Return
    None for anything ambiguous.
    """
    if marketplace != "ebay" or not host:
        return None
    h = host[4:] if host.startswith("www.") else host
    return _EBAY_HOST_TO_COUNTRY.get(h)  # None for ebay.com et al


def _normalise_listing_url(raw_href: str | None) -> tuple[str | None, str | None]:
    """
    Strip affiliate / tracking params; keep scheme + host + path only.

    Returns ``(normalised_url, error)`` where error is None on success or
    "unsafe_scheme" when the URL is non-http(s).
    """
    if not raw_href:
        return None, None
    try:
        parts = urlsplit(raw_href.strip())
    except ValueError:
        return None, None
    scheme = (parts.scheme or "").lower()
    if scheme not in ("http", "https"):
        return None, "unsafe_scheme"
    if not parts.hostname:
        return None, None
    cleaned = urlunsplit((scheme, parts.netloc.lower(), parts.path, "", ""))
    return cleaned, None


def _extract_item_id(*, tr: Tag, marketplace: str, raw_href: str | None) -> str | None:
    """Item ID extraction by marketplace, with URL-path fallback for eBay."""
    tr_id = (tr.get("id") or "").strip()
    if marketplace == "ebay":
        m = _EBAY_TR_ID_RE.match(tr_id)
        if m:
            return m.group(1)
        if raw_href:
            try:
                path = urlsplit(raw_href).path
            except ValueError:
                path = ""
            m2 = _EBAY_URL_ITEM_RE.search(path)
            if m2:
                return m2.group(1)
        return None
    if marketplace == "heritage_auctions":
        m = _HA_TR_ID_RE.match(tr_id)
        return m.group(1) if m else None
    if marketplace == "pwcc":
        m = _PWCC_TR_ID_RE.match(tr_id)
        return m.group(1) if m else None
    if marketplace == "tcgplayer":
        m = _TCG_TR_ID_RE.match(tr_id)
        return m.group(1) if m else None
    return None


# ────────────────────────────────────────────────────────────────────────────
# Title parsing — condition / variant / language
# ────────────────────────────────────────────────────────────────────────────

def _classify_condition(*, title_lower: str, section_info: SectionInfo | None) -> tuple[str | None, str]:
    """
    Condition extraction is advisory only. The section remains authoritative
    for graded vs raw; we never override a graded section.
    """
    matched: str | None = None
    bucket: str = "unknown"
    for label, tokens in _CONDITION_TOKENS:
        for t in tokens:
            # Pad with spaces so we don't match inside e.g. "nmint"
            if t.startswith(" ") or t.endswith(" "):
                token_needle = t
            else:
                token_needle = t
            if token_needle in title_lower:
                matched = matched or token_needle.strip()
                bucket = label
                break
        if matched:
            break

    if section_info is not None and section_info.raw_or_graded == "graded":
        # Section trumps title for the bucket; preserve the matched text.
        return (matched, "graded")
    if section_info is not None and section_info.raw_or_graded == "raw":
        if bucket == "unknown":
            return (None, "raw_unknown")
        return (matched, bucket)
    return (matched, bucket)


def _first_edition_status(title_lower: str) -> str:
    if _FIRST_ED_RE.search(title_lower):
        return "first_edition"
    if _UNLIMITED_RE.search(title_lower):
        return "unlimited"
    return "unknown"


def _variant_text(title: str) -> str | None:
    """Capture bracketed variant text plus shadowless mention."""
    bits: list[str] = []
    for m in re.finditer(r"\[([^\]]+)\]", title):
        bits.append(m.group(1).strip())
    if _SHADOWLESS_RE.search(title):
        if not any(b.lower() == "shadowless" for b in bits):
            bits.append("Shadowless")
    if not bits:
        return None
    return "; ".join(bits)


def _check_wrong_card_number(expected_card_number: str, title: str | None) -> bool:
    """
    Cross-check the title's ``#NN`` or ``NN/NNN`` against the explicitly
    supplied expected card number.

    Returns True only when a clear mismatch is found. If the title contains
    neither pattern, returns False (silent — downstream can flag missingness).
    Numeric comparison strips leading zeros and any trailing letter suffix
    so ``#85``, ``#085`` and ``085/108`` all reconcile to the same card.
    """
    if not title or not expected_card_number:
        return False
    title_num: str | None = None
    m = re.search(r"#(\d{1,4})([A-Za-z]*)", title)
    if m:
        title_num = m.group(1)
    else:
        m2 = re.search(r"\b(\d{1,4})([A-Za-z]?)/\d{1,4}\b", title)
        if m2:
            title_num = m2.group(1)
    if not title_num:
        return False

    def _norm(n: str) -> str:
        return n.lstrip("0") or "0"

    return _norm(title_num) != _norm(expected_card_number)


def _normalise_title(title: str) -> str:
    """
    NFKC + lowercase + collapse whitespace + normalise punctuation spacing.

    Preserves meaningful card numbers (``#4``, ``4/102``). Does not blindly
    strip punctuation.
    """
    if not title:
        return ""
    s = unicodedata.normalize("NFKC", title)
    s = s.lower().strip()
    # Normalise spacing around punctuation we expect to keep.
    s = re.sub(r"\s+", " ", s)
    # Pad slashes and hashes so two whitespace-different titles collapse.
    s = re.sub(r"\s*#\s*", " # ", s)
    s = re.sub(r"\s*/\s*", "/", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ────────────────────────────────────────────────────────────────────────────
# Layout signature
# ────────────────────────────────────────────────────────────────────────────

def _compute_layout_signature(soup: BeautifulSoup, *, language: str) -> str:
    """
    A signature over the *structural* features that prove the parser still
    matches the page, NOT the data itself. Two responses for the same card on
    consecutive days should yield the same signature.
    """
    section_class_slugs: set[str] = set()
    sale_anchor_classes: set[str] = set()
    row_cell_classes: set[str] = set()
    has_completed_auctions_anchor = bool(soup.find("a", attrs={"name": "completed-auctions"}))
    has_full_prices = bool(soup.find("div", id="full-prices"))

    for div in soup.find_all("div", class_=True):
        if not isinstance(div, Tag):
            continue
        classes = div.get("class") or []
        for c in classes:
            m = _SECTION_CLASS_RE.match(c)
            if m and "tab" not in classes:
                section_class_slugs.add(m.group("slug"))

    for a in soup.find_all("a", class_=True):
        if not isinstance(a, Tag):
            continue
        for c in (a.get("class") or []):
            if c in _ANCHOR_CLASS_TO_MARKETPLACE:
                sale_anchor_classes.add(c)

    # Only inspect cells that live inside a sortable rows-table.
    for table in soup.find_all("table"):
        if not isinstance(table, Tag):
            continue
        tcls = table.get("class") or []
        if "hoverable-rows" not in tcls or "sortable" not in tcls:
            continue
        for td in table.find_all("td"):
            if not isinstance(td, Tag):
                continue
            for c in (td.get("class") or []):
                row_cell_classes.add(c)

    payload = "|".join((
        f"lang={language}",
        f"anchor={int(has_completed_auctions_anchor)}",
        f"full_prices={int(has_full_prices)}",
        "section_classes=" + ",".join(sorted(section_class_slugs)),
        "anchor_classes=" + ",".join(sorted(sale_anchor_classes)),
        "row_cell_classes=" + ",".join(sorted(row_cell_classes)),
    ))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _compute_layout_signature_for_unsupported(html: str, *, language: str) -> str:
    """
    Cheap signature for unsupported pages. Avoids a full bs4 parse on
    pages we are not going to read; uses a regex peek for the presence
    of the section anchor only.
    """
    has_anchor = bool(re.search(r'<a\s+name="completed-auctions"', html or ""))
    has_full_prices = bool(re.search(r'<div[^>]+id="full-prices"', html or ""))
    has_chart = bool(re.search(r'VGPC\.chart_data', html or ""))
    payload = "|".join((
        f"lang={language}",
        f"anchor={int(has_anchor)}",
        f"full_prices={int(has_full_prices)}",
        f"chart={int(has_chart)}",
        "unsupported",
    ))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ────────────────────────────────────────────────────────────────────────────
# Identity
# ────────────────────────────────────────────────────────────────────────────

def _sha256(*parts: str) -> str:
    h = hashlib.sha256()
    for i, p in enumerate(parts):
        if i:
            h.update(b"|")
        h.update((p or "").encode("utf-8"))
    return h.hexdigest()


# ────────────────────────────────────────────────────────────────────────────
# Misc helpers
# ────────────────────────────────────────────────────────────────────────────

_LANG_EN_RE = re.compile(r"^/game/pokemon-", re.I)
_LANG_JA_RE = re.compile(r"^/game/jp-pokemon-", re.I)


def _infer_language(page_url: str) -> str:
    """Language inference is strictly URL-based — never title-based."""
    if not page_url:
        return "unknown"
    try:
        parts = urlsplit(page_url)
    except ValueError:
        return "unknown"
    path = parts.path or ""
    if _LANG_JA_RE.match(path):
        return "ja"
    if _LANG_EN_RE.match(path):
        return "en"
    return "unknown"


def _strip_url(url: str) -> str | None:
    """Best-effort canonical form for the pricecharting_url echo field."""
    if not url:
        return None
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return None
    if not parts.scheme or not parts.hostname:
        return None
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, "", ""))
