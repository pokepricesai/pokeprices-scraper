"""
Tests for recent_sales_parser.py — Block 4A-S1.

All scenarios are exercised via either:
  - a real PriceCharting HTML fixture under ``tests/fixtures/recent_sales/``,
    or
  - a tiny hand-built HTML snippet wrapped by ``_wrap_section()``.

No live HTTP requests are made.
"""
from __future__ import annotations

import os
from textwrap import dedent

import pytest

import recent_sales_parser as p
from recent_sales_parser import (
    RECENT_SALES_PARSER_VERSION,
    SCHEMA_VERSION,
    SOURCE_ATTRIBUTION,
    SECTION_MAP,
    parse_recent_sales,
)

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "recent_sales")


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────

def _fixture(name: str) -> str:
    with open(os.path.join(FIXTURE_DIR, name), encoding="utf-8") as f:
        return f.read()


def _wrap_section(section_class: str, tbody_rows_html: str) -> str:
    """Build a minimal page with one completed-auctions section + rows."""
    return dedent(f"""
        <html><body>
        <a name="completed-auctions"></a>
        <div class="{section_class}" style="display:block;">
          <table class="hoverable-rows sortable">
            <thead><tr><th>Date</th><th>TW</th><th>Title</th><th>Price</th><th></th></tr></thead>
            <tbody>
              {tbody_rows_html}
            </tbody>
          </table>
        </div>
        </body></html>
    """)


def _ebay_row(
    *,
    item_id: str | None,
    date: str = "2026-06-01",
    title: str = "Generic eBay Pokemon listing",
    price_html: str = '<span class="js-price">$100.00</span>',
    listed_price_td: str = "",
    href_override: str | None = None,
    anchor_class: str = "js-ebay-completed-sale",
    label: str = "[eBay]",
    host: str = "ebay.co.uk",
) -> str:
    tr_id = f' id="ebay-{item_id}"' if item_id else ""
    href = href_override if href_override is not None else (
        f"https://www.{host}/itm/{item_id or '111111111111'}"
        "?nordt=true&mkevt=1&campid=5338999485&customid=noId&toolid=10001"
    )
    return f"""
    <tr{tr_id}>
      <td class="date">{date}</td>
      <td class="image"></td>
      <td class="title">
        <a target="_blank" class="{anchor_class}" href="{href}">{title}</a>
        {label}
      </td>
      <td class="numeric">{price_html}</td>
      <td class="numeric listed-price">{listed_price_td}</td>
      <td class="thumb-down"></td>
    </tr>
    """


PAGE_URL_EN = "https://www.pricecharting.com/game/pokemon-base-set/charizard-1st-edition-4"


def _parse(html: str, **kwargs):
    defaults = dict(
        page_url=PAGE_URL_EN,
        provider_card_id="715593",
        internal_card_slug="pc-715593",
    )
    defaults.update(kwargs)
    return parse_recent_sales(html, **defaults)


# ────────────────────────────────────────────────────────────────────────────
# Constants / module surface
# ────────────────────────────────────────────────────────────────────────────

def test_module_constants_exposed():
    assert RECENT_SALES_PARSER_VERSION == "recent_sales_parser@v1"
    assert SCHEMA_VERSION == 1
    assert SOURCE_ATTRIBUTION


def test_section_map_covers_every_required_grade():
    required_grades = {
        "Ungraded", "Grade 1", "Grade 2", "Grade 3", "Grade 4", "Grade 5",
        "Grade 6", "Grade 7", "Grade 8", "Grade 9", "Grade 9.5", "PSA 10",
        "CGC 10", "CGC 10 Pristine", "BGS 10", "BGS 10 Black",
        "SGC 10", "TAG 10", "ACE 10",
    }
    mapped = {info.grade for info in SECTION_MAP.values()}
    missing = required_grades - mapped
    assert not missing, f"section map missing grades: {missing}"


# ────────────────────────────────────────────────────────────────────────────
# Real fixtures
# ────────────────────────────────────────────────────────────────────────────

def test_fixture_high_volume_vintage():
    r = _parse(_fixture("base_charizard_1st.html"),
               page_url="https://www.pricecharting.com/game/pokemon-base-set/charizard-1st-edition-4",
               expected_card_number="4")
    assert r.parse_status == "ok"
    assert r.language == "en"
    assert r.section_count >= 10
    assert r.row_count >= 300
    # Marketplaces audit-confirmed on this page
    mkts = {row.marketplace_source for row in r.rows}
    assert {"ebay", "tcgplayer", "heritage_auctions", "pwcc"}.issubset(mkts)
    # PSA 10 section
    psa10_rows = [row for row in r.rows if row.observed_section == "completed-auctions-manual-only"]
    assert psa10_rows
    assert all(row.grading_company == "PSA" and row.grade == "PSA 10" for row in psa10_rows)


def test_fixture_high_volume_modern_chase():
    r = _parse(_fixture("svp_pikachu_holo.html"),
               page_url="https://www.pricecharting.com/game/pokemon-promo/pikachu-with-grey-felt-hat-085",
               expected_card_number="85")
    assert r.parse_status == "ok"
    assert r.section_count >= 5
    assert r.row_count > 100


def test_fixture_modern_sv():
    r = _parse(_fixture("obsidian_charizard_ex.html"),
               page_url="https://www.pricecharting.com/game/pokemon-obsidian-flames/charizard-ex-125",
               expected_card_number="125")
    assert r.parse_status == "ok"


def test_fixture_sealed_product():
    r = _parse(_fixture("base_booster_box.html"),
               page_url="https://www.pricecharting.com/game/pokemon-base-set/booster-box")
    assert r.parse_status == "ok"
    # Sealed pages expose only the Ungraded section
    assert {row.observed_section for row in r.rows} == {"completed-auctions-used"}
    assert r.row_count > 0


def test_fixture_sparse_no_sales():
    r = _parse(_fixture("pop3_celebi.html"),
               page_url="https://www.pricecharting.com/game/pokemon-pop-series-3/celebi-3")
    assert r.parse_status == "no_section"
    assert r.section_count == 0
    assert r.row_count == 0


def test_fixture_japanese_unsupported_layout():
    r = _parse(_fixture("jp_promo_pikachu.html"),
               page_url="https://www.pricecharting.com/game/jp-pokemon-promo/pikachu-001smp")
    assert r.parse_status == "unsupported_layout"
    assert r.language == "ja"
    assert r.rows == []
    assert r.warnings  # language warning emitted
    # Layout signature must still be deterministic + non-empty
    assert len(r.layout_signature) == 64


def test_fixture_layout_signature_is_stable_across_parse_calls():
    html = _fixture("base_booster_box.html")
    a = _parse(html, page_url="https://www.pricecharting.com/game/pokemon-base-set/booster-box")
    b = _parse(html, page_url="https://www.pricecharting.com/game/pokemon-base-set/booster-box")
    assert a.layout_signature == b.layout_signature


# ────────────────────────────────────────────────────────────────────────────
# Synthetic — counts
# ────────────────────────────────────────────────────────────────────────────

def test_one_sale():
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="1001"))
    r = _parse(html)
    assert r.row_count == 1
    assert r.rows[0].parse_status == "ok"


def test_five_sales():
    rows = "".join(_ebay_row(item_id=str(2000 + i)) for i in range(5))
    r = _parse(_wrap_section("completed-auctions-used", rows))
    assert r.row_count == 5
    assert all(row.parse_status == "ok" for row in r.rows)


def test_many_sales():
    rows = "".join(_ebay_row(item_id=str(3000 + i)) for i in range(50))
    r = _parse(_wrap_section("completed-auctions-used", rows))
    assert r.row_count == 50


def test_no_section_at_all():
    r = _parse("<html><body><p>nothing here</p></body></html>")
    assert r.parse_status == "no_section"


# ────────────────────────────────────────────────────────────────────────────
# Strong identity vs fallback
# ────────────────────────────────────────────────────────────────────────────

def test_ebay_strong_id_from_tr_attribute():
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="377234484140"))
    r = _parse(html)
    row = r.rows[0]
    assert row.marketplace_source == "ebay"
    assert row.marketplace_item_id == "377234484140"
    assert row.parse_confidence == 100
    assert row.provider_sale_key  # deterministic strong hash


def test_ebay_url_fallback_id_when_tr_id_missing():
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id=None,
        href_override="https://www.ebay.com/itm/some-listing-title-227329865069?campid=5338999485"))
    r = _parse(html)
    row = r.rows[0]
    assert row.marketplace_source == "ebay"
    assert row.marketplace_item_id == "227329865069"
    assert row.parse_confidence == 100  # strong-id once URL extraction succeeded


def test_tcgplayer_fallback_key_when_no_id():
    row_html = """
    <tr>
      <td class="date">2026-06-15</td>
      <td class="image"></td>
      <td class="title">
        <a target="_blank" class="js-tcgplayer-completed-sale"
           href="https://partner.tcgplayer.com/c/3029031/1780961/21018?u=https%3A%2F%2Fwww.tcgplayer.com%2Fproduct%2F518861%2F-">
           Pikachu with Grey Felt Hat #85 Near Mint English</a>
        [TCGPlayer]
      </td>
      <td class="numeric"><span class="js-price">$40.00</span></td>
      <td class="numeric listed-price"></td>
      <td class="thumb-down"></td>
    </tr>
    """
    r = _parse(_wrap_section("completed-auctions-used", row_html))
    row = r.rows[0]
    assert row.marketplace_source == "tcgplayer"
    assert row.marketplace_item_id is None
    assert row.parse_confidence == 80  # fallback key
    assert row.provider_sale_key  # fallback hash still present


def test_heritage_auctions_row():
    row_html = """
    <tr id="ha-332617-60044">
      <td class="date">2026-05-14</td>
      <td class="image"></td>
      <td class="title">
        <a target="_blank" class="js-ha-completed-sale"
           href="https://www.ha.com/itm/-/-/-/a/332617-60044.s?type=DA-DMC-PriceCharting">
           Pokémon Charizard #4 1st Edition Base Set PSA Trading Card Game VG-EX 4</a>
        [HeritageAuctions]
      </td>
      <td class="numeric"><span class="js-price">$9,625.00</span></td>
      <td class="numeric listed-price"></td>
      <td class="thumb-down"></td>
    </tr>
    """
    r = _parse(_wrap_section("completed-auctions-used", row_html))
    row = r.rows[0]
    assert row.marketplace_source == "heritage_auctions"
    assert row.marketplace_item_id == "332617-60044"
    assert row.sale_price_cents == 962500


def test_pwcc_row():
    row_html = """
    <tr id="pwcc-abc12345">
      <td class="date">2026-04-01</td>
      <td class="image"></td>
      <td class="title">
        <a target="_blank" class="js-pwcc-completed-sale"
           href="https://www.pwccmarketplace.com/auctions/12345">PWCC sale</a>
        [PWCC]
      </td>
      <td class="numeric"><span class="js-price">$1,500.00</span></td>
      <td class="numeric listed-price"></td>
      <td class="thumb-down"></td>
    </tr>
    """
    r = _parse(_wrap_section("completed-auctions-loose-and-box", row_html))
    row = r.rows[0]
    assert row.marketplace_source == "pwcc"
    assert row.marketplace_item_id == "abc12345"
    assert row.grade == "BGS 10"


# ────────────────────────────────────────────────────────────────────────────
# Section mapping
# ────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("section_slug,expected_grade,expected_company,raw_or_graded", [
    ("used", "Ungraded", None, "raw"),
    ("new", "Grade 8", None, "graded"),
    ("graded", "Grade 9", None, "graded"),
    ("manual-only", "PSA 10", "PSA", "graded"),
    ("box-only", "Grade 9.5", None, "graded"),
    ("loose-and-box", "BGS 10", "BGS", "graded"),
    ("grade-seventeen", "CGC 10", "CGC", "graded"),
    ("grade-eighteen", "SGC 10", "SGC", "graded"),
    ("grade-twenty", "BGS 10 Black", "BGS", "graded"),
    ("grade-twenty-one", "TAG 10", "TAG", "graded"),
    ("grade-twenty-two", "ACE 10", "ACE", "graded"),
    ("grade-nineteen", "CGC 10 Pristine", "CGC", "graded"),
])
def test_section_mapping_for_each_supported_grade(section_slug, expected_grade, expected_company, raw_or_graded):
    html = _wrap_section(f"completed-auctions-{section_slug}", _ebay_row(item_id="9999"))
    r = _parse(html)
    assert r.rows
    row = r.rows[0]
    assert row.grade == expected_grade
    assert row.grading_company == expected_company
    info = SECTION_MAP[section_slug]
    assert info.raw_or_graded == raw_or_graded


def test_unknown_section_is_quarantined_not_dropped():
    html = _wrap_section("completed-auctions-grade-three-thousand", _ebay_row(item_id="1234"))
    r = _parse(html)
    # Row is preserved
    assert r.row_count == 1
    row = r.rows[0]
    assert row.parse_status == "quarantined"
    assert "unknown_section_class" in (row.rejection_reason or "")
    # Warning surfaces the unknown section
    assert any("unknown_section_class" in w for w in r.warnings)


# ────────────────────────────────────────────────────────────────────────────
# Best Offer behaviour
# ────────────────────────────────────────────────────────────────────────────

def test_best_offer_accepted_price_picked_over_list():
    price_html = (
        '<span class="js-price" title="best offer accepted price">$9,800.00</span>'
        '<br>'
        '<span class="js-price listed-price-inline" title="best offer list price">$15,000.00</span>'
    )
    listed_td = '<span class="js-price" title="best offer list price">$15,000.00</span>'
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="42", price_html=price_html, listed_price_td=listed_td))
    row = _parse(html).rows[0]
    assert row.sale_price_cents == 980000
    assert row.original_price_cents == 1500000
    assert row.best_offer_status == "accepted"


def test_non_best_offer_has_no_original_price():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="43", price_html='<span class="js-price">$100.00</span>'))
    row = _parse(html).rows[0]
    assert row.sale_price_cents == 10000
    assert row.original_price_cents is None
    assert row.best_offer_status == "not_best_offer"


# ────────────────────────────────────────────────────────────────────────────
# Price extraction edge cases
# ────────────────────────────────────────────────────────────────────────────

def test_malformed_date_rejects_row():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="44", date="yesterday"))
    r = _parse(html)
    row = r.rows[0]
    assert row.parse_status == "rejected"
    assert row.rejection_reason == "malformed_date"


def test_malformed_price_rejects_row():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="45", price_html='<span class="js-price">$abc</span>'))
    r = _parse(html)
    row = r.rows[0]
    assert row.parse_status == "rejected"
    assert row.rejection_reason == "invalid_final_price"


def test_missing_price_rejects_row():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="46", price_html=""))
    r = _parse(html)
    row = r.rows[0]
    assert row.parse_status == "rejected"
    assert row.rejection_reason == "missing_final_price"


def test_zero_or_negative_price_rejects_row():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="47", price_html='<span class="js-price">$0.00</span>'))
    r = _parse(html)
    assert r.rows[0].parse_status == "rejected"
    assert r.rows[0].rejection_reason == "invalid_final_price"


# ────────────────────────────────────────────────────────────────────────────
# Quality classification
# ────────────────────────────────────────────────────────────────────────────

def test_missing_marketplace_quarantines():
    """No anchor class, no bracket label, no recognisable host → quarantine."""
    row_html = """
    <tr>
      <td class="date">2026-06-01</td>
      <td class="image"></td>
      <td class="title">
        <a target="_blank" href="https://www.example.invalid/listing/123">An unknown sale</a>
      </td>
      <td class="numeric"><span class="js-price">$100.00</span></td>
      <td class="numeric listed-price"></td>
      <td class="thumb-down"></td>
    </tr>
    """
    r = _parse(_wrap_section("completed-auctions-used", row_html))
    row = r.rows[0]
    assert row.parse_status == "quarantined"
    assert "missing_marketplace" in (row.rejection_reason or "")
    assert row.marketplace_source == "unknown"


def test_missing_title_quarantines():
    row_html = """
    <tr id="ebay-50">
      <td class="date">2026-06-01</td>
      <td class="image"></td>
      <td class="title"></td>
      <td class="numeric"><span class="js-price">$100.00</span></td>
      <td class="numeric listed-price"></td>
      <td class="thumb-down"></td>
    </tr>
    """
    r = _parse(_wrap_section("completed-auctions-used", row_html))
    row = r.rows[0]
    assert row.parse_status == "quarantined"
    assert "missing_title" in (row.rejection_reason or "")


def test_lot_bundle_title_is_quarantined():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="60", title="Pokemon Charizard lot of 5 cards"))
    r = _parse(html)
    row = r.rows[0]
    assert row.parse_status == "quarantined"
    assert "lot_or_bundle" in (row.rejection_reason or "")


def test_proxy_title_is_quarantined():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="61", title="Charizard 1st ed PROXY replica metal card"))
    r = _parse(html)
    row = r.rows[0]
    assert row.parse_status == "quarantined"
    assert "proxy_or_reprint" in (row.rejection_reason or "")


def test_wrong_language_title_is_quarantined_on_en_page():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="62", title="ピカチュウ プロモ 山岡 PROMO"))
    r = _parse(html)
    row = r.rows[0]
    assert row.parse_status == "quarantined"
    assert "wrong_language" in (row.rejection_reason or "")
    assert "wrong_language" in row.anomaly_flags


def test_wrong_card_number_when_explicit_expectation_supplied():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="63", title="Pokemon Pikachu #99 unrelated card"))
    r = _parse(html, expected_card_number="4")
    row = r.rows[0]
    assert row.parse_status == "quarantined"
    assert "wrong_card_number" in (row.rejection_reason or "")


def test_correct_card_number_passes_with_leading_zeros():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="64", title="Pikachu #085 Promo"))
    r = _parse(html, expected_card_number="85")
    assert r.rows[0].parse_status == "ok"


def test_conflicting_marketplace_quarantines_and_records_metadata():
    """Anchor class says ebay; bracket label says TCGPlayer."""
    row_html = """
    <tr id="ebay-70">
      <td class="date">2026-06-01</td>
      <td class="image"></td>
      <td class="title">
        <a target="_blank" class="js-ebay-completed-sale"
           href="https://www.ebay.co.uk/itm/70">Conflicting row</a>
        [TCGPlayer]
      </td>
      <td class="numeric"><span class="js-price">$100.00</span></td>
      <td class="numeric listed-price"></td>
      <td class="thumb-down"></td>
    </tr>
    """
    r = _parse(_wrap_section("completed-auctions-used", row_html))
    row = r.rows[0]
    assert row.parse_status == "quarantined"
    assert "conflicting_marketplace" in (row.rejection_reason or "")
    assert row.raw_metadata is not None
    assert row.raw_metadata.get("anchor_class_marketplace") == "ebay"
    assert row.raw_metadata.get("bracket_marketplace") == "tcgplayer"


def test_unsafe_url_scheme_rejects_row():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="80",
                  href_override="javascript:alert('xss')"))
    r = _parse(html)
    row = r.rows[0]
    assert row.parse_status == "rejected"
    assert row.rejection_reason == "unsafe_url"
    # Critical safety property
    assert row.listing_url is None


# ────────────────────────────────────────────────────────────────────────────
# URL normalisation
# ────────────────────────────────────────────────────────────────────────────

def test_listing_url_strips_affiliate_params_and_fragment():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="90",
                  href_override="https://www.ebay.co.uk/itm/90?nordt=true&rt=nc&mkevt=1&mkcid=1&campid=5338999485&customid=noId&toolid=10001#segment"))
    row = _parse(html).rows[0]
    assert row.listing_url == "https://www.ebay.co.uk/itm/90"


# ────────────────────────────────────────────────────────────────────────────
# Deduplication / identity
# ────────────────────────────────────────────────────────────────────────────

def test_duplicate_row_in_same_response_collapses():
    """Two identical rows in the same section should dedup to one."""
    row = _ebay_row(item_id="100")
    html = _wrap_section("completed-auctions-used", row + row)
    r = _parse(html)
    assert r.row_count == 1


def test_same_item_in_two_sections_produces_two_keys():
    """A reclassified row appears under two sections — both kept, both keys differ."""
    used_html = _wrap_section("completed-auctions-used", _ebay_row(item_id="200"))
    psa10_html = _wrap_section("completed-auctions-manual-only", _ebay_row(item_id="200"))
    # Combine into one page
    combined = used_html.replace("</body>", "") + psa10_html.split("<body>", 1)[1]
    r = _parse(combined)
    assert r.row_count == 2
    keys = {row.provider_sale_key for row in r.rows}
    assert len(keys) == 2


def test_strong_key_includes_section_so_reclassification_is_visible():
    a = _parse(_wrap_section("completed-auctions-used", _ebay_row(item_id="300"))).rows[0]
    b = _parse(_wrap_section("completed-auctions-manual-only", _ebay_row(item_id="300"))).rows[0]
    assert a.provider_sale_key != b.provider_sale_key


def test_raw_hash_differs_when_title_changes_strong_key_same():
    """Strong key is identity; raw_hash is change detection."""
    a = _parse(_wrap_section("completed-auctions-used",
                _ebay_row(item_id="400", title="Original title"))).rows[0]
    b = _parse(_wrap_section("completed-auctions-used",
                _ebay_row(item_id="400", title="Corrected title"))).rows[0]
    assert a.provider_sale_key == b.provider_sale_key
    assert a.raw_hash != b.raw_hash


# ────────────────────────────────────────────────────────────────────────────
# Variant / first-edition / condition
# ────────────────────────────────────────────────────────────────────────────

def test_first_edition_detected():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="500", title="Charizard 1st Edition Base Set"))
    row = _parse(html).rows[0]
    assert row.first_edition_status == "first_edition"


def test_unlimited_detected():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="501", title="Charizard Base Set Unlimited"))
    row = _parse(html).rows[0]
    assert row.first_edition_status == "unlimited"


def test_first_edition_unknown_when_ambiguous():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="502", title="Pokemon Charizard #4"))
    row = _parse(html).rows[0]
    assert row.first_edition_status == "unknown"


def test_shadowless_variant_captured():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="503", title="Charizard Shadowless [1st Edition]"))
    row = _parse(html).rows[0]
    assert "1st Edition" in (row.variant_text or "")
    assert "Shadowless" in (row.variant_text or "")


def test_damaged_condition_in_raw_section():
    html = _wrap_section("completed-auctions-used",
        _ebay_row(item_id="504", title="Charizard damaged poor condition"))
    row = _parse(html).rows[0]
    assert row.condition_bucket == "damaged"
    assert row.condition_text


def test_graded_section_overrides_title_nm():
    """A "NM" mention in a PSA 10 section row must NOT downgrade to near_mint."""
    html = _wrap_section("completed-auctions-manual-only",
        _ebay_row(item_id="505", title="Charizard NM gem mint"))
    row = _parse(html).rows[0]
    assert row.condition_bucket == "graded"


# ────────────────────────────────────────────────────────────────────────────
# Layout signature & selector drift
# ────────────────────────────────────────────────────────────────────────────

def test_layout_signature_is_64_hex_chars():
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="600"))
    r = _parse(html)
    assert len(r.layout_signature) == 64
    int(r.layout_signature, 16)  # raises if not hex


def test_layout_signature_unchanged_when_only_data_changes():
    """Two pages with identical structure but different prices/titles must hash the same."""
    a = _parse(_wrap_section("completed-auctions-used",
        _ebay_row(item_id="700", title="A", price_html='<span class="js-price">$100.00</span>')))
    b = _parse(_wrap_section("completed-auctions-used",
        _ebay_row(item_id="701", title="B", price_html='<span class="js-price">$200.00</span>')))
    assert a.layout_signature == b.layout_signature


def test_layout_signature_changes_when_structure_drifts():
    a = _parse(_wrap_section("completed-auctions-used", _ebay_row(item_id="800")))
    drifted = _wrap_section("completed-auctions-used", _ebay_row(item_id="800")).replace(
        "hoverable-rows sortable", "hoverable_rows sortable")
    b = _parse(drifted)
    assert a.layout_signature != b.layout_signature
    # And: the renamed page no longer detects the section
    assert b.parse_status == "no_section"


# ────────────────────────────────────────────────────────────────────────────
# Defensive
# ────────────────────────────────────────────────────────────────────────────

def test_parser_never_raises_on_garbage():
    for h in ["", "<", "<<<>>>", "<html", "<html><body><table>", "definitely not html"]:
        r = _parse(h)
        assert r.parse_status in ("no_section", "ok", "failed", "unsupported_layout")
        assert r.rows == []


def test_unrelated_table_is_ignored():
    """A page with only games_table (cross-product table) must not yield sale rows."""
    html = """
    <html><body>
    <table id="games_table" class='js-addable hoverable-rows sortable'>
      <thead><tr><th>x</th></tr></thead>
      <tbody><tr><td>unrelated</td></tr></tbody>
    </table>
    </body></html>
    """
    r = _parse(html)
    assert r.parse_status == "no_section"
    assert r.row_count == 0


def test_import_run_id_propagated_into_rows():
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="900"))
    r = _parse(html, import_run_id="run-abc-123")
    assert r.rows[0].import_run_id == "run-abc-123"


def test_parser_version_propagated_into_rows_and_result():
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="901"))
    r = _parse(html)
    assert r.parser_version == RECENT_SALES_PARSER_VERSION
    assert r.rows[0].parser_version == RECENT_SALES_PARSER_VERSION


def test_to_dict_is_json_friendly():
    """Result should serialise to plain dicts (no dataclass references)."""
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="902"))
    r = _parse(html)
    d = r.to_dict()
    assert isinstance(d, dict)
    assert isinstance(d["rows"], list)
    assert isinstance(d["rows"][0], dict)


def test_marketplace_country_known_for_uk_ebay_only():
    uk = _parse(_wrap_section("completed-auctions-used",
        _ebay_row(item_id="903", host="ebay.co.uk"))).rows[0]
    com = _parse(_wrap_section("completed-auctions-used",
        _ebay_row(item_id="904", host="ebay.com"))).rows[0]
    assert uk.marketplace_country == "GB"
    assert com.marketplace_country is None  # never guess US from bare ebay.com
