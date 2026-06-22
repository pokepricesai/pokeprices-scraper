"""
Tests for recent_sales_ingestion.py — Block 4B-S-2A.

All tests are offline. The ingestion controller depends on a
``SupabaseClient`` instance that exposes 4 methods; tests inject a
``FakeSupabaseClient`` so no network call ever happens.

A small synthetic HTML helper builds a parser-compatible page; we reuse
the test-suite style established in test_recent_sales_parser.py.
"""
from __future__ import annotations

import json
import os
import sys
from textwrap import dedent
from typing import Any

import pytest

import recent_sales_ingestion as rsi
from recent_sales_ingestion import (
    INGESTION_ENABLED_ENV,
    DRY_RUN_ENV,
    RecentSalesIngestion,
    SupabaseClient,
    _DB_BEST_OFFER_STATUS_MAP,
    _DB_CONDITION_BUCKET_MAP,
    _DB_STATUSES,
    _IMPORT_TYPE_TO_DB_SOURCE,
    _resolve_db_source,
    _sale_row_to_db_row,
    is_ingestion_enabled,
    is_dry_run,
    parse_expected_card_number,
)
from recent_sales_parser import SaleRow, SOURCE_ATTRIBUTION, RECENT_SALES_PARSER_VERSION, SCHEMA_VERSION


# ────────────────────────────────────────────────────────────────────────────
# Fake Supabase client
# ────────────────────────────────────────────────────────────────────────────

class FakeSupabaseClient:
    """Capture every call without touching the network."""

    def __init__(
        self,
        *,
        allow_list: set[str] | None = None,
        raise_on_allow_list: Exception | None = None,
        raise_on_upsert: Exception | None = None,
        run_id_to_assign: str | None = "run-test-abc",
    ):
        self._allow_list = allow_list or set()
        self._raise_on_allow_list = raise_on_allow_list
        self._raise_on_upsert = raise_on_upsert
        self._run_id_to_assign = run_id_to_assign

        self.allow_list_calls = 0
        self.upsert_calls: list[list[dict]] = []
        self.market_run_inserts: list[dict] = []
        self.market_run_updates: list[tuple[str, dict]] = []

    def get_allow_list(self, *, provider: str = "pricecharting") -> set[str]:
        self.allow_list_calls += 1
        if self._raise_on_allow_list is not None:
            raise self._raise_on_allow_list
        return set(self._allow_list)

    def upsert_recent_sales(self, rows: list[dict]) -> int:
        if self._raise_on_upsert is not None:
            raise self._raise_on_upsert
        self.upsert_calls.append([dict(r) for r in rows])
        return len(rows)

    def insert_market_run(self, payload: dict) -> str | None:
        self.market_run_inserts.append(dict(payload))
        return self._run_id_to_assign

    def update_market_run(self, run_id: str, payload: dict) -> bool:
        self.market_run_updates.append((run_id, dict(payload)))
        return True


# ────────────────────────────────────────────────────────────────────────────
# Synthetic HTML helpers (kept local to avoid coupling with parser tests)
# ────────────────────────────────────────────────────────────────────────────

def _wrap_section(section_class: str, tbody_rows_html: str) -> str:
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


def _ebay_row(*, item_id: str, date: str = "2026-06-01",
              title: str = "Pokemon Charizard listing", price: str = "$100.00") -> str:
    return f"""
    <tr id="ebay-{item_id}">
      <td class="date">{date}</td>
      <td class="image"></td>
      <td class="title">
        <a target="_blank" class="js-ebay-completed-sale"
           href="https://www.ebay.co.uk/itm/{item_id}?campid=5338999485">{title}</a>
        [eBay]
      </td>
      <td class="numeric"><span class="js-price">{price}</span></td>
      <td class="numeric listed-price"></td>
      <td class="thumb-down"></td>
    </tr>
    """


def _quarantine_row(item_id: str) -> str:
    # lot/bundle title → parser marks quarantined
    return _ebay_row(item_id=item_id, title="Pokemon Charizard lot of 5 cards")


def _reject_row(item_id: str) -> str:
    # malformed date → parser marks rejected
    return _ebay_row(item_id=item_id, date="yesterday")


PAGE_URL = "https://www.pricecharting.com/game/pokemon-base-set/charizard-1st-edition-4"


# ────────────────────────────────────────────────────────────────────────────
# Flag fail-closed behaviour
# ────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("value,expected", [
    ("true", True),
    ("True", False),
    ("TRUE", False),
    ("1", False),
    ("yes", False),
    ("on", False),
    ("", False),
    (" true", False),
    ("true ", False),
])
def test_is_ingestion_enabled_strict(value: str, expected: bool):
    assert is_ingestion_enabled({INGESTION_ENABLED_ENV: value}) is expected


def test_is_ingestion_enabled_missing_var_is_false():
    assert is_ingestion_enabled({}) is False


@pytest.mark.parametrize("value,expected", [
    ("true", True),
    ("True", False),
    ("1", False),
    ("", False),
])
def test_is_dry_run_strict(value: str, expected: bool):
    assert is_dry_run({DRY_RUN_ENV: value}) is expected


def test_init_for_scraper_run_returns_none_when_flag_off(monkeypatch):
    monkeypatch.delenv(INGESTION_ENABLED_ENV, raising=False)
    # Even if SUPABASE env vars are present, no supabase work happens
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "k")

    fake = FakeSupabaseClient(allow_list={"X"})
    monkeypatch.setattr(rsi, "build_default_supabase_client", lambda: fake)
    assert rsi.init_for_scraper_run() is None
    # No allow-list fetch attempted
    assert fake.allow_list_calls == 0
    assert fake.market_run_inserts == []


def test_init_for_scraper_run_returns_none_when_supabase_missing(monkeypatch):
    monkeypatch.setenv(INGESTION_ENABLED_ENV, "true")
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    assert rsi.init_for_scraper_run() is None


def test_init_for_scraper_run_returns_none_when_allow_list_load_fails(monkeypatch):
    monkeypatch.setenv(INGESTION_ENABLED_ENV, "true")
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "k")
    fake = FakeSupabaseClient(
        allow_list={"X"},
        raise_on_allow_list=RuntimeError("simulated PostgREST 500"),
    )
    monkeypatch.setattr(rsi, "build_default_supabase_client", lambda: fake)
    assert rsi.init_for_scraper_run() is None


def test_init_for_scraper_run_starts_when_flag_on(monkeypatch):
    monkeypatch.setenv(INGESTION_ENABLED_ENV, "true")
    monkeypatch.delenv(DRY_RUN_ENV, raising=False)
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "k")
    fake = FakeSupabaseClient(allow_list={"123", "456"})
    monkeypatch.setattr(rsi, "build_default_supabase_client", lambda: fake)
    ig = rsi.init_for_scraper_run()
    assert ig is not None
    assert ig.allow_list == {"123", "456"}
    assert ig.run_id == "run-test-abc"
    assert len(fake.market_run_inserts) == 1
    p = fake.market_run_inserts[0]
    assert p["provider"] == "pricecharting"
    assert p["status"] == "running"
    # Block 4B-W-1 source CHECK: only ('scraper_nightly','admin_manual',
    # 'backfill','pilot'). The descriptive 'recent_sales_pilot' label maps
    # to 'pilot' and survives in the finish-time notes JSON.
    assert p["source"] == "pilot"
    assert "import_type" not in p
    assert p["parser_version"]


# ────────────────────────────────────────────────────────────────────────────
# Allow-list filtering
# ────────────────────────────────────────────────────────────────────────────

def test_card_not_in_allow_list_is_skipped():
    fake = FakeSupabaseClient(allow_list={"999"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="1000"))
    result = ig.maybe_ingest(html=html, provider_card_id="not-in-list",
                             page_url=PAGE_URL)
    assert result is None
    assert ig.cards_seen == 1
    assert ig.cards_allowlisted == 0
    assert ig.cards_parsed == 0
    # No write attempted
    assert fake.upsert_calls == []


def test_card_in_allow_list_is_parsed():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    result = ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    assert result is not None
    assert result.parse_status == "ok"
    assert ig.cards_allowlisted == 1
    assert ig.cards_parsed == 1
    assert ig.rows_ok == 1


def test_provider_card_id_coerced_to_string_for_allow_list_check():
    """If a caller passes the int form, the allow-list (str) still matches."""
    fake = FakeSupabaseClient(allow_list={"715593"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    res = ig.maybe_ingest(html=html, provider_card_id=715593, page_url=PAGE_URL)
    assert res is not None
    assert ig.cards_parsed == 1


# ────────────────────────────────────────────────────────────────────────────
# OK rows -> upsert; quarantined/rejected -> not inserted
# ────────────────────────────────────────────────────────────────────────────

def test_ok_rows_are_buffered_and_flushed_with_provider_sale_key():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section(
        "completed-auctions-used",
        _ebay_row(item_id="11111111111") + _ebay_row(item_id="22222222222"),
    )
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.rows_ok == 2
    assert ig.rows_upserted == 2
    assert len(fake.upsert_calls) == 1
    written = fake.upsert_calls[0]
    assert {r["provider_sale_key"] for r in written}
    # Identity is via provider_sale_key — every row carries one
    assert all("provider_sale_key" in r and r["provider_sale_key"] for r in written)
    # raw_hash is also present but is NOT the identity
    assert all("raw_hash" in r for r in written)
    # internal_card_slug is the bare numeric per spec
    assert all(r["internal_card_slug"] == "42" for r in written)
    assert all(r["provider_card_id"] == "42" for r in written)


def test_recent_sales_payload_keeps_anomaly_flags_drops_schema_version():
    """Block 4B-W-1 schema requires anomaly_flags (NOT NULL jsonb) and has
    no schema_version column. The payload must reflect that."""
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert fake.upsert_calls, "expected one upsert batch"
    for r in fake.upsert_calls[0]:
        assert "schema_version" not in r, "schema_version is not in recent_sales schema"
        assert "anomaly_flags" in r, "anomaly_flags is required NOT NULL"
        assert isinstance(r["anomaly_flags"], list)  # OK rows → []
        # raw_metadata column accepts NULL; we permit it in the payload
        # (parser sets None for OK rows). Either present-as-None or absent
        # is acceptable.
        if "raw_metadata" in r:
            assert r["raw_metadata"] is None


def test_recent_sales_payload_includes_required_schema_columns():
    """Every required NOT-NULL column without a DB-side default must be set."""
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    required_no_default = {
        "provider_sale_key", "provider", "provider_card_id", "internal_card_slug",
        "pricecharting_url", "observed_section", "sale_date", "marketplace_source",
        "listing_title", "sale_price_cents", "raw_hash", "parser_version",
        "parse_confidence", "parse_status", "anomaly_flags",
    }
    for r in fake.upsert_calls[0]:
        missing = required_no_default - set(r)
        assert not missing, f"missing required columns: {missing}"


def test_quarantined_rows_counted_but_not_written():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section(
        "completed-auctions-used",
        _ebay_row(item_id="11111111111")  # ok
        + _quarantine_row("22222222222"),  # quarantined (lot)
    )
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.rows_ok == 1
    assert ig.rows_quarantined == 1
    # Only the OK row was written
    assert sum(len(b) for b in fake.upsert_calls) == 1


def test_rejected_rows_counted_but_not_written():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section(
        "completed-auctions-used",
        _ebay_row(item_id="11111111111")  # ok
        + _reject_row("22222222222"),  # rejected (bad date)
    )
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.rows_ok == 1
    assert ig.rows_rejected == 1
    assert sum(len(b) for b in fake.upsert_calls) == 1


def test_provider_sale_key_is_used_for_upsert_via_supabase_client_url(monkeypatch):
    """SupabaseClient must hit the on_conflict=provider_sale_key endpoint."""
    captured: dict = {}

    class StubSession:
        def post(self, url, json, headers, timeout):
            captured["url"] = url
            captured["headers"] = headers
            captured["body"] = json

            class _R:
                status_code = 201
                text = ""
            return _R()

    c = SupabaseClient("https://example.supabase.co", "k", session=StubSession())
    c.upsert_recent_sales([{"provider_sale_key": "abc"}])
    assert "on_conflict=provider_sale_key" in captured["url"]
    assert "resolution=merge-duplicates" in captured["headers"]["Prefer"]


def test_recent_sales_upsert_url_does_not_use_raw_hash_or_item_id():
    """Identity must never be raw_hash or marketplace_item_id."""
    assert rsi.UPSERT_CONFLICT_COLUMN == "provider_sale_key"


# ────────────────────────────────────────────────────────────────────────────
# Dry-run behaviour
# ────────────────────────────────────────────────────────────────────────────

def test_dry_run_does_not_write_recent_sales():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list(), dry_run=True)
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.rows_ok == 1
    assert ig.rows_upserted == 0
    # No upsert calls
    assert fake.upsert_calls == []


def test_dry_run_skips_market_import_runs_create_and_update():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list(), dry_run=True)
    ig.start()
    ig.finish()
    assert fake.market_run_inserts == []
    assert fake.market_run_updates == []
    # And run_id stays None
    assert ig.run_id is None


# ────────────────────────────────────────────────────────────────────────────
# Market import runs lifecycle
# ────────────────────────────────────────────────────────────────────────────

def test_market_run_created_at_start_with_running_status():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    assert ig.run_id == "run-test-abc"
    assert len(fake.market_run_inserts) == 1
    body = fake.market_run_inserts[0]
    assert body["status"] == "running"
    assert body["provider"] == "pricecharting"
    # CHECK list is ('scraper_nightly','admin_manual','backfill','pilot').
    # 'recent_sales_pilot' is NOT allowed; mapping resolves it to 'pilot'.
    assert body["source"] == "pilot"
    assert "import_type" not in body
    assert "started_at" in body
    assert body["parser_version"]


def test_market_run_updated_at_finish_with_stats():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section(
        "completed-auctions-used",
        _ebay_row(item_id="11111111111") + _quarantine_row("22222222222"),
    )
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.maybe_ingest(html=html, provider_card_id="not-on-list", page_url=PAGE_URL)
    ig.finish(status="success")

    assert len(fake.market_run_updates) == 1
    run_id, payload = fake.market_run_updates[0]
    assert run_id == "run-test-abc"
    # Schema-aligned top-level fields. CHECK list is
    # ('running','success','partial','failed'); 'completed' is NOT allowed.
    assert payload["status"] == "success"
    assert "completed_at" in payload
    assert "finished_at" not in payload, "Block 4B-W-1 column is completed_at"
    assert payload["pages_processed"] == 2          # was: cards_seen
    assert payload["rows_ok"] == 1
    assert payload["rows_quarantined"] == 1
    assert payload["rows_rejected"] == 0
    assert payload["rows_duplicate"] == 0
    assert "duration_ms" in payload
    assert payload["duration_ms"] is None or isinstance(payload["duration_ms"], int)
    # Non-schema stats moved into the ``notes`` text column as JSON
    for absent in (
        "cards_seen", "cards_allowlisted", "cards_parsed",
        "rows_inserted", "rows_updated", "errors_count",
    ):
        assert absent not in payload, f"{absent} is not a market_import_runs column"
    notes = json.loads(payload["notes"])
    assert notes["import_type"] == "recent_sales_pilot"
    assert notes["cards_allowlisted"] == 1
    assert notes["cards_parsed"] == 1
    assert notes["rows_upserted"] == 1
    assert notes["errors_count"] == 0


def test_market_run_marked_failed_on_context_manager_exception():
    fake = FakeSupabaseClient(allow_list={"42"})
    with pytest.raises(RuntimeError):
        with RecentSalesIngestion(fake, allow_list=fake.get_allow_list()) as ig:
            html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
            ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
            raise RuntimeError("simulated downstream failure")
    assert len(fake.market_run_updates) == 1
    _, payload = fake.market_run_updates[0]
    assert payload["status"] == "failed"


def test_errors_count_reflects_upsert_failure():
    fake = FakeSupabaseClient(
        allow_list={"42"},
        raise_on_upsert=RuntimeError("simulated upsert failure"),
    )
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.errors_count >= 1
    _, payload = fake.market_run_updates[0]
    # errors_count is recorded inside the schema's ``notes`` JSON, not at top-level
    notes = json.loads(payload["notes"])
    assert notes["errors_count"] >= 1
    assert notes["rows_upserted"] == 0


# ────────────────────────────────────────────────────────────────────────────
# Allow-list URL construction (no network)
# ────────────────────────────────────────────────────────────────────────────

def test_get_allow_list_url_filters_provider_and_enabled():
    captured: dict = {}

    class StubSession:
        def get(self, url, headers, timeout):
            captured["url"] = url

            class _R:
                status_code = 200
                text = '[{"provider_card_id":"715593"},{"provider_card_id":42}]'
                def json(self):
                    import json
                    return json.loads(self.text)
            return _R()

    c = SupabaseClient("https://example.supabase.co", "k", session=StubSession())
    result = c.get_allow_list()
    assert result == {"715593", "42"}  # coerced to strings
    assert "provider=eq.pricecharting" in captured["url"]
    assert "enabled=eq.true" in captured["url"]
    assert "select=provider_card_id" in captured["url"]


# ────────────────────────────────────────────────────────────────────────────
# parse_expected_card_number helper
# ────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("product_name,expected", [
    ("Charizard [1st Edition] #4", "4"),
    ("Pikachu with Grey Felt Hat #085", "085"),
    ("Mew #25a", "25"),
    ("Booster Box", None),
    ("", None),
    (None, None),
])
def test_parse_expected_card_number(product_name, expected):
    assert parse_expected_card_number(product_name) == expected


# ────────────────────────────────────────────────────────────────────────────
# v8 integration smoke (no main run; verifies import + gating only)
# ────────────────────────────────────────────────────────────────────────────

def test_v8_imports_and_does_not_initialise_ingestion_by_default(monkeypatch):
    """Importing v8 alone must not touch Supabase or call the ingestion init."""
    monkeypatch.delenv(INGESTION_ENABLED_ENV, raising=False)

    # Block any accidental Supabase call from the module's `init_for_scraper_run`.
    import recent_sales_ingestion as _rsi_mod
    def _no_supabase():
        raise AssertionError("build_default_supabase_client must not run with flag off")
    monkeypatch.setattr(_rsi_mod, "build_default_supabase_client", _no_supabase)

    # Reimport v8 cleanly. If `import_for_scraper_run` were called at import time
    # (it must not be — it's only called inside main()), the AssertionError above
    # would propagate.
    if "pokeprices_scraper_v8" in sys.modules:
        del sys.modules["pokeprices_scraper_v8"]
    import pokeprices_scraper_v8  # noqa: F401
    # And: confirm the gating still reports disabled.
    assert _rsi_mod.is_ingestion_enabled() is False


def test_v8_module_exposes_recent_sales_ingestion_import():
    """v8 must hold a reference to the ingestion module for its main() hook."""
    if "pokeprices_scraper_v8" in sys.modules:
        del sys.modules["pokeprices_scraper_v8"]
    import pokeprices_scraper_v8 as v8
    # Either the module imported cleanly (_rsi is non-None) or the import
    # failure was captured for a runtime warning — both states are valid;
    # what we never want is an unhandled ImportError at module top level.
    assert hasattr(v8, "_rsi")
    assert hasattr(v8, "_rsi_import_error")


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-W-1 CHECK-vocabulary mapping (recent_sales)
# ────────────────────────────────────────────────────────────────────────────

# Schema-allowed values, copied from migrations/2026-06-17-recent-sales-stage-1.sql
_DB_ALLOWED_BEST_OFFER = {"none", "accepted", "unknown"}
_DB_ALLOWED_CONDITION_BUCKET = {
    "mint", "near_mint", "lightly_played", "played", "poor", "unknown",
}
_DB_ALLOWED_RAW_OR_GRADED = {"raw", "graded"}


def _ebay_row_bo_accepted(item_id: str) -> str:
    """One Best-Offer accepted row."""
    return f"""
    <tr id="ebay-{item_id}">
      <td class="date">2026-06-01</td>
      <td class="image"></td>
      <td class="title">
        <a target="_blank" class="js-ebay-completed-sale"
           href="https://www.ebay.co.uk/itm/{item_id}">Pokemon Charizard</a>
        [eBay]
      </td>
      <td class="numeric">
        <span class="js-price" title="best offer accepted price">$80.00</span>
        <br>
        <span class="js-price listed-price-inline" title="best offer list price">$120.00</span>
      </td>
      <td class="numeric listed-price">
        <span class="js-price" title="best offer list price">$120.00</span>
      </td>
      <td class="thumb-down"></td>
    </tr>
    """


def test_best_offer_status_not_best_offer_maps_to_none():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    # Plain price row → parser emits best_offer_status='not_best_offer'
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert fake.upsert_calls, "expected one upsert batch"
    row = fake.upsert_calls[0][0]
    assert row["best_offer_status"] == "none"
    assert row["best_offer_status"] in _DB_ALLOWED_BEST_OFFER


def test_best_offer_status_accepted_passes_through():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row_bo_accepted("22222222222"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    row = fake.upsert_calls[0][0]
    assert row["best_offer_status"] == "accepted"
    assert row["best_offer_status"] in _DB_ALLOWED_BEST_OFFER


def test_best_offer_status_unknown_passes_through_via_unit_map():
    assert _DB_BEST_OFFER_STATUS_MAP["unknown"] == "unknown"


@pytest.mark.parametrize("parser_value,db_value", list(_DB_BEST_OFFER_STATUS_MAP.items()))
def test_all_parser_best_offer_status_values_have_db_mapping(parser_value, db_value):
    assert db_value in _DB_ALLOWED_BEST_OFFER
    assert _DB_BEST_OFFER_STATUS_MAP[parser_value] == db_value


@pytest.mark.parametrize("parser_value,db_value", [
    ("near_mint",         "near_mint"),
    ("lightly_played",    "lightly_played"),
    ("moderately_played", "played"),
    ("heavily_played",    "played"),
    ("damaged",           "poor"),
    ("graded",            "unknown"),
    ("raw_unknown",       "unknown"),
    ("unknown",           "unknown"),
])
def test_condition_bucket_mapping_table_matches_spec(parser_value, db_value):
    assert _DB_CONDITION_BUCKET_MAP[parser_value] == db_value
    assert db_value in _DB_ALLOWED_CONDITION_BUCKET


@pytest.mark.parametrize("parser_value", list(_DB_CONDITION_BUCKET_MAP))
def test_all_parser_condition_bucket_values_have_db_mapping(parser_value):
    assert _DB_CONDITION_BUCKET_MAP[parser_value] in _DB_ALLOWED_CONDITION_BUCKET


def _row_with_condition(condition_bucket: str) -> SaleRow:
    """Build a minimal OK SaleRow for direct _sale_row_to_db_row testing."""
    return SaleRow(
        schema_version=SCHEMA_VERSION, provider="pricecharting",
        provider_card_id="42", internal_card_slug="42",
        pricecharting_url="https://x", observed_section="completed-auctions-used",
        sale_date="2026-06-01", marketplace_source="ebay", marketplace_country="GB",
        listing_title="Title", sale_price_cents=10000, original_price_cents=None,
        display_currency="USD", source_currency=None, grading_company=None, grade=None,
        condition_text=None, condition_bucket=condition_bucket,
        listing_url="https://www.ebay.co.uk/itm/1", marketplace_item_id="1",
        best_offer_status="not_best_offer", language="en",
        first_edition_status="unknown", variant_text=None,
        provider_sale_key="k", raw_hash="h",
        parser_version=RECENT_SALES_PARSER_VERSION, parse_confidence=100,
        parse_status="ok", rejection_reason=None,
        source_attribution=SOURCE_ATTRIBUTION,
    )


@pytest.mark.parametrize("parser_value,db_value", [
    ("moderately_played", "played"),
    ("heavily_played",    "played"),
    ("damaged",           "poor"),
    ("graded",            "unknown"),
    ("raw_unknown",       "unknown"),
    ("near_mint",         "near_mint"),
    ("lightly_played",    "lightly_played"),
    ("unknown",           "unknown"),
])
def test_sale_row_to_db_row_applies_condition_bucket_mapping(parser_value, db_value):
    row = _row_with_condition(parser_value)
    d = _sale_row_to_db_row(row)
    assert d["condition_bucket"] == db_value


def test_sale_row_to_db_row_drops_schema_version_keeps_anomaly_flags():
    row = _row_with_condition("near_mint")
    d = _sale_row_to_db_row(row)
    assert "schema_version" not in d
    assert "anomaly_flags" in d
    assert isinstance(d["anomaly_flags"], list)


def test_sale_row_to_db_row_unknown_condition_bucket_coerces_to_unknown():
    row = _row_with_condition("future_bucket_added_later")
    d = _sale_row_to_db_row(row)
    assert d["condition_bucket"] == "unknown"
    assert d["condition_bucket"] in _DB_ALLOWED_CONDITION_BUCKET


def test_sale_row_to_db_row_unknown_best_offer_coerces_to_unknown():
    row = _row_with_condition("near_mint")
    row.best_offer_status = "weird_new_value"
    d = _sale_row_to_db_row(row)
    assert d["best_offer_status"] == "unknown"


def test_sale_row_to_db_row_derives_raw_or_graded_from_used_section():
    row = _row_with_condition("near_mint")
    row.observed_section = "completed-auctions-used"
    d = _sale_row_to_db_row(row)
    assert d["raw_or_graded"] == "raw"
    assert d["raw_or_graded"] in _DB_ALLOWED_RAW_OR_GRADED


@pytest.mark.parametrize("section", [
    "completed-auctions-manual-only",
    "completed-auctions-graded",
    "completed-auctions-new",
    "completed-auctions-grade-seventeen",
    "completed-auctions-loose-and-box",
])
def test_sale_row_to_db_row_derives_graded_for_non_used_sections(section):
    row = _row_with_condition("near_mint")
    row.observed_section = section
    d = _sale_row_to_db_row(row)
    assert d["raw_or_graded"] == "graded"


def test_sale_row_to_db_row_leaves_raw_or_graded_null_for_unrecognised_section():
    row = _row_with_condition("near_mint")
    row.observed_section = "something-else-entirely"
    d = _sale_row_to_db_row(row)
    assert d.get("raw_or_graded") is None


# ────────────────────────────────────────────────────────────────────────────
# market_import_runs.source resolver
# ────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("label,db_source", list(_IMPORT_TYPE_TO_DB_SOURCE.items()))
def test_resolve_db_source_maps_known_labels(label, db_source):
    assert _resolve_db_source(label) == db_source
    assert db_source in {"scraper_nightly", "admin_manual", "backfill", "pilot"}


def test_resolve_db_source_pilot_label_explicit():
    assert _resolve_db_source("recent_sales_pilot") == "pilot"


def test_resolve_db_source_rejects_unknown():
    with pytest.raises(ValueError):
        _resolve_db_source("foobar")


# ────────────────────────────────────────────────────────────────────────────
# Status validation
# ────────────────────────────────────────────────────────────────────────────

def test_finish_default_status_is_success():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    ig.finish()  # no explicit status
    _, payload = fake.market_run_updates[0]
    assert payload["status"] == "success"


def test_finish_with_bogus_status_coerces_to_failed_and_warns(caplog):
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    with caplog.at_level("WARNING", logger="pokeprices.recent_sales_ingestion"):
        ig.finish(status="bogus")
    _, payload = fake.market_run_updates[0]
    assert payload["status"] == "failed"
    assert any("coercing to 'failed'" in r.getMessage() for r in caplog.records)


def test_context_manager_success_uses_success_status():
    fake = FakeSupabaseClient(allow_list={"42"})
    with RecentSalesIngestion(fake, allow_list=fake.get_allow_list()):
        pass
    _, payload = fake.market_run_updates[0]
    assert payload["status"] == "success"


@pytest.mark.parametrize("status", sorted(_DB_STATUSES))
def test_all_db_statuses_are_accepted_by_finish(status):
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    ig.finish(status=status)
    _, payload = fake.market_run_updates[0]
    assert payload["status"] == status


# ────────────────────────────────────────────────────────────────────────────
# Forbidden-value sweeps over every Supabase write captured by the fake
# ────────────────────────────────────────────────────────────────────────────

def _run_minimal_pilot(allow_list_set, *, dry_run=False):
    """End-to-end fake-driven run for forbidden-value sweeps."""
    fake = FakeSupabaseClient(allow_list=allow_list_set)
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list(), dry_run=dry_run)
    ig.start()
    html = _wrap_section(
        "completed-auctions-used",
        _ebay_row(item_id="11111111111") + _ebay_row_bo_accepted("22222222222"),
    )
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    return fake


def test_no_market_run_payload_contains_status_completed():
    fake = _run_minimal_pilot({"42"})
    for body in fake.market_run_inserts:
        assert body.get("status") != "completed"
    for _, body in fake.market_run_updates:
        assert body.get("status") != "completed"


def test_no_market_run_payload_contains_source_recent_sales_pilot():
    fake = _run_minimal_pilot({"42"})
    for body in fake.market_run_inserts:
        assert body.get("source") != "recent_sales_pilot"
        assert body.get("source") in {"scraper_nightly", "admin_manual", "backfill", "pilot"}


def test_no_recent_sales_row_uses_parser_only_best_offer_value():
    fake = _run_minimal_pilot({"42"})
    for batch in fake.upsert_calls:
        for row in batch:
            if row.get("best_offer_status") is not None:
                assert row["best_offer_status"] in _DB_ALLOWED_BEST_OFFER
                assert row["best_offer_status"] != "not_best_offer"


def test_no_recent_sales_row_uses_parser_only_condition_bucket_value():
    fake = _run_minimal_pilot({"42"})
    for batch in fake.upsert_calls:
        for row in batch:
            cb = row.get("condition_bucket")
            if cb is not None:
                assert cb in _DB_ALLOWED_CONDITION_BUCKET
                assert cb not in {
                    "moderately_played", "heavily_played", "damaged",
                    "graded", "raw_unknown",
                }


def test_recent_sales_row_has_raw_or_graded_set_for_known_section():
    fake = _run_minimal_pilot({"42"})
    for batch in fake.upsert_calls:
        for row in batch:
            if row.get("observed_section", "").startswith("completed-auctions-"):
                assert row["raw_or_graded"] in _DB_ALLOWED_RAW_OR_GRADED


# ────────────────────────────────────────────────────────────────────────────
# add_run_notes — extras land inside the schema's notes JSON, never as
# top-level columns. Built-in keys win on collision.
# ────────────────────────────────────────────────────────────────────────────

def test_add_run_notes_extras_appear_in_notes_json():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    ig.add_run_notes(fetched=18, skipped_429=2, skipped_http_error=0)
    ig.finish()
    _, payload = fake.market_run_updates[0]
    # NOT top-level
    for k in ("fetched", "skipped_429", "skipped_http_error"):
        assert k not in payload
    notes = json.loads(payload["notes"])
    assert notes["fetched"] == 18
    assert notes["skipped_429"] == 2
    assert notes["skipped_http_error"] == 0
    # Built-in keys still present
    assert notes["import_type"] == "recent_sales_pilot"
    assert "cards_parsed" in notes
    assert "rows_upserted" in notes


def test_add_run_notes_does_not_overwrite_builtin_audit_keys():
    """If a runner accidentally calls add_run_notes(rows_ok=999) the
    built-in audit value must still win on the wire."""
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    # Try to clobber a built-in key
    ig.add_run_notes(
        import_type="something_else",
        cards_parsed=999,
        rows_upserted=999,
        errors_count=999,
    )
    ig.finish()
    _, payload = fake.market_run_updates[0]
    notes = json.loads(payload["notes"])
    assert notes["import_type"] == "recent_sales_pilot"
    assert notes["cards_parsed"] == 1
    assert notes["rows_upserted"] == 1
    assert notes["errors_count"] == 0


def test_add_run_notes_multiple_calls_accumulate():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    ig.add_run_notes(fetched=5)
    ig.add_run_notes(skipped_429=1)
    ig.add_run_notes(fetched=10)  # latest wins on repeat key
    ig.finish()
    _, payload = fake.market_run_updates[0]
    notes = json.loads(payload["notes"])
    assert notes["fetched"] == 10
    assert notes["skipped_429"] == 1
