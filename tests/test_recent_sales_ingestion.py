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
        active_rows_by_card: dict[str, list[dict]] | None = None,
        raise_on_allow_list: Exception | None = None,
        raise_on_upsert: Exception | None = None,
        raise_on_prune: Exception | None = None,
        raise_on_get_active: Exception | None = None,
        prune_affected_per_call: int | None = None,
        run_id_to_assign: str | None = "run-test-abc",
    ):
        self._allow_list = allow_list or set()
        # Seed the DB-side active OK rows used by the grade-aware prune
        # path. Keyed by provider_card_id; default empty list per card.
        self._active_rows_by_card = {
            str(k): list(v) for k, v in (active_rows_by_card or {}).items()
        }
        self._raise_on_allow_list = raise_on_allow_list
        self._raise_on_upsert = raise_on_upsert
        self._raise_on_prune = raise_on_prune
        self._raise_on_get_active = raise_on_get_active
        # When None, the prune returns "(seeded active rows for the
        # card) NOT IN (kept_psks)" — i.e. realistic count. Pass an int
        # to override (legacy tests).
        self._prune_affected_per_call = prune_affected_per_call
        self._run_id_to_assign = run_id_to_assign

        self.allow_list_calls = 0
        self.upsert_calls: list[list[dict]] = []
        self.market_run_inserts: list[dict] = []
        self.market_run_updates: list[tuple[str, dict]] = []
        # Each prune call: (provider_card_id, kept_psks)
        self.prune_calls: list[tuple[str, list[str]]] = []
        # Each active-rows fetch call: provider_card_id
        self.get_active_calls: list[str] = []

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

    def get_active_recent_sales_for_card(
        self, *, provider_card_id: str,
    ) -> list[dict]:
        self.get_active_calls.append(str(provider_card_id))
        if self._raise_on_get_active is not None:
            raise self._raise_on_get_active
        return [dict(r) for r in self._active_rows_by_card.get(str(provider_card_id), [])]

    def prune_recent_sales_superseded(
        self, *, provider_card_id: str,
        kept_provider_sale_keys: list[str],
    ) -> int:
        if self._raise_on_prune is not None:
            raise self._raise_on_prune
        kept = list(kept_provider_sale_keys)
        self.prune_calls.append((str(provider_card_id), kept))
        if self._prune_affected_per_call is not None:
            return self._prune_affected_per_call
        # Realistic: count of seeded active rows not in kept.
        seeded = self._active_rows_by_card.get(str(provider_card_id), [])
        keep_set = {str(k) for k in kept}
        return sum(
            1 for r in seeded if str(r.get("provider_sale_key", "")) not in keep_set
        )


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


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-S-6A — allow-list pagination (Part A)
# ────────────────────────────────────────────────────────────────────────────


class _PaginatingStubSession:
    """Drives the SupabaseClient against a synthetic allow-list of any
    size. Pages the rows out in `page_size` chunks honouring the
    `limit=` / `offset=` query params; this is the contract the new
    `get_allow_list` relies on for EOF detection."""

    def __init__(self, all_rows: list[dict]):
        self.all_rows = all_rows
        self.calls: list[str] = []

    @staticmethod
    def _qs(url: str) -> dict:
        # PostgREST URLs are simple ?a=…&b=… — split by '?' then '&'.
        q = url.split("?", 1)[1] if "?" in url else ""
        out: dict = {}
        for pair in q.split("&"):
            if "=" in pair:
                k, v = pair.split("=", 1)
                out[k] = v
        return out

    def get(self, url, headers, timeout):
        self.calls.append(url)
        qs = self._qs(url)
        try:
            limit = int(qs.get("limit", "1000"))
            offset = int(qs.get("offset", "0"))
        except ValueError:  # pragma: no cover - guard against URL-encoding regressions
            limit, offset = 1000, 0
        page = self.all_rows[offset: offset + limit]
        body = json.dumps(page)

        class _R:
            status_code = 200
            text = body
            def json(_self):
                return json.loads(_self.text)

        return _R()


def test_get_allow_list_pages_through_2500_rows():
    rows = [{"provider_card_id": str(i)} for i in range(1, 2501)]
    sess = _PaginatingStubSession(rows)
    c = SupabaseClient("https://example.supabase.co", "k", session=sess)
    result = c.get_allow_list(page_size=1000)
    assert len(result) == 2500
    # Sanity: matches every input row.
    assert result == {str(i) for i in range(1, 2501)}
    # 1000 + 1000 + 500 → stop after the short page; 3 requests total.
    assert len(sess.calls) == 3


def test_get_allow_list_stops_on_short_page():
    rows = [{"provider_card_id": str(i)} for i in range(1, 1500)]
    sess = _PaginatingStubSession(rows)
    c = SupabaseClient("https://example.supabase.co", "k", session=sess)
    result = c.get_allow_list(page_size=1000)
    assert len(result) == 1499
    # 1000 + 499 → short page on the second request, no third request.
    assert len(sess.calls) == 2


def test_get_allow_list_one_page_when_total_below_page_size():
    # The legacy 58-card case must still issue exactly one request.
    rows = [{"provider_card_id": str(i)} for i in range(1, 59)]
    sess = _PaginatingStubSession(rows)
    c = SupabaseClient("https://example.supabase.co", "k", session=sess)
    result = c.get_allow_list(page_size=1000)
    assert len(result) == 58
    assert len(sess.calls) == 1


def test_get_allow_list_handles_exact_page_size_with_extra_empty_page():
    # Boundary case: total is an exact multiple of page_size. The loader
    # must issue one more (empty) request to learn it has reached EOF.
    rows = [{"provider_card_id": str(i)} for i in range(1, 2001)]  # exactly 2000
    sess = _PaginatingStubSession(rows)
    c = SupabaseClient("https://example.supabase.co", "k", session=sess)
    result = c.get_allow_list(page_size=1000)
    assert len(result) == 2000
    # 1000 + 1000 + 0 (empty short page) → 3 requests.
    assert len(sess.calls) == 3


def test_get_allow_list_includes_order_for_deterministic_paging():
    sess = _PaginatingStubSession([])
    c = SupabaseClient("https://example.supabase.co", "k", session=sess)
    c.get_allow_list(page_size=1000)
    assert sess.calls, "expected at least one request"
    # ORDER BY pins page boundaries against concurrent writes.
    assert "order=provider_card_id" in sess.calls[0]


def test_get_allow_list_legacy_two_row_url_assertions_still_hold():
    # Mirror of the existing legacy test — confirms the URL filter shape
    # didn't drift when we added pagination.
    rows = [{"provider_card_id": "715593"}, {"provider_card_id": 42}]
    sess = _PaginatingStubSession(rows)
    c = SupabaseClient("https://example.supabase.co", "k", session=sess)
    result = c.get_allow_list(page_size=1000)
    assert result == {"715593", "42"}
    url = sess.calls[0]
    assert "provider=eq.pricecharting" in url
    assert "enabled=eq.true" in url
    assert "select=provider_card_id" in url


def test_get_allow_list_safety_ceiling_does_not_infinite_loop(caplog):
    # If somehow PostgREST returned a full page forever (no short page),
    # we must give up rather than spin. Force the condition by having
    # the stub always return ``page_size`` rows.
    class _AlwaysFullSession:
        def __init__(self):
            self.calls = 0
        def get(self, url, headers, timeout):
            self.calls += 1
            body = json.dumps([{"provider_card_id": f"x{self.calls}"}] * 10)
            class _R:
                status_code = 200
                text = body
                def json(_self):
                    return json.loads(_self.text)
            return _R()

    sess = _AlwaysFullSession()
    c = SupabaseClient("https://example.supabase.co", "k", session=sess)
    with caplog.at_level("WARNING", logger="pokeprices.recent_sales_ingestion"):
        c.get_allow_list(page_size=10)
    # Safety ceiling = 10_000_000 // 10 = 1_000_000 — far too many to be
    # the *normal* exit. We just check the loader stopped voluntarily and
    # logged the safety bail.
    assert sess.calls > 0
    assert any("safety ceiling" in r.getMessage() for r in caplog.records)


def test_get_allow_list_rejects_non_positive_page_size():
    c = SupabaseClient("https://example.supabase.co", "k", session=_PaginatingStubSession([]))
    with pytest.raises(ValueError):
        c.get_allow_list(page_size=0)


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-S-6A — grade key normalisation (Part B)
# ────────────────────────────────────────────────────────────────────────────


def _row_for_grade(*, section="completed-auctions-used", company=None, grade=None,
                   sale_date="2026-06-01", provider_sale_key=None,
                   provider_card_id="42", confidence=100) -> SaleRow:
    return SaleRow(
        schema_version=SCHEMA_VERSION, provider="pricecharting",
        provider_card_id=provider_card_id, internal_card_slug=provider_card_id,
        pricecharting_url="https://x", observed_section=section,
        sale_date=sale_date, marketplace_source="ebay", marketplace_country="GB",
        listing_title="Title", sale_price_cents=10000, original_price_cents=None,
        display_currency="USD", source_currency=None,
        grading_company=company, grade=grade,
        condition_text=None, condition_bucket="unknown",
        listing_url="https://x/i", marketplace_item_id="i",
        best_offer_status="not_best_offer", language="en",
        first_edition_status="unknown", variant_text=None,
        provider_sale_key=provider_sale_key or f"k-{provider_card_id}-{sale_date}-{grade or 'raw'}",
        raw_hash="h",
        parser_version=RECENT_SALES_PARSER_VERSION, parse_confidence=confidence,
        parse_status="ok", rejection_reason=None,
        source_attribution=SOURCE_ATTRIBUTION,
    )


@pytest.mark.parametrize("section,company,grade,expected", [
    # Raw section → "Raw" regardless of company/grade
    ("completed-auctions-used", None, None, "Raw"),
    ("completed-auctions-used", "PSA", "PSA 10", "Raw"),
    # Brand-prefixed grade dedupes
    ("completed-auctions-manual-only", "PSA", "PSA 10", "PSA 10"),
    ("completed-auctions-loose-and-box", "BGS", "BGS 10", "BGS 10"),
    # Case-insensitive brand prefix
    ("completed-auctions-graded", "psa", "PSA 9", "PSA 9"),
    # Company + grade without overlap → joined
    ("completed-auctions-graded", "PSA", "9", "PSA 9"),
    # Only one side present
    ("completed-auctions-graded", "PSA", None, "PSA"),
    ("completed-auctions-graded", None, "Grade 9", "Grade 9"),
    # Graded section but no usable info → fallback label
    ("completed-auctions-graded", None, None, "Graded"),
    ("completed-auctions-graded", "", "", "Graded"),
])
def test_grade_key_normalisation(section, company, grade, expected):
    row = _row_for_grade(section=section, company=company, grade=grade)
    assert rsi._grade_key(row) == expected


def test_grade_key_accepts_dict_form():
    # The cap helper accepts pre-converted rows too; same key either way.
    row = _row_for_grade(section="completed-auctions-manual-only",
                         company="PSA", grade="PSA 10")
    assert rsi._grade_key(row) == "PSA 10"
    assert rsi._grade_key(row.to_dict()) == "PSA 10"


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-S-6A — per-grade row cap (Part B)
# ────────────────────────────────────────────────────────────────────────────


def test_cap_rows_per_grade_disabled_when_max_is_none():
    rows = [_row_for_grade(sale_date=f"2026-06-{d:02d}") for d in range(1, 16)]
    kept, dropped = rsi._cap_rows_per_grade(rows, max_per_grade=None)
    assert len(kept) == 15
    assert dropped == 0


@pytest.mark.parametrize("max_per", [0, -1])
def test_cap_rows_per_grade_non_positive_disables_cap(max_per):
    rows = [_row_for_grade(sale_date=f"2026-06-{d:02d}") for d in range(1, 16)]
    kept, dropped = rsi._cap_rows_per_grade(rows, max_per_grade=max_per)
    assert len(kept) == 15
    assert dropped == 0


def test_cap_rows_per_grade_keeps_latest_n_by_sale_date():
    rows = [
        _row_for_grade(sale_date=f"2026-06-{d:02d}", provider_sale_key=f"k{d:02d}")
        for d in range(1, 21)  # 20 rows, dates 06-01 .. 06-20
    ]
    kept, dropped = rsi._cap_rows_per_grade(rows, max_per_grade=10)
    assert len(kept) == 10
    assert dropped == 10
    # Latest 10: dates 06-11..06-20
    kept_dates = sorted(r.sale_date for r in kept)
    assert kept_dates == [f"2026-06-{d:02d}" for d in range(11, 21)]


def test_cap_rows_per_grade_buckets_raw_and_psa10_separately():
    raw_rows = [
        _row_for_grade(section="completed-auctions-used",
                       sale_date=f"2026-06-{d:02d}", provider_sale_key=f"r{d:02d}")
        for d in range(1, 16)  # 15 raw
    ]
    psa10_rows = [
        _row_for_grade(section="completed-auctions-manual-only",
                       company="PSA", grade="PSA 10",
                       sale_date=f"2026-06-{d:02d}", provider_sale_key=f"p{d:02d}")
        for d in range(1, 16)  # 15 PSA 10
    ]
    kept, dropped = rsi._cap_rows_per_grade(
        raw_rows + psa10_rows, max_per_grade=10,
    )
    # 10 raw + 10 PSA 10 survive; 5 + 5 dropped.
    assert len(kept) == 20
    assert dropped == 10
    raw_kept = [r for r in kept if r.observed_section == "completed-auctions-used"]
    psa_kept = [r for r in kept if r.observed_section == "completed-auctions-manual-only"]
    assert len(raw_kept) == 10
    assert len(psa_kept) == 10


def test_cap_rows_per_grade_buckets_per_card_id():
    # Two cards each with 12 raw rows. Cap = 10 per card per grade → 4
    # dropped (2 from each card).
    rows_card1 = [
        _row_for_grade(provider_card_id="1",
                       sale_date=f"2026-06-{d:02d}",
                       provider_sale_key=f"a{d:02d}")
        for d in range(1, 13)
    ]
    rows_card2 = [
        _row_for_grade(provider_card_id="2",
                       sale_date=f"2026-06-{d:02d}",
                       provider_sale_key=f"b{d:02d}")
        for d in range(1, 13)
    ]
    kept, dropped = rsi._cap_rows_per_grade(
        rows_card1 + rows_card2, max_per_grade=10,
    )
    assert len(kept) == 20
    assert dropped == 4
    assert sum(1 for r in kept if r.provider_card_id == "1") == 10
    assert sum(1 for r in kept if r.provider_card_id == "2") == 10


def test_cap_rows_per_grade_uses_confidence_as_secondary_sort():
    same_date = "2026-06-15"
    high = _row_for_grade(sale_date=same_date, confidence=100,
                          provider_sale_key="A")
    low = _row_for_grade(sale_date=same_date, confidence=50,
                         provider_sale_key="B")
    kept, dropped = rsi._cap_rows_per_grade([low, high], max_per_grade=1)
    assert len(kept) == 1
    assert dropped == 1
    assert kept[0] is high  # higher confidence wins the tie


def test_cap_rows_per_grade_uses_provider_sale_key_for_final_tie_break():
    # Identical date + confidence → deterministic ordering by psk.
    rows = [
        _row_for_grade(sale_date="2026-06-15", confidence=100,
                       provider_sale_key=k)
        for k in ["A", "B", "C"]
    ]
    kept_a, _ = rsi._cap_rows_per_grade(rows, max_per_grade=1)
    kept_b, _ = rsi._cap_rows_per_grade(list(reversed(rows)), max_per_grade=1)
    # Same surviving row regardless of input order.
    assert kept_a[0].provider_sale_key == kept_b[0].provider_sale_key


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-S-6A — ingestion controller wiring of the cap
# ────────────────────────────────────────────────────────────────────────────


def _ebay_row_on_date(item_id, date):
    return _ebay_row(item_id=item_id, date=date)


def test_ingestion_caps_ok_rows_per_grade_when_over_limit():
    # Build a single raw section with 12 OK eBay rows on distinct dates.
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(
        fake, allow_list=fake.get_allow_list(),
        max_sales_per_grade=10,
    )
    ig.start()
    bodies = "\n".join(
        _ebay_row_on_date(f"item{d:02d}", f"2026-06-{d:02d}")
        for d in range(1, 13)
    )
    html = _wrap_section("completed-auctions-used", bodies)
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.rows_ok == 12
    assert ig.rows_after_grade_cap == 10
    assert ig.rows_dropped_by_grade_cap == 2
    # Only 10 rows reached the upsert.
    assert sum(len(b) for b in fake.upsert_calls) == 10


def test_ingestion_cap_default_is_five():
    # The class default must match the module constant — that's the
    # single number the workflow assumes. Lowered from 10 to 5 in this
    # revision because the card page only shows the latest 5/grade and
    # daily_prices already holds long-form history.
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    assert ig.max_sales_per_grade == rsi.DEFAULT_MAX_SALES_PER_GRADE == 5


def test_ingestion_cap_can_be_disabled_with_zero():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(
        fake, allow_list=fake.get_allow_list(), max_sales_per_grade=0,
    )
    ig.start()
    bodies = "\n".join(
        _ebay_row_on_date(f"item{d:02d}", f"2026-06-{d:02d}")
        for d in range(1, 13)
    )
    html = _wrap_section("completed-auctions-used", bodies)
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.rows_after_grade_cap == 12
    assert ig.rows_dropped_by_grade_cap == 0
    assert sum(len(b) for b in fake.upsert_calls) == 12


def test_ingestion_cap_under_limit_passes_everything_through():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(
        fake, allow_list=fake.get_allow_list(), max_sales_per_grade=10,
    )
    ig.start()
    bodies = (
        _ebay_row(item_id="11111111111")
        + _ebay_row(item_id="22222222222")
        + _ebay_row(item_id="33333333333")
    )
    html = _wrap_section("completed-auctions-used", bodies)
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.rows_ok == 3
    assert ig.rows_after_grade_cap == 3
    assert ig.rows_dropped_by_grade_cap == 0
    assert sum(len(b) for b in fake.upsert_calls) == 3


def test_ingestion_cap_does_not_affect_quarantined_or_rejected_counts():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(
        fake, allow_list=fake.get_allow_list(), max_sales_per_grade=10,
    )
    ig.start()
    # 12 OK + 1 quarantined + 1 rejected, all in the same raw section.
    bodies = "\n".join(
        _ebay_row_on_date(f"item{d:02d}", f"2026-06-{d:02d}")
        for d in range(1, 13)
    ) + _quarantine_row("99999999999") + _reject_row("88888888888")
    html = _wrap_section("completed-auctions-used", bodies)
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.rows_ok == 12
    assert ig.rows_quarantined == 1
    assert ig.rows_rejected == 1
    # The cap only touches OK rows.
    assert ig.rows_after_grade_cap == 10
    assert ig.rows_dropped_by_grade_cap == 2


def test_ingestion_dry_run_still_counts_cap_kept_and_dropped():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(
        fake, allow_list=fake.get_allow_list(), max_sales_per_grade=5, dry_run=True,
    )
    ig.start()
    bodies = "\n".join(
        _ebay_row_on_date(f"item{d:02d}", f"2026-06-{d:02d}")
        for d in range(1, 13)
    )
    html = _wrap_section("completed-auctions-used", bodies)
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    # No DB writes, but counters still reflect what would have been written.
    assert ig.rows_after_grade_cap == 5
    assert ig.rows_dropped_by_grade_cap == 7
    assert fake.upsert_calls == []


def test_ingestion_notes_carry_max_sales_per_grade_and_counters():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(
        fake, allow_list=fake.get_allow_list(), max_sales_per_grade=10,
    )
    ig.start()
    bodies = "\n".join(
        _ebay_row_on_date(f"item{d:02d}", f"2026-06-{d:02d}")
        for d in range(1, 13)
    )
    html = _wrap_section("completed-auctions-used", bodies)
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    _, payload = fake.market_run_updates[0]
    notes = json.loads(payload["notes"])
    assert notes["max_sales_per_grade"] == 10
    assert notes["rows_after_grade_cap"] == 10
    assert notes["rows_dropped_by_grade_cap"] == 2


def test_ingestion_notes_carry_max_sales_per_grade_none_when_disabled():
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(
        fake, allow_list=fake.get_allow_list(), max_sales_per_grade=None,
    )
    ig.start()
    ig.finish()
    _, payload = fake.market_run_updates[0]
    notes = json.loads(payload["notes"])
    assert notes["max_sales_per_grade"] is None
    assert notes["rows_after_grade_cap"] == 0
    assert notes["rows_dropped_by_grade_cap"] == 0


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-S-6A (revised) — cap=5 behaviour, raw vs PSA 10 buckets
# ────────────────────────────────────────────────────────────────────────────


def test_ingestion_default_cap_keeps_only_latest_five_raw_rows():
    # 12 raw rows with distinct dates 2026-06-01 .. 2026-06-12. Default
    # cap=5 → only the latest five 06-08 .. 06-12 survive.
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    bodies = "\n".join(
        _ebay_row_on_date(f"item{d:02d}", f"2026-06-{d:02d}")
        for d in range(1, 13)
    )
    html = _wrap_section("completed-auctions-used", bodies)
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.rows_ok == 12
    assert ig.rows_after_grade_cap == 5
    assert ig.rows_dropped_by_grade_cap == 7
    written = [r for batch in fake.upsert_calls for r in batch]
    assert len(written) == 5
    written_dates = sorted(r["sale_date"] for r in written)
    assert written_dates == [f"2026-06-{d:02d}" for d in range(8, 13)]


def test_ingestion_default_cap_buckets_raw_and_psa10_independently():
    # 8 raw + 8 PSA 10 → 5 + 5 survive, 3 + 3 dropped.
    raw_rows = [
        _row_for_grade(
            section="completed-auctions-used",
            sale_date=f"2026-06-{d:02d}",
            provider_sale_key=f"r{d:02d}",
        )
        for d in range(1, 9)
    ]
    psa10_rows = [
        _row_for_grade(
            section="completed-auctions-manual-only",
            company="PSA", grade="PSA 10",
            sale_date=f"2026-06-{d:02d}",
            provider_sale_key=f"p{d:02d}",
        )
        for d in range(1, 9)
    ]
    kept, dropped = rsi._cap_rows_per_grade(
        raw_rows + psa10_rows, max_per_grade=5,
    )
    assert len(kept) == 10  # 5 raw + 5 PSA 10
    assert dropped == 6     # 3 raw + 3 PSA 10
    raw_kept = [r for r in kept if r.observed_section == "completed-auctions-used"]
    psa_kept = [r for r in kept if r.observed_section == "completed-auctions-manual-only"]
    assert len(raw_kept) == 5
    assert len(psa_kept) == 5
    # Newest 5 of each bucket survive.
    assert sorted(r.sale_date for r in raw_kept) == [f"2026-06-{d:02d}" for d in range(4, 9)]
    assert sorted(r.sale_date for r in psa_kept) == [f"2026-06-{d:02d}" for d in range(4, 9)]


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-S-6A (revised) — inline supersede prune
# ────────────────────────────────────────────────────────────────────────────


def test_prune_called_card_wide_after_upsert():
    # Grade-aware prune: exactly ONE PATCH per processed card (not one
    # per observed_section). The keep set is the latest-N-per-grade
    # across the union of existing active + scrape rows; here no
    # existing rows are seeded, so the keep set equals the scrape's
    # kept psks.
    fake = FakeSupabaseClient(allow_list={"42"}, prune_affected_per_call=3)
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section(
        "completed-auctions-used",
        _ebay_row(item_id="11111111111") + _ebay_row(item_id="22222222222"),
    ) + _wrap_section(
        "completed-auctions-manual-only",
        _ebay_row(item_id="33333333333") + _ebay_row(item_id="44444444444"),
    )
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    # ONE prune call card-wide (not one per section).
    assert len(fake.prune_calls) == 1
    (pcid, kept_psks) = fake.prune_calls[0]
    assert pcid == "42"
    # 4 scrape rows → 4 kept psks survive (cap = 5, well under).
    assert len(kept_psks) == 4
    # The prune fetches existing-active first.
    assert fake.get_active_calls == ["42"]
    # Counter reflects the FakeSupabaseClient's return value.
    assert ig.rows_pruned_old_active == 3


def test_prune_not_called_when_dry_run():
    fake = FakeSupabaseClient(allow_list={"42"}, prune_affected_per_call=5)
    ig = RecentSalesIngestion(
        fake, allow_list=fake.get_allow_list(), dry_run=True,
    )
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    assert fake.prune_calls == []
    assert ig.rows_pruned_old_active == 0


def test_prune_not_called_when_prune_inline_false():
    fake = FakeSupabaseClient(allow_list={"42"}, prune_affected_per_call=5)
    ig = RecentSalesIngestion(
        fake, allow_list=fake.get_allow_list(), prune_inline=False,
    )
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert fake.prune_calls == []
    assert ig.rows_pruned_old_active == 0


def test_prune_failure_is_caught_and_counted():
    fake = FakeSupabaseClient(
        allow_list={"42"},
        raise_on_prune=RuntimeError("simulated PGRST 400 — review_status missing"),
    )
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    # Must not raise: prune errors are caught + logged + counted.
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.errors_count >= 1
    assert ig.rows_pruned_old_active == 0
    # Upsert still succeeded; the kept rows are written.
    assert sum(len(b) for b in fake.upsert_calls) == 1


def test_prune_not_called_when_no_ok_rows_to_keep():
    # Card produces only a quarantined row → kept_rows is empty → no
    # prune call (we don't issue queries that would match every OK row).
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _quarantine_row("22222222222"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert ig.rows_ok == 0
    assert fake.prune_calls == []


def test_prune_notes_include_rows_pruned_old_active():
    fake = FakeSupabaseClient(allow_list={"42"}, prune_affected_per_call=7)
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    _, payload = fake.market_run_updates[0]
    notes = json.loads(payload["notes"])
    assert notes["rows_pruned_old_active"] == 7


def test_prune_kept_psks_are_exactly_the_kept_set():
    # Cap=5, 8 raw rows → only the latest 5 PSKs land in the prune call.
    fake = FakeSupabaseClient(allow_list={"42"})
    ig = RecentSalesIngestion(
        fake, allow_list=fake.get_allow_list(), max_sales_per_grade=5,
    )
    ig.start()
    bodies = "\n".join(
        _ebay_row_on_date(f"item{d:02d}", f"2026-06-{d:02d}")
        for d in range(1, 9)  # 8 rows
    )
    html = _wrap_section("completed-auctions-used", bodies)
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    assert len(fake.prune_calls) == 1
    (_pcid, kept_psks) = fake.prune_calls[0]
    # 5 kept PSKs — same number as the cap, all non-empty.
    assert len(kept_psks) == 5
    assert all(isinstance(k, str) and k for k in kept_psks)


# SupabaseClient-level: URL shape + return value of the prune PATCH.

def test_supabase_client_prune_url_and_body_shape():
    captured: dict = {}

    class StubSession:
        def patch(self, url, json, headers, timeout):
            captured["url"] = url
            captured["body"] = json
            captured["headers"] = headers
            class _R:
                status_code = 200
                text = '[{"id":1},{"id":2},{"id":3}]'
                def json(_self):
                    import json as _j
                    return _j.loads(_self.text)
            return _R()

    c = SupabaseClient("https://example.supabase.co", "k", session=StubSession())
    n = c.prune_recent_sales_superseded(
        provider_card_id="42",
        kept_provider_sale_keys=["abc", "def", "ghi"],
    )
    assert n == 3
    # Card-wide PATCH: no observed_section partition.
    assert "provider_card_id=eq.42" in captured["url"]
    assert "observed_section=" not in captured["url"]
    assert "parse_status=eq.ok" in captured["url"]
    assert "review_status=eq.active" in captured["url"]
    assert "provider_sale_key=not.in.(abc,def,ghi)" in captured["url"]
    assert captured["body"] == {"review_status": "superseded"}
    assert "return=representation" in captured["headers"]["Prefer"]


def test_supabase_client_prune_no_op_with_empty_kept_set():
    # Defensive — empty kept set must not issue an HTTP call (would
    # otherwise mark every OK row for the card as superseded).
    calls: list = []

    class StubSession:
        def patch(self, url, json, headers, timeout):
            calls.append(url)
            class _R:
                status_code = 200
                text = "[]"
            return _R()

    c = SupabaseClient("https://example.supabase.co", "k", session=StubSession())
    n = c.prune_recent_sales_superseded(
        provider_card_id="42",
        kept_provider_sale_keys=[],
    )
    assert n == 0
    assert calls == []


def test_supabase_client_prune_handles_204_no_content():
    class StubSession:
        def patch(self, url, json, headers, timeout):
            class _R:
                status_code = 204
                text = ""
            return _R()

    c = SupabaseClient("https://example.supabase.co", "k", session=StubSession())
    n = c.prune_recent_sales_superseded(
        provider_card_id="42",
        kept_provider_sale_keys=["abc"],
    )
    assert n == 0


def test_supabase_client_prune_raises_on_pgrst_error():
    class StubSession:
        def patch(self, url, json, headers, timeout):
            class _R:
                status_code = 400
                text = '{"message":"column \\"review_status\\" does not exist"}'
            return _R()

    c = SupabaseClient("https://example.supabase.co", "k", session=StubSession())
    with pytest.raises(RuntimeError, match="prune failed"):
        c.prune_recent_sales_superseded(
            provider_card_id="42",
            kept_provider_sale_keys=["abc"],
        )


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-S-6A (grade-aware prune) — get_active_recent_sales_for_card
# ────────────────────────────────────────────────────────────────────────────


def test_supabase_client_get_active_url_filters_card_and_status():
    captured: dict = {}

    class StubSession:
        def get(self, url, headers, timeout):
            captured["url"] = url
            class _R:
                status_code = 200
                text = '[{"provider_sale_key":"k1","sale_date":"2026-06-15"}]'
                def json(_self):
                    import json as _j
                    return _j.loads(_self.text)
            return _R()

    c = SupabaseClient("https://example.supabase.co", "k", session=StubSession())
    rows = c.get_active_recent_sales_for_card(provider_card_id="42")
    assert len(rows) == 1
    assert "provider_card_id=eq.42" in captured["url"]
    assert "parse_status=eq.ok" in captured["url"]
    assert "review_status=eq.active" in captured["url"]
    # The grade-key columns must be in the select clause so the combined
    # cap can re-bucket DB rows against scrape rows.
    for col in ("provider_sale_key", "observed_section", "grading_company",
                "grade", "raw_or_graded", "sale_date", "parse_confidence"):
        assert col in captured["url"]


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-S-6A (grade-aware prune) — _compute_card_final_keep helper
# ────────────────────────────────────────────────────────────────────────────


def _existing_db_row(*, psk, section, company=None, grade=None,
                     raw_or_graded=None, sale_date, confidence=100):
    return {
        "provider_sale_key": psk,
        "observed_section": section,
        "grading_company": company,
        "grade": grade,
        "raw_or_graded": raw_or_graded,
        "sale_date": sale_date,
        "parse_confidence": confidence,
    }


def test_compute_card_final_keep_unions_existing_and_scrape():
    existing = [
        _existing_db_row(psk="e1", section="completed-auctions-used",
                         raw_or_graded="raw", sale_date="2026-05-01"),
        _existing_db_row(psk="e2", section="completed-auctions-used",
                         raw_or_graded="raw", sale_date="2026-05-02"),
    ]
    scrape = [
        _row_for_grade(section="completed-auctions-used",
                       sale_date="2026-06-15", provider_sale_key="s1"),
    ]
    keep, to_upsert = rsi._compute_card_final_keep(
        provider_card_id="42",
        existing_active_rows=existing,
        scrape_kept_rows=scrape,
        max_per_grade=5,
    )
    # 3 rows total, cap=5 → all 3 kept.
    assert keep == {"e1", "e2", "s1"}
    # The scrape row survived the combined cap → included in the upsert set.
    assert [r.provider_sale_key for r in to_upsert] == ["s1"]


def test_compute_card_final_keep_drops_scrape_rows_outside_top_n():
    # 5 existing rows with newer dates than the scrape row → scrape
    # row's psk is NOT in final_keep → it must not be upserted.
    existing = [
        _existing_db_row(psk=f"e{i}", section="completed-auctions-used",
                         raw_or_graded="raw",
                         sale_date=f"2026-06-{20+i:02d}")
        for i in range(1, 6)  # dates 06-21 .. 06-25
    ]
    scrape = [
        _row_for_grade(section="completed-auctions-used",
                       sale_date="2026-05-01", provider_sale_key="old"),
    ]
    keep, to_upsert = rsi._compute_card_final_keep(
        provider_card_id="42",
        existing_active_rows=existing,
        scrape_kept_rows=scrape,
        max_per_grade=5,
    )
    assert keep == {"e1", "e2", "e3", "e4", "e5"}
    # The scrape row falls out of the top 5 by date → not upserted.
    assert to_upsert == []


def test_compute_card_final_keep_buckets_psa9_and_psa10_independently():
    # Both rows share observed_section='completed-auctions-graded' (a
    # generic graded section) but differ in (company, grade). A
    # section-partitioned prune would have lumped them together — the
    # grade-aware cap must keep them apart.
    psa9 = [
        _existing_db_row(psk=f"p9-{i}", section="completed-auctions-graded",
                         company="PSA", grade="PSA 9",
                         sale_date=f"2026-06-{i:02d}")
        for i in range(1, 7)  # 6 rows
    ]
    psa10 = [
        _existing_db_row(psk=f"p10-{i}", section="completed-auctions-graded",
                         company="PSA", grade="PSA 10",
                         sale_date=f"2026-06-{i:02d}")
        for i in range(1, 7)  # 6 rows
    ]
    keep, _ = rsi._compute_card_final_keep(
        provider_card_id="42",
        existing_active_rows=psa9 + psa10,
        scrape_kept_rows=[],
        max_per_grade=5,
    )
    # 5 PSA 9 + 5 PSA 10 survive.
    assert len({k for k in keep if k.startswith("p9-")}) == 5
    assert len({k for k in keep if k.startswith("p10-")}) == 5


def test_compute_card_final_keep_scrape_wins_on_psk_collision():
    # Same psk in existing and scrape → scrape's parsed metadata wins.
    existing = [_existing_db_row(psk="dup", section="completed-auctions-used",
                                 raw_or_graded="raw", sale_date="2026-05-01")]
    scrape = [
        _row_for_grade(section="completed-auctions-used",
                       sale_date="2026-06-15", provider_sale_key="dup"),
    ]
    keep, to_upsert = rsi._compute_card_final_keep(
        provider_card_id="42",
        existing_active_rows=existing,
        scrape_kept_rows=scrape,
        max_per_grade=5,
    )
    assert keep == {"dup"}
    # The scrape row is upserted (it overwrites in the merge), so the
    # combined cap sees the *fresh* sale_date — and the freshness
    # matters when the cap has to pick winners.
    assert [r.provider_sale_key for r in to_upsert] == ["dup"]


def test_compute_card_final_keep_empty_inputs_returns_empty():
    keep, to_upsert = rsi._compute_card_final_keep(
        provider_card_id="42",
        existing_active_rows=[],
        scrape_kept_rows=[],
        max_per_grade=5,
    )
    assert keep == set()
    assert to_upsert == []


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-S-6A (grade-aware prune) — end-to-end via RecentSalesIngestion
# ────────────────────────────────────────────────────────────────────────────


def test_ingestion_prune_supersedes_existing_rows_outside_combined_keep():
    # 7 existing raw rows + a scrape that adds 1 fresher raw row.
    # Combined cap (5) → newest 5 survive (4 existing + 1 scrape).
    # PATCH supersedes the 3 oldest existing rows.
    existing = [
        _existing_db_row(psk=f"e{i}", section="completed-auctions-used",
                         raw_or_graded="raw",
                         sale_date=f"2026-06-{i:02d}")
        for i in range(1, 8)  # 7 rows, dates 06-01 .. 06-07
    ]
    fake = FakeSupabaseClient(
        allow_list={"42"}, active_rows_by_card={"42": existing},
    )
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    # Scrape adds one row dated 06-15 (newest of all).
    html = _wrap_section(
        "completed-auctions-used",
        _ebay_row_on_date("item-new", "2026-06-15"),
    )
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    # One PATCH for the card.
    assert len(fake.prune_calls) == 1
    (_pcid, kept_psks) = fake.prune_calls[0]
    keep_set = set(kept_psks)
    # New row + 4 newest existing survive.
    assert "e7" in keep_set and "e6" in keep_set
    assert "e5" in keep_set and "e4" in keep_set
    # Scrape row's psk is in the keep set too.
    scrape_psk = next(iter(k for k in keep_set if not k.startswith("e")))
    assert scrape_psk
    # Total kept = 5.
    assert len(keep_set) == 5
    # FakeSupabaseClient reports affected = (seeded rows) - (kept ∩ seeded)
    # = 7 - 4 = 3.
    assert ig.rows_pruned_old_active == 3


def test_ingestion_prune_does_not_upsert_scrape_rows_dropped_by_combined_cap():
    # 5 existing rows, all newer than the scrape row → scrape row falls
    # out of the top 5 → must not be upserted.
    existing = [
        _existing_db_row(psk=f"e{i}", section="completed-auctions-used",
                         raw_or_graded="raw",
                         sale_date=f"2026-06-{20+i:02d}")
        for i in range(1, 6)  # 5 rows, dates 06-21 .. 06-25
    ]
    fake = FakeSupabaseClient(
        allow_list={"42"}, active_rows_by_card={"42": existing},
    )
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section(
        "completed-auctions-used",
        _ebay_row_on_date("item-old", "2026-05-01"),  # older than all
    )
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    # No upsert (the only scrape row was dropped from the combined cap).
    assert fake.upsert_calls == []
    # Prune still ran (the 5 existing rows are exactly the keep set →
    # no rows superseded, but the PATCH was issued).
    assert len(fake.prune_calls) == 1
    (_pcid, kept_psks) = fake.prune_calls[0]
    assert set(kept_psks) == {"e1", "e2", "e3", "e4", "e5"}


def test_ingestion_prune_fetches_active_rows_per_card():
    fake = FakeSupabaseClient(allow_list={"42", "99"})
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.maybe_ingest(html=html, provider_card_id="99", page_url=PAGE_URL)
    ig.finish()
    # One get-active call per processed card.
    assert fake.get_active_calls == ["42", "99"]


def test_ingestion_falls_back_to_scrape_only_when_get_active_fails():
    fake = FakeSupabaseClient(
        allow_list={"42"},
        raise_on_get_active=RuntimeError("simulated PGRST 500"),
    )
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    html = _wrap_section("completed-auctions-used", _ebay_row(item_id="11111111111"))
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    # Read failed → no prune attempted.
    assert fake.prune_calls == []
    # But the scrape row still got upserted (cap-on-write keeps the
    # table size bounded even without the prune).
    assert sum(len(b) for b in fake.upsert_calls) == 1
    # And the error is counted.
    assert ig.errors_count >= 1


def test_ingestion_prune_psa9_and_psa10_share_section_kept_independently_e2e():
    # 6 PSA 9 + 6 PSA 10 already in DB, both under
    # observed_section='completed-auctions-graded' (a generic section
    # that doesn't pin grading_company by itself). A section-partitioned
    # prune would lump the 12 rows into one bucket and supersede 7. The
    # grade-aware prune must keep 5 of each → supersede exactly 2.
    existing = [
        _existing_db_row(psk=f"p9-{i}", section="completed-auctions-graded",
                         company="PSA", grade="PSA 9",
                         sale_date=f"2026-06-{i:02d}")
        for i in range(1, 7)
    ] + [
        _existing_db_row(psk=f"p10-{i}", section="completed-auctions-graded",
                         company="PSA", grade="PSA 10",
                         sale_date=f"2026-06-{i:02d}")
        for i in range(1, 7)
    ]
    fake = FakeSupabaseClient(
        allow_list={"42"}, active_rows_by_card={"42": existing},
    )
    ig = RecentSalesIngestion(fake, allow_list=fake.get_allow_list())
    ig.start()
    # Scrape produces a raw row so the prune block fires; the scrape's
    # raw row + the existing graded rows are unioned for the cap.
    html = _wrap_section(
        "completed-auctions-used",
        _ebay_row_on_date("item-new", "2026-06-15"),
    )
    ig.maybe_ingest(html=html, provider_card_id="42", page_url=PAGE_URL)
    ig.finish()
    # One prune PATCH for the card.
    assert len(fake.prune_calls) == 1
    (_pcid, kept_psks) = fake.prune_calls[0]
    keep_set = set(kept_psks)
    # Top-5 PSA 9 (dates 06-02..06-06) survived.
    surviving_p9 = {k for k in keep_set if k.startswith("p9-")}
    assert len(surviving_p9) == 5
    assert "p9-1" not in surviving_p9   # oldest PSA 9 dropped
    # Top-5 PSA 10 (dates 06-02..06-06) survived.
    surviving_p10 = {k for k in keep_set if k.startswith("p10-")}
    assert len(surviving_p10) == 5
    assert "p10-1" not in surviving_p10  # oldest PSA 10 dropped
    # The scrape's new raw row (its own bucket → "Raw") also survives.
    raw_keep = [k for k in keep_set if not k.startswith("p9-")
                and not k.startswith("p10-")]
    assert len(raw_keep) == 1
    # Total keep = 5 + 5 + 1 = 11; affected = 12 existing - 10 kept = 2.
    assert ig.rows_pruned_old_active == 2
