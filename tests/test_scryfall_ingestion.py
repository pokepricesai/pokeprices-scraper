"""Unit tests for scryfall_ingestion transforms + streaming parser.

Network-free. Uses fake HTTP responses via a minimal session fake, and
fake Supabase writes via an in-memory client. Runs cleanly under
Python 3.14 with only stdlib + pytest.
"""

from __future__ import annotations

import io
import json
import gzip
from typing import Any

import pytest

import scryfall_ingestion as si


# ─── Transform tests ────────────────────────────────────────────────────────


def test_transform_set_ok():
    scryfall = {
        "object": "set",
        "id":               "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "code":             "NEO",
        "name":             "Kamigawa: Neon Dynasty",
        "set_type":         "expansion",
        "released_at":      "2022-02-18",
        "card_count":       302,
        "digital":          False,
        "foil_only":        False,
        "nonfoil_only":     False,
        "icon_svg_uri":     "https://svgs.scryfall.io/sets/neo.svg",
        "scryfall_uri":     "https://scryfall.com/sets/neo",
        "parent_set_code":  None,
        "block":            None,
        "block_code":       None,
    }
    row = si.transform_set(scryfall)
    assert row is not None
    assert row["code"] == "neo"                   # lower-cased
    assert row["scryfall_id"] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    assert row["name"] == "Kamigawa: Neon Dynasty"
    assert row["set_type"] == "expansion"
    assert row["parent_set_code"] is None


def test_transform_set_rejects_missing_code_or_name():
    assert si.transform_set({"code": "neo"}) is None      # missing name
    assert si.transform_set({"name": "X"}) is None        # missing code
    assert si.transform_set({}) is None


def test_transform_oracle_card_ok():
    card = {
        "object":          "card",
        "oracle_id":       "11111111-2222-3333-4444-555555555555",
        "name":            "Lightning Bolt",
        "mana_cost":       "{R}",
        "cmc":             1.0,
        "type_line":       "Instant",
        "oracle_text":     "Lightning Bolt deals 3 damage to any target.",
        "power":           None,
        "toughness":       None,
        "loyalty":         None,
        "defense":         None,
        "colors":          ["R"],
        "color_identity":  ["R"],
        "keywords":        [],
        "produced_mana":   None,
        "reserved":        False,
        "layout":          "normal",
        "game_changer":    False,
        "card_faces":      None,
        "legalities": {
            "standard":  "not_legal",
            "modern":    "legal",
            "legacy":    "legal",
            "vintage":   "legal",
            "commander": "legal",
            "pauper":    "legal",
        },
    }
    row = si.transform_oracle_card(card)
    assert row is not None
    assert row["oracle_id"] == "11111111-2222-3333-4444-555555555555"
    assert row["name"] == "Lightning Bolt"
    assert row["mana_value"] == 1.0
    assert row["colors"] == ["R"]
    assert row["reserved"] is False
    assert row["layout"] == "normal"
    # card_faces stays None on single-face cards
    assert row["card_faces"] is None


def test_transform_oracle_card_multiface():
    card = {
        "oracle_id": "aa",
        "name":      "Delver of Secrets // Insectile Aberration",
        "layout":    "transform",
        "cmc":       1.0,
        "type_line": "Creature — Human Wizard // Creature — Human Insect",
        "colors":    ["U"],
        "card_faces": [
            {"name": "Delver of Secrets", "mana_cost": "{U}"},
            {"name": "Insectile Aberration", "mana_cost": ""},
        ],
    }
    row = si.transform_oracle_card(card)
    assert row is not None
    assert row["layout"] == "transform"
    assert row["card_faces"] is not None
    assert len(row["card_faces"]) == 2


def test_transform_oracle_card_rejects_missing_oracle_id():
    assert si.transform_oracle_card({"name": "X"}) is None
    assert si.transform_oracle_card({"oracle_id": "x", "name": ""}) is None
    assert si.transform_oracle_card({"oracle_id": "x"}) is None


def test_transform_printing_body_ok():
    card = {
        "object":            "card",
        "id":                "cccccccc-dddd-eeee-ffff-000000000000",
        "oracle_id":         "11111111-2222-3333-4444-555555555555",
        "name":              "Lightning Bolt",
        "set":               "M11",     # note uppercase → must be lowercased
        "collector_number":  "146",
        "lang":              "en",
        "layout":            "normal",
        "rarity":            "common",
        "artist":            "Christopher Moeller",
        "illustration_id":   "abababab-cdcd-efef-0101-020202020202",
        "image_uris": {
            "normal":   "https://example.com/n.jpg",
            "small":    "https://example.com/s.jpg",
            "art_crop": "https://example.com/a.jpg",
        },
        "promo":       False,
        "reprint":     True,
        "variation":   False,
        "full_art":    False,
        "textless":    False,
        "digital":     False,
        "released_at": "2010-07-16",
        "scryfall_uri": "https://scryfall.com/card/m11/146/lightning-bolt",
        "border_color": "black",
    }
    body = si.transform_printing_body(card)
    assert body is not None
    assert body["scryfall_id"] == "cccccccc-dddd-eeee-ffff-000000000000"
    assert body["set_code"] == "m11"
    assert body["image_uri"] == "https://example.com/n.jpg"
    assert body["image_uri_small"] == "https://example.com/s.jpg"
    assert body["borderless"] is False
    # oracle_card_id / set_id are resolved by the orchestrator, not the
    # transform.
    assert "oracle_card_id" not in body
    assert "set_id" not in body


def test_transform_printing_body_rejects_no_oracle_id():
    """A non-Oracle Scryfall object (token/emblem/art card) is quarantined."""
    art_card = {
        "id":  "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        "set": "SLD",
        "name": "Full Art Something",
        "layout": "art_series",
        # note: no oracle_id
    }
    assert si.transform_printing_body(art_card) is None


def test_transform_finishes_from_array():
    assert si.transform_finishes({"finishes": ["nonfoil", "foil"]}) == ["nonfoil", "foil"]
    assert si.transform_finishes({"finishes": ["nonfoil", "foil", "etched"]}) == ["nonfoil", "foil", "etched"]
    # legacy fallback if the array is missing
    assert si.transform_finishes({"finishes": None, "nonfoil": True, "foil": False}) == ["nonfoil"]
    assert si.transform_finishes({"nonfoil": True, "foil": True}) == ["nonfoil", "foil"]
    assert si.transform_finishes({}) == []


def test_transform_legalities():
    m = {
        "standard":  "not_legal",
        "modern":    "legal",
        "commander": "legal",
        "pauper":    "not_legal",
    }
    pairs = si.transform_legalities({"legalities": m})
    assert set(pairs) == {
        ("standard", "not_legal"),
        ("modern", "legal"),
        ("commander", "legal"),
        ("pauper", "not_legal"),
    }
    assert si.transform_legalities({"legalities": None}) == []
    assert si.transform_legalities({}) == []


def test_transform_ruling_body_ok():
    rl = {
        "object":       "ruling",
        "oracle_id":    "11111111-2222-3333-4444-555555555555",
        "source":       "wotc",
        "published_at": "2004-10-04",
        "comment":      "Lightning Bolt deals damage as its resolution.",
    }
    body = si.transform_ruling_body(rl)
    assert body is not None
    assert body["oracle_id"] == "11111111-2222-3333-4444-555555555555"
    assert body["source"] == "wotc"
    assert body["published_at"] == "2004-10-04"
    assert body["comment"].startswith("Lightning Bolt")


def test_transform_ruling_body_rejects_missing():
    assert si.transform_ruling_body({}) is None
    assert si.transform_ruling_body({"oracle_id": "x"}) is None
    assert si.transform_ruling_body({"oracle_id": "x", "source": "wotc"}) is None


# ─── Streaming JSON-array parser tests ──────────────────────────────────────


class _FakeResp:
    """Minimal fake requests.Response for iter_content()/raw path."""
    def __init__(self, payload: bytes, *, gzipped: bool = False, http_encoding: str | None = None):
        self._payload = payload
        self.raw = io.BytesIO(payload)
        self.headers = {"Content-Encoding": http_encoding} if http_encoding else {}
        self._gzipped = gzipped
        self.status_code = 200

    def iter_content(self, chunk_size: int = 8192):
        for i in range(0, len(self._payload), chunk_size):
            yield self._payload[i : i + chunk_size]


class _FakeScryfallSession:
    """A requests.Session-shaped fake that returns fixed payloads by URL."""
    def __init__(self, responses: dict[str, _FakeResp]):
        self._responses = responses
        self.headers: dict[str, str] = {}

    def get(self, url: str, *, timeout: float = 60.0, stream: bool = False, **_ignored):
        resp = self._responses.get(url)
        if resp is None:
            raise RuntimeError(f"no fake response registered for {url}")
        return resp


def _client_with_responses(responses: dict[str, _FakeResp]) -> si.ScryfallClient:
    session = _FakeScryfallSession(responses)
    return si.ScryfallClient(session=session, timeout=1.0, max_retries=0, retry_backoff=0)


def _jsonl(*records: dict) -> bytes:
    """Encode records as JSONL (newline-delimited JSON)."""
    return ("\n".join(json.dumps(r) for r in records) + "\n").encode("utf-8")


def test_iter_bulk_records_parses_jsonl():
    payload = _jsonl(
        {"object": "card", "name": "A"},
        {"object": "card", "name": "B"},
        {"object": "card", "name": "C"},
    )
    responses = {"https://example.com/bulk.jsonl": _FakeResp(payload)}
    client = _client_with_responses(responses)
    got = list(client.iter_bulk_records("https://example.com/bulk.jsonl"))
    assert [r["name"] for r in got] == ["A", "B", "C"]


def test_iter_bulk_records_handles_split_cards_and_escapes_in_jsonl():
    payload = _jsonl(
        {"name": "Split // Card", "faces": [{"name": "Split"}, {"name": "Card"}]},
        {"comment": "text with } and { and \" and \\ chars"},
    )
    responses = {"https://example.com/bulk.jsonl": _FakeResp(payload)}
    client = _client_with_responses(responses)
    got = list(client.iter_bulk_records("https://example.com/bulk.jsonl"))
    assert len(got) == 2
    assert got[0]["name"] == "Split // Card"
    assert got[1]["comment"].startswith("text with }")


def test_iter_bulk_records_tolerates_blank_lines_and_legacy_array_brackets():
    """A file that starts with '[' and ends with ']' (legacy JSON-array
    delivery) must still parse — we skip the wrapper lines, tolerate
    trailing commas, and blank lines."""
    payload = b'[\n{"name": "A"},\n\n{"name": "B"}\n]\n'
    responses = {"https://example.com/mixed.jsonl": _FakeResp(payload)}
    client = _client_with_responses(responses)
    got = list(client.iter_bulk_records("https://example.com/mixed.jsonl"))
    assert [r["name"] for r in got] == ["A", "B"]


def test_iter_bulk_records_empty():
    responses = {"https://example.com/bulk.jsonl": _FakeResp(b"")}
    client = _client_with_responses(responses)
    assert list(client.iter_bulk_records("https://example.com/bulk.jsonl")) == []


def test_iter_bulk_records_gzipped_payload():
    """Scryfall's current delivery: application-level gzip on a JSONL
    body. HTTP Content-Encoding is NOT gzip — the file itself is gzip.
    We detect via the 1f 8b magic bytes on the raw payload."""
    inner = _jsonl(
        {"name": "gzipped card A"},
        {"name": "gzipped card B"},
    )
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(inner)
    payload = buf.getvalue()
    responses = {"https://example.com/bulk.jsonl.gz": _FakeResp(payload)}
    client = _client_with_responses(responses)
    got = list(client.iter_bulk_records("https://example.com/bulk.jsonl.gz"))
    assert [r["name"] for r in got] == ["gzipped card A", "gzipped card B"]


# ─── Fake Supabase + orchestrator smoke test ────────────────────────────────


class _FakeSupabase:
    """In-memory Supabase surface. Records writes without a real network."""
    def __init__(self):
        self.calls: list[dict] = []
        self.tables: dict[str, list[dict]] = {}
        self.run_id: str | None = None
        self.market_run_update: dict | None = None
        # Preloaded id-maps so orchestrator FK-resolution has data
        # without needing an INSERT roundtrip.
        self._oracle_map: dict[str, str] = {}
        self._set_map: dict[str, str] = {}
        self._printing_map: dict[str, str] = {}

    def seed_oracle_map(self, m): self._oracle_map.update(m)
    def seed_set_map(self, m):    self._set_map.update(m)
    def seed_printing_map(self, m): self._printing_map.update(m)

    def upsert(self, table, rows, *, on_conflict, return_representation=False):
        self.calls.append({"op": "upsert", "table": table, "rows": len(rows), "on_conflict": on_conflict})
        self.tables.setdefault(table, []).extend(rows)
        return len(rows)

    def lookup_id_map(self, table, *, natural_key, id_col="id", page_size=1000):
        if table == si.T_ORACLE_CARDS: return dict(self._oracle_map)
        if table == si.T_SETS:         return dict(self._set_map)
        if table == si.T_PRINTINGS:    return dict(self._printing_map)
        return {}

    def insert_market_run(self, payload):
        self.calls.append({"op": "insert_market_run", "payload": payload})
        self.run_id = "test-run-id"
        return self.run_id

    def update_market_run(self, run_id, payload):
        self.market_run_update = {"run_id": run_id, "payload": payload}
        return True


def test_orchestrator_dry_run_sets_only(monkeypatch):
    """Sets-only smoke test with faked Scryfall + Supabase.

    Confirms: sets are read, transformed, and would have been upserted;
    dry-run skips actual writes; market_import_runs row NOT opened.
    """
    fake_sets = [
        {"object": "set", "id": "s1", "code": "NEO", "name": "Kamigawa: Neon Dynasty"},
        {"object": "set", "id": "s2", "code": "M11", "name": "Magic 2011"},
    ]
    class _FakeScryfall:
        def get_bulk_metadata(self):
            return {}
        def iter_sets(self):
            yield from fake_sets
        def iter_bulk_records(self, *a, **kw):
            return iter([])
    fake_supa = _FakeSupabase()
    ing = si.ScryfallCatalogueIngestion(
        fake_supa, _FakeScryfall(),
        dry_run=True,
        run_sets=True, run_oracle=False, run_legalities=False,
        run_printings=False, run_finishes=False, run_rulings=False,
    )
    ing.start()
    ing.run()
    ing.finish(status="success")
    assert ing.stats.sets_read == 2
    # dry-run: nothing was upserted
    assert ing.stats.sets_upserted == 2   # counts what WOULD have been written
    assert len(fake_supa.tables.get(si.T_SETS, [])) == 0  # actual DB untouched
    # No market_import_runs opened in dry-run
    assert fake_supa.run_id is None


def test_orchestrator_writes_full_pipeline_with_faked_scryfall():
    """Full pipeline smoke test — proves FK resolution + phase ordering.

    Sets first, then oracle + legalities, then printings + finishes, then
    rulings. Every phase upserts through the fake supabase and we assert
    the recorded call shape.
    """
    # Faked Scryfall: 1 set, 1 oracle card, 1 printing, 1 ruling.
    fake_sets = [
        {"object": "set", "id": "seta", "code": "NEO", "name": "Kamigawa: Neon Dynasty"},
    ]
    fake_oracle = [{
        "object": "card",
        "oracle_id": "ora1",
        "name": "Test Card",
        "mana_cost": "{U}",
        "cmc": 1.0,
        "type_line": "Instant",
        "colors": ["U"],
        "legalities": {"commander": "legal", "modern": "legal"},
    }]
    fake_default = [{
        "object": "card",
        "id":               "prta",
        "oracle_id":        "ora1",
        "name":             "Test Card",
        "set":              "NEO",
        "collector_number": "1",
        "lang":             "en",
        "layout":           "normal",
        "finishes":         ["nonfoil", "foil"],
    }]
    fake_rulings = [{
        "oracle_id":    "ora1",
        "source":       "wotc",
        "published_at": "2022-02-18",
        "comment":      "The rules text is what it is.",
    }]

    class _FakeScryfall:
        def get_bulk_metadata(self):
            return {
                si.BULK_TYPE_ORACLE:  {"download_uri": "https://x/oracle.json"},
                si.BULK_TYPE_DEFAULT: {"download_uri": "https://x/default.json"},
                si.BULK_TYPE_RULINGS: {"download_uri": "https://x/rulings.json"},
            }
        def iter_sets(self):
            yield from fake_sets
        def iter_bulk_records(self, uri, *, content_encoding=None):
            if uri == "https://x/oracle.json":  return iter(fake_oracle)
            if uri == "https://x/default.json": return iter(fake_default)
            if uri == "https://x/rulings.json": return iter(fake_rulings)
            return iter([])

    fake_supa = _FakeSupabase()
    # Pre-seed the id-maps so FK resolution succeeds after each phase.
    fake_supa.seed_set_map({"neo": "set-internal-uuid"})
    fake_supa.seed_oracle_map({"ora1": "ora-internal-uuid"})
    fake_supa.seed_printing_map({"prta": "prt-internal-uuid"})

    ing = si.ScryfallCatalogueIngestion(
        fake_supa, _FakeScryfall(),
        dry_run=False,
    )
    ing.start()
    ing.run()
    ing.finish(status="success")

    # Assert every table received a row.
    assert len(fake_supa.tables.get(si.T_SETS, [])) == 1
    assert len(fake_supa.tables.get(si.T_ORACLE_CARDS, [])) == 1
    assert len(fake_supa.tables.get(si.T_PRINTINGS, [])) == 1
    assert len(fake_supa.tables.get(si.T_FINISHES, [])) == 2   # nonfoil + foil
    assert len(fake_supa.tables.get(si.T_LEGALITIES, [])) == 2
    assert len(fake_supa.tables.get(si.T_RULINGS, [])) == 1

    # Printing must carry BOTH resolved FKs.
    printing = fake_supa.tables[si.T_PRINTINGS][0]
    assert printing["oracle_card_id"] == "ora-internal-uuid"
    assert printing["set_id"] == "set-internal-uuid"

    # Finishes must point at the printing's internal UUID.
    for f in fake_supa.tables[si.T_FINISHES]:
        assert f["printing_id"] == "prt-internal-uuid"
        assert f["finish"] in ("nonfoil", "foil")

    # Legality upserts should use (oracle_card_id, format) as key.
    leg_calls = [c for c in fake_supa.calls if c.get("table") == si.T_LEGALITIES]
    assert leg_calls
    assert all(c["on_conflict"] == "oracle_card_id,format" for c in leg_calls)

    # Ruling upsert should use the four-column composite key.
    ruling_calls = [c for c in fake_supa.calls if c.get("table") == si.T_RULINGS]
    assert ruling_calls
    assert all(c["on_conflict"] == "oracle_card_id,source,published_at,comment_hash" for c in ruling_calls)

    # market_import_runs opened + updated.
    assert fake_supa.run_id == "test-run-id"
    assert fake_supa.market_run_update is not None
    payload = fake_supa.market_run_update["payload"]
    assert payload["status"] == "success"
    assert "notes" in payload


def test_orchestrator_quarantines_printings_without_oracle_id():
    """Art-card / token / emblem Scryfall objects lack oracle_id and must
    be quarantined, not forced into the model."""
    class _FakeScryfall:
        def get_bulk_metadata(self):
            return {si.BULK_TYPE_DEFAULT: {"download_uri": "https://x/default.json"}}
        def iter_sets(self): return iter([])
        def iter_bulk_records(self, uri, *, content_encoding=None):
            if uri == "https://x/default.json":
                yield {"id": "prta", "set": "NEO", "name": "Art card", "layout": "art_series"}  # no oracle_id
            return iter([])
    fake_supa = _FakeSupabase()
    fake_supa.seed_set_map({"neo": "set-internal-uuid"})
    ing = si.ScryfallCatalogueIngestion(
        fake_supa, _FakeScryfall(),
        dry_run=False,
        run_sets=False, run_oracle=False, run_legalities=False,
        run_printings=True, run_finishes=False, run_rulings=False,
    )
    ing.start()
    ing.run()
    ing.finish(status="success")
    assert ing.stats.printings_read == 1
    assert ing.stats.printings_no_oracle == 1
    assert ing.stats.printings_upserted == 0
    assert fake_supa.tables.get(si.T_PRINTINGS, []) == []


# ─── Safety-belt tests ──────────────────────────────────────────────────────


def test_is_ingestion_enabled():
    assert si.is_ingestion_enabled({}) is False
    assert si.is_ingestion_enabled({"MTG_CATALOGUE_INGESTION_ENABLED": ""}) is False
    assert si.is_ingestion_enabled({"MTG_CATALOGUE_INGESTION_ENABLED": "false"}) is False
    assert si.is_ingestion_enabled({"MTG_CATALOGUE_INGESTION_ENABLED": "TRUE"}) is True
    assert si.is_ingestion_enabled({"MTG_CATALOGUE_INGESTION_ENABLED": " true "}) is True


# ─── Fail-closed market_import_runs behaviour ───────────────────────────────


def test_fail_closed_dry_run_needs_no_market_import_runs():
    """Dry-run mode: start() returns None and produces zero writes; a
    missing / failing Supabase client is not fatal."""
    class _AlwaysFailingSupabase:
        def insert_market_run(self, payload):
            raise RuntimeError("would fail — but dry-run shouldn't call this")
        # Rest of the API is unreachable in dry-run.
        def upsert(self, *a, **k):
            raise AssertionError("dry-run must not upsert")
        def lookup_id_map(self, *a, **k):
            return {}
        def update_market_run(self, *a, **k):
            raise AssertionError("dry-run must not update market_import_runs")
    class _EmptyScryfall:
        def get_bulk_metadata(self):
            return {}
        def iter_sets(self):
            return iter([{"object": "set", "id": "s1", "code": "NEO", "name": "Kamigawa"}])
        def iter_bulk_records(self, *a, **k):
            return iter([])
    ing = si.ScryfallCatalogueIngestion(
        _AlwaysFailingSupabase(), _EmptyScryfall(),
        dry_run=True,
        run_sets=True, run_oracle=False, run_legalities=False,
        run_printings=False, run_finishes=False, run_rulings=False,
    )
    # start() must NOT raise in dry-run.
    assert ing.start() is None
    ing.run()
    ing.finish(status="success")
    assert ing.stats.sets_read == 1
    # Dry-run counts what "would have been" written; but no actual
    # Supabase upsert was invoked (asserted by _AlwaysFailingSupabase.upsert).


def test_fail_closed_real_run_succeeds_when_market_import_runs_created():
    """Real run: start() succeeds and run() proceeds normally when
    insert_market_run returns a run_id."""
    fake = _FakeSupabase()
    fake.seed_set_map({"neo": "set-uuid"})
    class _FakeScryfall:
        def get_bulk_metadata(self):
            return {}
        def iter_sets(self):
            return iter([{"object": "set", "id": "s1", "code": "NEO", "name": "Kamigawa"}])
        def iter_bulk_records(self, *a, **k):
            return iter([])
    ing = si.ScryfallCatalogueIngestion(
        fake, _FakeScryfall(),
        dry_run=False,
        run_sets=True, run_oracle=False, run_legalities=False,
        run_printings=False, run_finishes=False, run_rulings=False,
    )
    run_id = ing.start()
    assert run_id == "test-run-id"
    ing.run()
    ing.finish(status="success")
    assert len(fake.tables.get(si.T_SETS, [])) == 1
    assert fake.market_run_update is not None
    assert fake.market_run_update["payload"]["status"] == "success"


def test_fail_closed_real_run_aborts_when_market_import_runs_fails():
    """Real run: start() must RAISE MarketImportRunAbort when
    insert_market_run fails. run() must NOT be invoked. Zero catalogue
    writes must happen."""
    class _FailingSupabase(_FakeSupabase):
        def insert_market_run(self, payload):
            raise RuntimeError("provider CHECK constraint violated (23514)")
    fake = _FailingSupabase()
    class _FakeScryfall:
        def get_bulk_metadata(self):
            return {}
        def iter_sets(self):
            # If start() were to leak past its failure, this generator
            # would yield rows that _ingest_sets would then upsert —
            # which is exactly what we're testing NEVER happens.
            return iter([{"object": "set", "id": "s1", "code": "NEO", "name": "Kamigawa"}])
        def iter_bulk_records(self, *a, **k):
            return iter([])
    ing = si.ScryfallCatalogueIngestion(
        fake, _FakeScryfall(),
        dry_run=False,
    )
    with pytest.raises(si.MarketImportRunAbort):
        ing.start()
    # Zero catalogue rows written.
    assert fake.tables == {}
    # No market_import_runs update recorded — start() never got a run_id.
    assert fake.market_run_update is None


def test_fail_closed_real_run_aborts_when_supabase_client_missing():
    """Real run + no supabase client: start() must raise (would silently
    skip market_import_runs pre-fix)."""
    class _FakeScryfall:
        def get_bulk_metadata(self):
            return {}
        def iter_sets(self):
            return iter([])
        def iter_bulk_records(self, *a, **k):
            return iter([])
    ing = si.ScryfallCatalogueIngestion(
        supabase=None,
        scryfall=_FakeScryfall(),
        dry_run=False,
    )
    with pytest.raises(si.MarketImportRunAbort):
        ing.start()


def test_fail_closed_real_run_aborts_when_insert_returns_no_run_id():
    """Real run + insert_market_run returns None instead of raising:
    still fail-closed (would silently proceed without a run pre-fix)."""
    class _NullingSupabase(_FakeSupabase):
        def insert_market_run(self, payload):
            return None
    fake = _NullingSupabase()
    class _FakeScryfall:
        def get_bulk_metadata(self): return {}
        def iter_sets(self): return iter([{"object":"set","id":"s1","code":"NEO","name":"K"}])
        def iter_bulk_records(self, *a, **k): return iter([])
    ing = si.ScryfallCatalogueIngestion(fake, _FakeScryfall(), dry_run=False)
    with pytest.raises(si.MarketImportRunAbort):
        ing.start()
    assert fake.tables == {}


# ─── Mana-value fidelity tests ──────────────────────────────────────────────
#
# Regression suite for the 22003 numeric overflow that surfaced against
# Scryfall's joke-set cards (Gleemax cmc=1,000,000). The transform layer
# must NEVER clamp/round/truncate; it passes the CMC through untouched,
# and the DB column is unrestricted NUMERIC.


def _oracle_card_with_cmc(cmc):
    return {
        "object":       "card",
        "oracle_id":    "oracle-uuid",
        "name":         "Test",
        "mana_cost":    "",
        "cmc":          cmc,
        "type_line":    "Sorcery",
        "colors":       [],
    }


def test_transform_oracle_card_mana_value_integer():
    row = si.transform_oracle_card(_oracle_card_with_cmc(3))
    assert row is not None
    assert row["mana_value"] == 3.0
    assert row["mana_value"] == pytest.approx(3.0)


def test_transform_oracle_card_mana_value_fractional():
    # Un-set half-CMC cards ("Little Girl" etc.) publish cmc=0.5.
    row = si.transform_oracle_card(_oracle_card_with_cmc(0.5))
    assert row is not None
    assert row["mana_value"] == pytest.approx(0.5)


def test_transform_oracle_card_mana_value_one_million():
    # Gleemax (Unhinged) — this exact value caused Run 1 to fail.
    # The transform must not clamp, cap, or reject it.
    row = si.transform_oracle_card(_oracle_card_with_cmc(1_000_000))
    assert row is not None
    assert row["mana_value"] == pytest.approx(1_000_000.0)
    assert row["mana_value"] > 9_999.99  # would have overflowed NUMERIC(6,2)


def test_transform_oracle_card_mana_value_none():
    # Legitimate NULL — some Scryfall objects (rare face-only edge cases)
    # publish cmc as null. The transform must accept None, not coerce.
    row = si.transform_oracle_card(_oracle_card_with_cmc(None))
    assert row is not None
    assert row["mana_value"] is None


def test_transform_oracle_card_mana_value_string_fallback():
    # Defensive: if the source ever sends a numeric-looking string,
    # _parse_numeric_cmc coerces via float(); non-numeric returns None.
    row = si.transform_oracle_card(_oracle_card_with_cmc("2.0"))
    assert row is not None
    assert row["mana_value"] == pytest.approx(2.0)
    row2 = si.transform_oracle_card(_oracle_card_with_cmc("not-a-number"))
    assert row2 is not None
    assert row2["mana_value"] is None


# ─── Ruling-dedup regression tests ──────────────────────────────────────────
#
# Regression for HTTP 500 21000: "ON CONFLICT DO UPDATE command cannot
# affect row a second time". Postgres refuses to touch the same conflict
# target twice within one INSERT ... ON CONFLICT, so we must collapse
# duplicates before the batch reaches the DB. Effective identity is
# (oracle_card_id, source, published_at, comment) — comment_hash is a
# GENERATED column derived from comment.


def _ruling_ingestion_with_stream(fake_rulings):
    """Helper: run the ingestion in rulings-only mode against a given
    stream and return the (fake_supa, ingestion) pair."""
    class _FakeScryfall:
        def get_bulk_metadata(self):
            return {si.BULK_TYPE_RULINGS: {"download_uri": "https://x/rulings.json"}}
        def iter_sets(self): return iter([])
        def iter_bulk_records(self, uri, *, content_encoding=None):
            if uri == "https://x/rulings.json":
                return iter(fake_rulings)
            return iter([])
    fake_supa = _FakeSupabase()
    fake_supa.seed_oracle_map({"ora1": "ora-uuid-1", "ora2": "ora-uuid-2"})
    ing = si.ScryfallCatalogueIngestion(
        fake_supa, _FakeScryfall(),
        dry_run=False,
        run_sets=False, run_oracle=False, run_legalities=False,
        run_printings=False, run_finishes=False, run_rulings=True,
    )
    ing.start()
    ing.run()
    ing.finish(status="success")
    return fake_supa, ing


def test_rulings_dedup_identical_adjacent():
    """Two literally identical ruling records adjacent in the stream
    collapse to one upserted row."""
    r = {"oracle_id": "ora1", "source": "wotc", "published_at": "2022-02-18",
         "comment": "Same ruling twice."}
    fake_supa, ing = _ruling_ingestion_with_stream([r, dict(r)])
    assert ing.stats.rulings_read == 2
    assert ing.stats.rulings_duplicate == 1
    assert ing.stats.rulings_upserted == 1
    assert ing.stats.errors == 0
    assert len(fake_supa.tables.get(si.T_RULINGS, [])) == 1


def test_rulings_dedup_identical_apart_in_stream():
    """Duplicate rulings separated by other records must still collapse."""
    dup = {"oracle_id": "ora1", "source": "wotc", "published_at": "2022-02-18",
           "comment": "A"}
    other = {"oracle_id": "ora2", "source": "wotc", "published_at": "2022-02-18",
             "comment": "B"}
    other2 = {"oracle_id": "ora2", "source": "wotc", "published_at": "2023-01-01",
              "comment": "C"}
    fake_supa, ing = _ruling_ingestion_with_stream([dup, other, other2, dict(dup)])
    assert ing.stats.rulings_read == 4
    assert ing.stats.rulings_duplicate == 1
    assert ing.stats.rulings_upserted == 3
    assert ing.stats.errors == 0
    assert len(fake_supa.tables.get(si.T_RULINGS, [])) == 3


def test_rulings_dedup_different_comment_stays_separate():
    """Two rulings differing only by comment text remain separate rows."""
    r1 = {"oracle_id": "ora1", "source": "wotc", "published_at": "2022-02-18",
          "comment": "First ruling."}
    r2 = {"oracle_id": "ora1", "source": "wotc", "published_at": "2022-02-18",
          "comment": "Second, distinct ruling."}
    fake_supa, ing = _ruling_ingestion_with_stream([r1, r2])
    assert ing.stats.rulings_read == 2
    assert ing.stats.rulings_duplicate == 0
    assert ing.stats.rulings_upserted == 2
    assert len(fake_supa.tables.get(si.T_RULINGS, [])) == 2


def test_rulings_dedup_same_comment_different_source_or_date_stays_separate():
    """Same comment text — but different source, or different published_at —
    is a distinct ruling identity and must not collapse."""
    comment = "The rules text is unambiguous."
    r_same = {"oracle_id": "ora1", "source": "wotc", "published_at": "2022-02-18",
              "comment": comment}
    r_diff_source = {"oracle_id": "ora1", "source": "scryfall",
                     "published_at": "2022-02-18", "comment": comment}
    r_diff_date = {"oracle_id": "ora1", "source": "wotc",
                   "published_at": "2023-01-01", "comment": comment}
    fake_supa, ing = _ruling_ingestion_with_stream([r_same, r_diff_source, r_diff_date])
    assert ing.stats.rulings_duplicate == 0
    assert ing.stats.rulings_upserted == 3
    assert len(fake_supa.tables.get(si.T_RULINGS, [])) == 3


def test_rulings_duplicate_does_not_count_as_error_or_make_run_partial():
    """rulings_duplicate is normal source noise, not an ingestion failure:
    it must NOT increment the error counter, and the run must NOT become
    partial merely because duplicates exist."""
    r = {"oracle_id": "ora1", "source": "wotc", "published_at": "2022-02-18",
         "comment": "Dup."}
    _fake_supa, ing = _ruling_ingestion_with_stream([r, dict(r), dict(r)])
    assert ing.stats.rulings_duplicate == 2
    assert ing.stats.errors == 0


# ─── Legality-dedup regression tests ────────────────────────────────────────
#
# Belt-and-braces: Scryfall does not currently emit legality duplicates,
# but the same 21000 within-batch failure mode applies if the same
# (oracle_card_id, format) key is emitted twice. Dedup before batching.


def test_legalities_dedup_collapses_duplicates():
    """If two Oracle records emit the same (oracle_id, format) pair — for
    example if a future Scryfall change re-emits reprints — the two
    entries must collapse to one upsert row and NOT hit the DB with a
    conflicting batch."""
    oracle_a = {
        "object": "card", "oracle_id": "ora1", "name": "A",
        "cmc": 1.0, "type_line": "Instant", "colors": [],
        "legalities": {"modern": "legal", "commander": "legal"},
    }
    # Duplicate ORACLE object (same oracle_id, same legalities). Scryfall
    # dedupes by oracle_id in the Oracle bulk, but we prove the pipeline
    # is defensive against the pathological case anyway.
    oracle_a_dup = dict(oracle_a)

    class _FakeScryfall:
        def get_bulk_metadata(self):
            return {si.BULK_TYPE_ORACLE: {"download_uri": "https://x/oracle.json"}}
        def iter_sets(self): return iter([])
        def iter_bulk_records(self, uri, *, content_encoding=None):
            if uri == "https://x/oracle.json":
                return iter([oracle_a, oracle_a_dup])
            return iter([])

    fake_supa = _FakeSupabase()
    fake_supa.seed_oracle_map({"ora1": "ora-uuid-1"})
    ing = si.ScryfallCatalogueIngestion(
        fake_supa, _FakeScryfall(),
        dry_run=False,
        run_sets=False, run_oracle=True, run_legalities=True,
        run_printings=False, run_finishes=False, run_rulings=False,
    )
    ing.start()
    ing.run()
    ing.finish(status="success")

    # Two legality pairs per oracle × two identical oracle objects = four
    # source pairs. Dedup collapses each (oracle_card_id, format) key to
    # one, so only 2 rows reach the DB and duplicate counter = 2.
    assert ing.stats.legalities_upserted == 2
    assert ing.stats.legalities_duplicate == 2
    assert ing.stats.errors == 0
    assert len(fake_supa.tables.get(si.T_LEGALITIES, [])) == 2
    keys = {(r["oracle_card_id"], r["format"]) for r in fake_supa.tables[si.T_LEGALITIES]}
    assert keys == {("ora-uuid-1", "modern"), ("ora-uuid-1", "commander")}


def test_legalities_duplicate_does_not_count_as_error():
    """legalities_duplicate is normal defensive-dedup noise, not an
    error."""
    oracle_a = {
        "object": "card", "oracle_id": "ora1", "name": "A",
        "cmc": 1.0, "type_line": "Instant", "colors": [],
        "legalities": {"modern": "legal"},
    }
    class _FakeScryfall:
        def get_bulk_metadata(self):
            return {si.BULK_TYPE_ORACLE: {"download_uri": "https://x/oracle.json"}}
        def iter_sets(self): return iter([])
        def iter_bulk_records(self, uri, *, content_encoding=None):
            if uri == "https://x/oracle.json":
                return iter([oracle_a, dict(oracle_a), dict(oracle_a)])
            return iter([])
    fake_supa = _FakeSupabase()
    fake_supa.seed_oracle_map({"ora1": "ora-uuid-1"})
    ing = si.ScryfallCatalogueIngestion(
        fake_supa, _FakeScryfall(),
        dry_run=False,
        run_sets=False, run_oracle=True, run_legalities=True,
        run_printings=False, run_finishes=False, run_rulings=False,
    )
    ing.start()
    ing.run()
    ing.finish(status="success")
    assert ing.stats.legalities_duplicate == 2
    assert ing.stats.errors == 0
