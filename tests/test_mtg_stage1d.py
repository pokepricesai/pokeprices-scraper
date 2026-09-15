"""Unit tests for the Stage 1D subpackage.

Focus:
    * storage forecast math (verifies the numbers in the runbook).
    * partition-name derivation safety.
    * lock acquire semantics with a stub Supabase client.
    * build-guard idempotency logic.
    * gap-repair classification decision function.
    * validation warn / hard-fail routing.

Tests use unittest so they can be invoked as ``python -m unittest
tests.test_mtg_stage1d`` without pytest. They do not touch the network
or the DB; every Supabase interaction goes through a small stub client
that queues expected responses.
"""
from __future__ import annotations

import json
import re
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

from mtg_stage1d import (
    allprices_today, build_guard, current_prices, gap_repair, identifier_delta,
    lock, partitions, scryfall_delta, validation,
)
from tests.fixtures import mtg_allprices_today_tiny as fx_apt


# ─── Stub SupabaseClient ────────────────────────────────────────────────────

class StubSupabaseClient:
    """Minimal stand-in for the real mtgjson_ingestion.SupabaseClient.

    Queue-based: tests set up ``expected`` in order; each ``_req`` call
    consumes one entry and returns its ``(code, body)`` tuple.

    Records every call in ``calls`` for inspection.
    """

    def __init__(self, url: str = "https://example.supabase.co", key: str = "test-key"):
        self.url = url
        self.key = key
        self.expected: list[tuple[int, str, dict | None]] = []  # (code, body, match)
        self.calls: list[dict] = []

    def enqueue(self, code: int, body_obj, match: dict | None = None):
        body = body_obj if isinstance(body_obj, str) else json.dumps(body_obj)
        self.expected.append((code, body, match))

    def _req(self, path: str, *, method: str = "GET", body=None, prefer: str | None = None,
             max_retries: int = 5) -> tuple[int, str]:
        self.calls.append({"path": path, "method": method, "body": body, "prefer": prefer})
        if not self.expected:
            raise AssertionError(f"unexpected _req call: {method} {path}")
        code, response, _match = self.expected.pop(0)
        return code, response


# ─── Storage forecast math ─────────────────────────────────────────────────

class StorageForecastMathTests(unittest.TestCase):
    """The Stage 1D runbook cites daily/monthly/annual raw growth. Guard
    those figures — future refactors must reconcile if the math changes."""

    ROWS_PER_DAY = 727_000
    HEAP_BYTES_PER_ROW = 170
    INDEX_BYTES_PER_ROW = 160     # PK + composite UNIQUE + finish/date btree; BRIN ~0
    TOTAL_BYTES_PER_ROW = HEAP_BYTES_PER_ROW + INDEX_BYTES_PER_ROW

    def test_daily_heap_growth_MB(self):
        heap_mb = self.ROWS_PER_DAY * self.HEAP_BYTES_PER_ROW / 1024 / 1024
        self.assertGreater(heap_mb, 110)
        self.assertLess(heap_mb, 130)

    def test_daily_total_growth_MB(self):
        total_mb = self.ROWS_PER_DAY * self.TOTAL_BYTES_PER_ROW / 1024 / 1024
        # ~228 MB/day; guard 200..260 MB
        self.assertGreater(total_mb, 200)
        self.assertLess(total_mb, 260)

    def test_monthly_total_growth_GB(self):
        monthly_gb = self.ROWS_PER_DAY * self.TOTAL_BYTES_PER_ROW * 30 / 1024 / 1024 / 1024
        # ~6.7 GB/month; guard 6..8 GB
        self.assertGreater(monthly_gb, 6)
        self.assertLess(monthly_gb, 8)

    def test_annual_total_growth_GB(self):
        annual_gb = self.ROWS_PER_DAY * self.TOTAL_BYTES_PER_ROW * 365 / 1024 / 1024 / 1024
        # ~81 GB/year raw growth alone
        self.assertGreater(annual_gb, 70)
        self.assertLess(annual_gb, 95)

    def test_48gb_disk_headroom_months(self):
        """If the DB is currently ~48 GB total the 48 GB disk is at the
        provisioned ceiling; if it's meaningfully below that, months_left
        is measured in low single digits.

        Guards the runbook's claim that the disk becomes constrained
        within ~1-6 months, not years.
        """
        monthly_gb = self.ROWS_PER_DAY * self.TOTAL_BYTES_PER_ROW * 30 / 1024 / 1024 / 1024
        # Even if you assume 12 GB of headroom on a 48 GB disk:
        months_left = 12 / monthly_gb
        self.assertLess(months_left, 2.5)


# ─── Partition-name derivation ─────────────────────────────────────────────

class PartitionNameSafetyTests(unittest.TestCase):
    def test_first_of_month_normalises_middle_of_month(self):
        self.assertEqual(partitions._first_of_month(date(2026, 9, 15)), date(2026, 9, 1))

    def test_first_of_month_stable_on_first(self):
        self.assertEqual(partitions._first_of_month(date(2026, 9, 1)), date(2026, 9, 1))

    def test_next_month_january_boundary(self):
        self.assertEqual(partitions._next_month_first(date(2026, 12, 15)), date(2027, 1, 1))
        self.assertEqual(partitions._next_month_first(date(2026, 12, 31)), date(2027, 1, 1))

    def test_next_month_february_boundary(self):
        self.assertEqual(partitions._next_month_first(date(2028, 1, 31)), date(2028, 2, 1))

    def test_partition_name_shape(self):
        """Simulate what the SQL function derives; the client cannot inject arbitrary names."""
        for target in [date(2026, 9, 1), date(2026, 12, 31), date(2030, 1, 15)]:
            m = partitions._first_of_month(target)
            suffix = f"{m.year:04d}_{m.month:02d}"
            child = f"mtg_price_observations_{suffix}"
            self.assertRegex(child, r"^mtg_price_observations_\d{4}_\d{2}$")


# ─── Lock acquire semantics ────────────────────────────────────────────────

class LockAcquireTests(unittest.TestCase):
    def _stub(self):
        return StubSupabaseClient()

    def test_acquire_fresh_returns_handle(self):
        s = self._stub()
        row = {
            "lock_key": "mtg_daily_ingest",
            "acquired_at": "2026-09-15T15:00:00+00:00",
            "acquired_by": "local:test:hostA:pid-1",
            "heartbeat_at": "2026-09-15T15:00:00+00:00",
            "expected_release_by": "2026-09-15T19:00:00+00:00",
        }
        s.enqueue(201, [row])
        h = lock.acquire(s, "mtg_daily_ingest", "test")
        self.assertEqual(h.lock_key, "mtg_daily_ingest")
        self.assertFalse(h.stale_takeover)

    def test_acquire_held_raises(self):
        s = self._stub()
        # First INSERT returns 201 + empty body (conflict swallowed).
        s.enqueue(201, [])
        # Fetch discovers existing holder that is FRESH (heartbeat within window).
        now = datetime.now(tz=timezone.utc)
        fresh = {
            "lock_key": "mtg_daily_ingest",
            "acquired_at": (now - timedelta(minutes=2)).isoformat(),
            "acquired_by": "local:other:hostB:pid-99",
            "heartbeat_at": (now - timedelta(minutes=1)).isoformat(),
            "expected_release_by": (now + timedelta(hours=3)).isoformat(),
        }
        s.enqueue(200, [fresh])
        with self.assertRaises(lock.LockAcquireFailed):
            lock.acquire(s, "mtg_daily_ingest", "test")

    def test_acquire_stale_takeover(self):
        s = self._stub()
        now = datetime.now(tz=timezone.utc)
        # 1st attempt: ON CONFLICT DO NOTHING returns empty.
        s.enqueue(201, [])
        # Fetch existing = stale (heartbeat 60m old, past expected_release_by)
        stale = {
            "lock_key": "mtg_daily_ingest",
            "acquired_at": (now - timedelta(hours=6)).isoformat(),
            "acquired_by": "local:crashed:hostC:pid-42",
            "heartbeat_at": (now - timedelta(hours=1)).isoformat(),
            "expected_release_by": (now - timedelta(hours=2)).isoformat(),
        }
        s.enqueue(200, [stale])
        # DELETE (takeover)
        s.enqueue(204, "")
        # Retry INSERT succeeds
        row = {
            "lock_key": "mtg_daily_ingest",
            "acquired_at": now.isoformat(),
            "acquired_by": "local:test:hostA:pid-1",
            "heartbeat_at": now.isoformat(),
            "expected_release_by": (now + timedelta(hours=4)).isoformat(),
        }
        s.enqueue(201, [row])

        h = lock.acquire(s, "mtg_daily_ingest", "test")
        self.assertTrue(h.stale_takeover)


# ─── Build-guard idempotency ──────────────────────────────────────────────

class BuildGuardTests(unittest.TestCase):
    def test_skip_when_current_le_most_recent(self):
        self.assertTrue(build_guard.should_skip("2026-09-13", "2026-09-14"))
        self.assertTrue(build_guard.should_skip("2026-09-14", "2026-09-14"))

    def test_no_skip_when_current_greater(self):
        self.assertFalse(build_guard.should_skip("2026-09-15", "2026-09-14"))

    def test_no_skip_when_no_prior(self):
        self.assertFalse(build_guard.should_skip("2026-09-15", None))

    def test_fetch_most_recent_returns_max_across_runs(self):
        s = StubSupabaseClient()
        notes_a = json.dumps({"mtgjson_meta": {"data": {"date": "2026-09-13"}}})
        notes_b = json.dumps({"mtgjson_meta": {"data": {"date": "2026-09-14"}}})
        notes_c = json.dumps({"mtgjson_meta": {"data": {"date": "2026-09-12"}}})
        s.enqueue(200, [
            {"id": 1, "status": "success", "started_at": "2026-09-15T15:00:00Z", "notes": notes_a},
            {"id": 2, "status": "success", "started_at": "2026-09-15T14:00:00Z", "notes": notes_b},
            {"id": 3, "status": "success", "started_at": "2026-09-15T13:00:00Z", "notes": notes_c},
        ])
        self.assertEqual(build_guard.most_recent_ingested_build_date(s), "2026-09-14")


# ─── Gap classification ────────────────────────────────────────────────────

class GapClassificationTests(unittest.TestCase):
    def test_source_side_when_allprices_empty(self):
        self.assertEqual(
            gap_repair.classify_repair_outcome("2026-08-06", source_observations_for_date=0, rows_inserted=0),
            "source_side_confirmed",
        )

    def test_ingestion_side_repaired_when_rows_inserted(self):
        self.assertEqual(
            gap_repair.classify_repair_outcome("2026-08-10", source_observations_for_date=500_000, rows_inserted=500_000),
            "ingestion_side_repaired",
        )

    def test_ingestion_side_unfilled(self):
        self.assertEqual(
            gap_repair.classify_repair_outcome("2026-08-10", source_observations_for_date=500_000, rows_inserted=0),
            "ingestion_side_unfilled",
        )

    def test_still_unknown_when_not_probed(self):
        self.assertEqual(
            gap_repair.classify_repair_outcome("2026-08-06", source_observations_for_date=None, rows_inserted=0),
            "still_unknown",
        )

    def test_source_side_confirmed_uses_date_scoped_signal(self):
        """Regression: obs_mapped=0 alone is NOT sufficient for
        source_side_confirmed. If the source has unmapped-UUID
        observations for the date, ingestion_side_unfilled is correct.
        """
        # A date with observations in source but all under unmapped UUIDs:
        # source_observations_for_date > 0 (we saw rows in source), but
        # rows_inserted = 0 (nothing landed because none mapped).
        self.assertEqual(
            gap_repair.classify_repair_outcome(
                "2026-08-06",
                source_observations_for_date=42,   # source HAS 42 rows for the date
                rows_inserted=0,
            ),
            "ingestion_side_unfilled",
        )


# ─── Validation warn / hard-fail routing ───────────────────────────────────

class ValidationRoutingTests(unittest.TestCase):
    def _medians(self):
        return {
            "total_obs":   700_000,
            "quarantine":  100_000,
            "per_provider": {
                "tcgplayer":   148_000,
                "cardkingdom": 220_000,
                "cardmarket":  144_000,
                "manapool":    148_000,
                "cardhoarder":  59_000,
            },
        }

    def _healthy(self):
        return {
            "checksum_ok":               True,
            "source_parsed_ok":          True,
            "target_date":               "2026-09-14",
            "source_build_date":         "2026-09-14",
            "partition_ready":           True,
            "lock_ok":                   True,
            "total_observations_inserted": 720_000,
            "historical_daily_medians":  self._medians(),
            "per_provider_inserted": {
                "tcgplayer":   148_000, "cardkingdom": 220_000,
                "cardmarket":  144_000, "manapool":    148_000,
                "cardhoarder":  59_000,
            },
            "quarantine_total":          105_000,
            "write_errors":              0,
        }

    def test_success_when_everything_healthy(self):
        v = validation.evaluate(**self._healthy())
        self.assertEqual(v.status, "success")
        self.assertEqual(v.hard_failures, [])
        self.assertEqual(v.warnings, [])

    def test_hard_fail_on_checksum(self):
        args = self._healthy()
        args["checksum_ok"] = False
        v = validation.evaluate(**args)
        self.assertEqual(v.status, "failed")
        self.assertIn("source_checksum_mismatch", v.hard_failures)

    def test_hard_fail_on_target_date_mismatch(self):
        args = self._healthy()
        args["source_build_date"] = "2026-09-13"
        v = validation.evaluate(**args)
        self.assertEqual(v.status, "failed")
        self.assertTrue(any("target_date_mismatch" in f for f in v.hard_failures))

    def test_hard_fail_on_implausibly_tiny(self):
        args = self._healthy()
        args["total_observations_inserted"] = 50_000  # <10% of median 700k
        v = validation.evaluate(**args)
        self.assertEqual(v.status, "failed")
        self.assertTrue(any("implausibly_tiny_total" in f for f in v.hard_failures))

    def test_warning_on_provider_absent(self):
        args = self._healthy()
        args["per_provider_inserted"] = dict(args["per_provider_inserted"])
        args["per_provider_inserted"]["cardhoarder"] = 0
        v = validation.evaluate(**args)
        self.assertEqual(v.status, "partial")
        self.assertTrue(any("provider_absent:cardhoarder" in w for w in v.warnings))

    def test_warning_on_provider_below_median(self):
        args = self._healthy()
        args["per_provider_inserted"] = dict(args["per_provider_inserted"])
        args["per_provider_inserted"]["tcgplayer"] = 50_000  # far below 148k
        v = validation.evaluate(**args)
        self.assertEqual(v.status, "partial")
        self.assertTrue(any("provider_below_median:tcgplayer" in w for w in v.warnings))

    def test_warning_on_high_quarantine(self):
        args = self._healthy()
        args["quarantine_total"] = 400_000  # 4x median 100k
        v = validation.evaluate(**args)
        self.assertEqual(v.status, "partial")
        self.assertTrue(any("quarantine_high" in w for w in v.warnings))

    def test_write_errors_are_hard_failure_not_warning(self):
        args = self._healthy()
        args["write_errors"] = 500
        v = validation.evaluate(**args)
        self.assertEqual(v.status, "failed")
        self.assertTrue(any("database_write_errors" in f for f in v.hard_failures))

    def test_compute_medians_returns_zero_for_empty_input(self):
        m = validation.compute_last_7_day_medians([])
        self.assertEqual(m["total_obs"], 0.0)
        self.assertEqual(m["quarantine"], 0.0)
        self.assertEqual(m["per_provider"], {})

    def test_compute_medians_reasonable(self):
        m = validation.compute_last_7_day_medians([
            {"inserted_new": 700_000, "obs_missing_finish": 90_000, "obs_unmapped_uuid": 10_000,
             "provider_counts": {"tcgplayer": 148_000}},
            {"inserted_new": 720_000, "obs_missing_finish": 92_000, "obs_unmapped_uuid": 8_000,
             "provider_counts": {"tcgplayer": 150_000}},
            {"inserted_new": 710_000, "obs_missing_finish": 88_000, "obs_unmapped_uuid": 12_000,
             "provider_counts": {"tcgplayer": 149_000}},
        ])
        self.assertAlmostEqual(m["total_obs"],   710_000.0, delta=1)
        self.assertAlmostEqual(m["quarantine"], 100_000.0, delta=1)
        self.assertAlmostEqual(m["per_provider"]["tcgplayer"], 149_000.0, delta=1)


# ─── Build-guard completion semantics ─────────────────────────────────────

class BuildGuardCompletionTests(unittest.TestCase):
    def test_success_counts_as_completed(self):
        self.assertTrue(build_guard.is_run_completed({"status": "success"}, {}))

    def test_partial_with_zero_errors_counts_as_completed(self):
        self.assertTrue(build_guard.is_run_completed(
            {"status": "partial"}, {"errors": 0}
        ))

    def test_partial_with_errors_does_not_count(self):
        self.assertFalse(build_guard.is_run_completed(
            {"status": "partial"}, {"errors": 500}
        ))

    def test_failed_does_not_count(self):
        self.assertFalse(build_guard.is_run_completed({"status": "failed"}, {"errors": 0}))

    def test_running_does_not_count(self):
        self.assertFalse(build_guard.is_run_completed({"status": "running"}, {"errors": 0}))

    def test_notes_as_string_parsed(self):
        # Real market_import_runs.notes is JSON-serialised text.
        row = {"status": "partial", "notes": json.dumps({"errors": 0})}
        self.assertTrue(build_guard.is_run_completed(row))

    def test_notes_string_with_errors_parsed(self):
        row = {"status": "partial", "notes": json.dumps({"errors": 42})}
        self.assertFalse(build_guard.is_run_completed(row))

    def test_most_recent_prefers_completed(self):
        # A partial-with-errors run for a later build MUST NOT set the guard.
        s = StubSupabaseClient()
        s.enqueue(200, [
            {"id": 1, "status": "partial",  "started_at": "2026-09-15T18:00:00Z",
             "notes": json.dumps({"mtgjson_meta": {"data": {"date": "2026-09-15"}}, "errors": 500})},
            {"id": 2, "status": "success",  "started_at": "2026-09-15T15:00:00Z",
             "notes": json.dumps({"mtgjson_meta": {"data": {"date": "2026-09-14"}}, "errors": 0})},
            {"id": 3, "status": "partial",  "started_at": "2026-09-14T15:00:00Z",
             "notes": json.dumps({"mtgjson_meta": {"data": {"date": "2026-09-13"}}, "errors": 0})},
        ])
        # Best completed = 2026-09-14 (the 09-15 partial-with-errors is not counted).
        self.assertEqual(build_guard.most_recent_ingested_build_date(s), "2026-09-14")


# ─── AllPricesToday fixture-based tests ───────────────────────────────────

class AllPricesTodayFixtureTests(unittest.TestCase):
    def _write_valid(self, tmpdir):
        import tempfile
        from pathlib import Path
        path = Path(tempfile.mkdtemp(dir=tmpdir)) / "AllPricesToday.json.gz"
        return fx_apt.build_file(fx_apt.VALID_MINIMAL_PAYLOAD, path)

    def test_read_meta_date_success(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_valid(tmp)
            date = allprices_today.read_meta_date(path)
            self.assertEqual(date, "2026-09-14")

    def test_read_meta_date_missing_date_raises(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "AllPricesToday.json.gz"
            fx_apt.build_file(fx_apt.MALFORMED_MISSING_DATE, path)
            with self.assertRaises(allprices_today.MalformedSource):
                allprices_today.read_meta_date(path)

    def test_verify_checksum_success(self):
        import hashlib, tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_valid(tmp)
            expected = hashlib.sha256(path.read_bytes()).hexdigest()
            actual = allprices_today.verify_checksum(path, expected)
            self.assertEqual(actual, expected)

    def test_verify_checksum_mismatch_raises(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_valid(tmp)
            with self.assertRaises(allprices_today.ChecksumMismatch):
                allprices_today.verify_checksum(path, "0" * 64)

    def test_gzip_streams_are_readable(self):
        """Sanity: the fixture is actually a valid gzip of valid JSON."""
        import gzip, tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_valid(tmp)
            with gzip.open(path, "rb") as f:
                first = f.read(1024)
            self.assertIn(b'"meta"', first)

    def test_build_guard_integration_via_fixture_date(self):
        """After reading meta.date from the fixture, the build-guard
        correctly compares against a most-recent value."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_valid(tmp)
            target = allprices_today.read_meta_date(path)
        self.assertTrue(build_guard.should_skip(target, "2026-09-14"))
        self.assertFalse(build_guard.should_skip(target, "2026-09-13"))


# ─── Identifier-delta core logic ──────────────────────────────────────────

class IdentifierDeltaTests(unittest.TestCase):
    def test_extract_scryfall_id_from_nested_identifiers(self):
        self.assertEqual(
            identifier_delta.extract_scryfall_id({"identifiers": {"scryfallId": "abc"}}),
            "abc",
        )

    def test_extract_scryfall_id_flat_fallback(self):
        self.assertEqual(
            identifier_delta.extract_scryfall_id({"scryfallId": "abc"}),
            "abc",
        )

    def test_extract_scryfall_id_missing(self):
        self.assertIsNone(identifier_delta.extract_scryfall_id({"identifiers": {}}))
        self.assertIsNone(identifier_delta.extract_scryfall_id({}))

    def test_apply_stream_skips_known(self):
        stats = identifier_delta.IdentifierDeltaStats()
        entries = [
            ("mtg-uuid-1", {"identifiers": {"scryfallId": "scry-1"}}),
        ]
        identifier_delta.apply_stream(
            iter(entries),
            known_uuids={"mtg-uuid-1"},
            scryfall_to_printing={"scry-1": "printing-1"},
            supabase=None,
            dry_run=True,
            stats=stats,
        )
        self.assertEqual(stats.already_known, 1)
        self.assertEqual(stats.newly_inserted, 0)

    def test_apply_stream_inserts_new(self):
        stats = identifier_delta.IdentifierDeltaStats()
        entries = [
            ("mtg-uuid-2", {"identifiers": {"scryfallId": "scry-2"}}),
            ("mtg-uuid-3", {"identifiers": {"scryfallId": "scry-3"}}),
        ]
        identifier_delta.apply_stream(
            iter(entries),
            known_uuids=set(),
            scryfall_to_printing={"scry-2": "printing-2", "scry-3": "printing-3"},
            supabase=None,
            dry_run=True,
            stats=stats,
        )
        self.assertEqual(stats.newly_inserted, 2)
        self.assertEqual(stats.unknown_scryfall_printing, 0)

    def test_apply_stream_counts_unknown_printing(self):
        stats = identifier_delta.IdentifierDeltaStats()
        entries = [
            ("mtg-uuid-4", {"identifiers": {"scryfallId": "scry-missing"}}),
        ]
        identifier_delta.apply_stream(
            iter(entries),
            known_uuids=set(),
            scryfall_to_printing={},
            supabase=None,
            dry_run=True,
            stats=stats,
        )
        self.assertEqual(stats.unknown_scryfall_printing, 1)
        self.assertEqual(stats.newly_inserted, 0)

    def test_apply_stream_counts_malformed(self):
        stats = identifier_delta.IdentifierDeltaStats()
        entries = [
            ("mtg-uuid-5", {"no": "scryfall_id_here"}),
            ("mtg-uuid-6", "not a dict"),
            ("",           {"identifiers": {"scryfallId": "scry-6"}}),
        ]
        identifier_delta.apply_stream(
            iter(entries),
            known_uuids=set(),
            scryfall_to_printing={"scry-6": "printing-6"},
            supabase=None,
            dry_run=True,
            stats=stats,
        )
        self.assertEqual(stats.malformed_rows, 3)
        self.assertEqual(stats.newly_inserted, 0)


# ─── Scryfall-delta decision + invocation surface ────────────────────────

class ScryfallDeltaTests(unittest.TestCase):
    """Full coverage of the three approved decision branches."""

    def _mock_supabase_with_last_updated_at(self, oracle: str, default: str, rulings: str,
                                             status: str = "success", errors: int = 0):
        s = StubSupabaseClient()
        s.enqueue(200, [{
            "id": "run-1",
            "status": status,
            "notes": json.dumps({
                "oracle_updated_at":  oracle,
                "default_updated_at": default,
                "rulings_updated_at": rulings,
                "errors":             errors,
            }),
        }])
        return s

    def test_skip_by_flag_short_circuits(self):
        # Passing skip=True must not attempt any network / DB.
        outcome = scryfall_delta.check_and_maybe_invoke(supabase=None, dry_run=True, skip=True)
        self.assertEqual(outcome.action, "skipped_by_flag")

    def test_unchanged_returns_without_invoking(self):
        s = self._mock_supabase_with_last_updated_at(
            "2026-09-14T10:00:00Z", "2026-09-14T10:00:00Z", "2026-09-14T10:00:00Z",
        )
        with patch.object(scryfall_delta, "_remote_updated_at", return_value={
            "oracle_cards":  "2026-09-14T10:00:00Z",
            "default_cards": "2026-09-14T10:00:00Z",
            "rulings":       "2026-09-14T10:00:00Z",
        }):
            # Ensure the ingester class is NOT touched when unchanged.
            with patch("scryfall_ingestion.ScryfallCatalogueIngestion") as CtorMock:
                outcome = scryfall_delta.check_and_maybe_invoke(s, dry_run=False)
        self.assertEqual(outcome.action, "unchanged")
        self.assertIsNone(outcome.scryfall_run_id)
        CtorMock.assert_not_called()

    def test_changed_invokes_existing_ingester(self):
        s = self._mock_supabase_with_last_updated_at(
            "2026-09-13T10:00:00Z", "2026-09-13T10:00:00Z", "2026-09-13T10:00:00Z",
        )
        instance = MagicMock()
        instance.stats.errors = 0
        instance.run_id = "run-xyz"
        with patch.object(scryfall_delta, "_remote_updated_at", return_value={
            "oracle_cards":  "2026-09-14T10:00:00Z",
            "default_cards": "2026-09-14T10:00:00Z",
            "rulings":       "2026-09-13T10:00:00Z",
        }):
            with patch("scryfall_ingestion.ScryfallCatalogueIngestion", return_value=instance) as CtorMock:
                with patch("scryfall_ingestion.ScryfallClient"):
                    outcome = scryfall_delta.check_and_maybe_invoke(s, dry_run=False)
        self.assertEqual(outcome.action, "invoked")
        self.assertEqual(outcome.scryfall_run_id, "run-xyz")
        self.assertEqual(outcome.status, "success")
        CtorMock.assert_called_once()
        instance.start.assert_called_once()
        instance.run.assert_called_once()
        instance.finish.assert_called_once_with(status="success")

    def test_changed_but_import_fails_is_surfaced(self):
        s = self._mock_supabase_with_last_updated_at(
            "2026-09-13T10:00:00Z", "2026-09-13T10:00:00Z", "2026-09-13T10:00:00Z",
        )
        instance = MagicMock()
        instance.start.side_effect = RuntimeError("simulated ingester crash")
        with patch.object(scryfall_delta, "_remote_updated_at", return_value={
            "oracle_cards":  "2026-09-14T10:00:00Z",
            "default_cards": "2026-09-14T10:00:00Z",
            "rulings":       "2026-09-14T10:00:00Z",
        }):
            with patch("scryfall_ingestion.ScryfallCatalogueIngestion", return_value=instance):
                with patch("scryfall_ingestion.ScryfallClient"):
                    outcome = scryfall_delta.check_and_maybe_invoke(s, dry_run=False)
        self.assertEqual(outcome.action, "failed")
        self.assertIn("simulated ingester crash", outcome.error or "")

    def test_previous_partial_with_errors_is_ignored_for_baseline(self):
        # If the most recent Scryfall run was partial-with-errors, the
        # freshness gate treats it as if we hadn't ingested that build
        # and MUST re-invoke.
        s = self._mock_supabase_with_last_updated_at(
            "2026-09-14T10:00:00Z", "2026-09-14T10:00:00Z", "2026-09-14T10:00:00Z",
            status="partial", errors=42,
        )
        # Add a follow-up row so the loop finds no completed prior — the
        # first row (partial-with-errors) is skipped inside
        # scryfall_delta._last_scryfall_updated_at.
        s.enqueue(200, [])
        instance = MagicMock()
        instance.stats.errors = 0
        instance.run_id = "run-abc"
        with patch.object(scryfall_delta, "_remote_updated_at", return_value={
            "oracle_cards":  "2026-09-14T10:00:00Z",
            "default_cards": "2026-09-14T10:00:00Z",
            "rulings":       "2026-09-14T10:00:00Z",
        }):
            with patch("scryfall_ingestion.ScryfallCatalogueIngestion", return_value=instance):
                with patch("scryfall_ingestion.ScryfallClient"):
                    outcome = scryfall_delta.check_and_maybe_invoke(s, dry_run=False)
        self.assertEqual(outcome.action, "invoked")


# ─── Identifier ON CONFLICT target regression ────────────────────────────
#
# Documents the Stage 1C schema invariant: UNIQUE(printing_id, provider,
# identifier_type) was DROPPED by migration 2026-09-14d because MTGJSON
# legitimately maps some Scryfall printings to multiple UUIDs.
# Retained UNIQUE is (provider, identifier_type, identifier_value).
# Any code that inserts here MUST target that constraint.

class IdentifierConflictTargetTests(unittest.TestCase):
    EXPECTED_TARGET = "provider,identifier_type,identifier_value"
    FORBIDDEN_TARGETS = {
        "printing_id,provider,identifier_type",
    }

    def test_identifier_delta_public_constant(self):
        self.assertEqual(identifier_delta.IDENTIFIER_CONFLICT_TARGET, self.EXPECTED_TARGET)

    def test_identifier_delta_module_source_uses_correct_target(self):
        import inspect
        src = inspect.getsource(identifier_delta)
        self.assertIn(f'on_conflict = "{self.EXPECTED_TARGET}"', src,
                      "identifier_delta._flush_inserts must use the value-side UNIQUE constraint")
        for bad in self.FORBIDDEN_TARGETS:
            self.assertNotIn(f'on_conflict = "{bad}"', src,
                             f"identifier_delta must not target the removed constraint {bad!r}")

    def test_apply_stream_dry_run_writes_nothing_but_counts_correctly(self):
        # Belt-and-braces: confirm the pure-logic core does not depend on
        # the on-conflict target at all; the target is only used at
        # flush time.
        stats = identifier_delta.IdentifierDeltaStats()
        entries = [
            ("mtg-uuid-A", {"identifiers": {"scryfallId": "scry-A"}}),
            ("mtg-uuid-B", {"identifiers": {"scryfallId": "scry-B"}}),
        ]
        identifier_delta.apply_stream(
            iter(entries),
            known_uuids={"mtg-uuid-A"},
            scryfall_to_printing={"scry-A": "print-A", "scry-B": "print-B"},
            supabase=None, dry_run=True, stats=stats,
        )
        self.assertEqual(stats.already_known, 1)
        self.assertEqual(stats.newly_inserted, 1)


# ─── Gap-repair classification + notes serialisation ─────────────────────

class GapRepairNotesTests(unittest.TestCase):
    def test_run_to_notes_records_sha(self):
        run = gap_repair.GapRepairRun()
        run.downloaded_allprices = True
        run.allprices_sha256 = "abcd" * 16
        run.dates_repaired = ["2026-08-10"]
        run.dates_source_confirmed = ["2026-08-06", "2026-08-29"]
        notes = run.to_notes()
        self.assertEqual(notes["allprices_sha256"], "abcd" * 16)
        self.assertEqual(notes["classifications"]["2026-08-06"]["status"], "source_side_confirmed")
        self.assertEqual(notes["classifications"]["2026-08-10"]["status"], "ingestion_side_repaired")
        self.assertEqual(notes["classifications"]["2026-08-06"]["sha256"], "abcd" * 16)

    def test_load_prior_classifications_infers_from_backfill_notes(self):
        # 2026-08-06: obs_source_for_date=0 → source_side_confirmed.
        # 2026-08-10: rows inserted → ingestion_side_repaired.
        s = StubSupabaseClient()
        s.enqueue(200, [
            {"started_at": "2026-09-15T12:40:00Z", "notes": json.dumps({
                "only_date": "2026-08-06",
                "obs_source_for_date": 0,
                "obs_mapped": 0,
                "inserted_new": 0,
                "sha256_actual": "sha-A",
            })},
            {"started_at": "2026-09-15T12:41:00Z", "notes": json.dumps({
                "only_date": "2026-08-10",
                "obs_source_for_date": 700_000,
                "obs_mapped": 700_000,
                "inserted_new": 700_000,
                "sha256_actual": "sha-A",
            })},
            # Row without only_date — normal daily ingest, ignored.
            {"started_at": "2026-09-15T12:42:00Z", "notes": json.dumps({
                "obs_mapped": 700000, "inserted_new": 700000,
            })},
        ])
        prior = gap_repair.load_prior_classifications(s)
        self.assertEqual(prior["2026-08-06"]["status"], "source_side_confirmed")
        self.assertEqual(prior["2026-08-06"]["sha256"], "sha-A")
        self.assertFalse(prior["2026-08-06"]["legacy"])
        self.assertEqual(prior["2026-08-10"]["status"], "ingestion_side_repaired")
        self.assertEqual(prior["2026-08-10"]["sha256"], "sha-A")
        self.assertFalse(prior["2026-08-10"]["legacy"])

    def test_load_prior_classifications_legacy_fallback(self):
        """Rows written before obs_source_for_date existed must fall
        back to obs_mapped; the ``legacy`` marker flags them so a caller
        can decide to re-probe."""
        s = StubSupabaseClient()
        s.enqueue(200, [
            {"started_at": "2026-09-15T12:40:00Z", "notes": json.dumps({
                "only_date": "2026-08-06",
                # NOTE: no obs_source_for_date key
                "obs_mapped": 0,
                "inserted_new": 0,
                "sha256_actual": "sha-legacy",
            })},
        ])
        prior = gap_repair.load_prior_classifications(s)
        self.assertEqual(prior["2026-08-06"]["status"], "source_side_confirmed")
        self.assertTrue(prior["2026-08-06"]["legacy"])


# ─── Current-price initial-population semantics ───────────────────────────

class CurrentPriceRefreshTests(unittest.TestCase):
    def test_look_back_days_default_incremental(self):
        # incremental should use the 3-day default.
        self.assertEqual(current_prices.INCREMENTAL_LOOK_BACK_DAYS, 3)

    def test_look_back_days_initial_population(self):
        # initial population uses a 30-day window.
        self.assertEqual(current_prices.INITIAL_POPULATION_LOOK_BACK_DAYS, 30)


# ─── Duplicate-build fallback: skip current-price refresh ─────────────────
# The 18:00 UTC fallback cron re-runs after the 15:00 UTC primary. When
# that fallback hits skipped_duplicate_build, the current-price refresh
# is redundant — the previous run already wrote it. This suite pins the
# decision logic.

from mtg_daily_pipeline import _should_skip_current_refresh, _should_run_identifier_delta   # noqa: E402


class SkipCurrentPriceRefreshDecisionTests(unittest.TestCase):
    def test_skip_flag_wins(self):
        self.assertEqual(
            _should_skip_current_refresh(True, False, False, {"inserted_new": 100}),
            (True, "skip_flag"),
        )

    def test_initial_population_forces_run(self):
        self.assertEqual(
            _should_skip_current_refresh(False, True, False, {"skipped": True}),
            (False, None),
        )

    def test_force_flag_forces_run(self):
        self.assertEqual(
            _should_skip_current_refresh(False, False, True, {"action": "skipped_duplicate_build"}),
            (False, None),
        )

    def test_skip_on_duplicate_build(self):
        outcome = _should_skip_current_refresh(
            False, False, False, {"action": "skipped_duplicate_build", "target_date": "2026-09-14"},
        )
        self.assertEqual(outcome, (True, "duplicate_build"))

    def test_skip_on_prices_step_skipped(self):
        self.assertEqual(
            _should_skip_current_refresh(False, False, False, {"skipped": True}),
            (True, "prices_step_skipped"),
        )

    def test_skip_on_missing_prices_summary(self):
        self.assertEqual(
            _should_skip_current_refresh(False, False, False, None),
            (True, "no_prices_step"),
        )

    def test_skip_on_zero_inserted(self):
        # A step that ran but wrote nothing (all conflicts) should also
        # skip refresh — the DB state hasn't changed from last time.
        self.assertEqual(
            _should_skip_current_refresh(
                False, False, False,
                {"status": "success", "inserted_new": 0, "conflict_skipped": 700000},
            ),
            (True, "no_new_prices"),
        )

    def test_skip_on_prices_error(self):
        self.assertEqual(
            _should_skip_current_refresh(False, False, False, {"error": "boom"}),
            (True, "prices_step_errored"),
        )

    def test_run_when_new_prices_landed(self):
        self.assertEqual(
            _should_skip_current_refresh(
                False, False, False,
                {"status": "success", "inserted_new": 727290},
            ),
            (False, None),
        )


# ─── Identifier-delta gating (build-guard-first reorder) ─────────────────
# The 18:00 UTC fallback cron must not download the ~50 MB
# AllIdentifiers file when the price build was already ingested by the
# 15:00 UTC primary. This suite pins the decision logic and the
# expected skip reasons.

class RunIdentifierDeltaDecisionTests(unittest.TestCase):
    def test_skip_flag_wins(self):
        self.assertEqual(
            _should_run_identifier_delta(True, False, {"inserted_new": 100}),
            (False, "skip_flag"),
        )

    def test_force_flag_wins(self):
        self.assertEqual(
            _should_run_identifier_delta(
                False, True, {"action": "skipped_duplicate_build"},
            ),
            (True, None),
        )

    def test_skip_on_duplicate_build(self):
        # This is the 18:00 fallback case the reorder is designed for.
        self.assertEqual(
            _should_run_identifier_delta(
                False, False, {"action": "skipped_duplicate_build", "target_date": "2026-09-14"},
            ),
            (False, "duplicate_build"),
        )

    def test_skip_when_prices_skipped(self):
        self.assertEqual(
            _should_run_identifier_delta(False, False, {"skipped": True}),
            (False, "prices_step_skipped"),
        )

    def test_skip_when_prices_errored(self):
        self.assertEqual(
            _should_run_identifier_delta(False, False, {"error": "boom"}),
            (False, "prices_step_errored"),
        )

    def test_skip_when_no_prices_summary(self):
        # If we somehow reach the identifier step with no prices summary
        # (should not happen given the reorder, but defence in depth):
        self.assertEqual(
            _should_run_identifier_delta(False, False, None),
            (False, "no_prices_step"),
        )

    def test_run_when_prices_pending_or_ingested(self):
        # Pending: the pipeline sets this after build guard passes but
        # BEFORE the actual ingest runs (order: G-scryfall → H-identifiers
        # → I-ingest). Identifier delta MUST run in that window.
        self.assertEqual(
            _should_run_identifier_delta(
                False, False, {"target_date": "2026-09-15", "action": "pending_ingest"},
            ),
            (True, None),
        )
        # Post-ingest state should also permit rerun (though pipeline
        # order means this path isn't exercised in production).
        self.assertEqual(
            _should_run_identifier_delta(
                False, False, {"status": "success", "inserted_new": 727290},
            ),
            (True, None),
        )


# ─── Scryfall freshness / invocation split ────────────────────────────────
# The scryfall_delta module now separates the cheap freshness probe
# from the expensive ingester invocation, so the pipeline can gate the
# invocation on the build guard result.

class ScryfallCheckFreshnessTests(unittest.TestCase):
    def test_check_freshness_returns_unchanged_when_matched(self):
        s = StubSupabaseClient()
        # Local: same three updated_at values.
        s.enqueue(200, [{
            "id": "prev-1", "status": "success",
            "notes": json.dumps({
                "oracle_updated_at":  "2026-09-15T09:00:00Z",
                "default_updated_at": "2026-09-15T09:05:00Z",
                "rulings_updated_at": "2026-09-15T09:00:30Z",
                "errors": 0,
            }),
        }])
        with patch.object(scryfall_delta, "_remote_updated_at", return_value={
            "oracle_cards":  "2026-09-15T09:00:00Z",
            "default_cards": "2026-09-15T09:05:00Z",
            "rulings":       "2026-09-15T09:00:30Z",
        }):
            f = scryfall_delta.check_freshness(s)
        self.assertFalse(f.changed)

    def test_check_freshness_detects_move(self):
        s = StubSupabaseClient()
        s.enqueue(200, [{
            "id": "prev-1", "status": "success",
            "notes": json.dumps({
                "oracle_updated_at":  "2026-09-14T09:00:00Z",
                "default_updated_at": "2026-09-14T09:05:00Z",
                "rulings_updated_at": "2026-09-14T09:00:30Z",
                "errors": 0,
            }),
        }])
        with patch.object(scryfall_delta, "_remote_updated_at", return_value={
            "oracle_cards":  "2026-09-15T09:00:00Z",
            "default_cards": "2026-09-14T09:05:00Z",
            "rulings":       "2026-09-14T09:00:30Z",
        }):
            f = scryfall_delta.check_freshness(s)
        self.assertTrue(f.changed)

    def test_check_and_maybe_invoke_defer_invocation(self):
        """With defer_invocation=True the wrapper must NOT call the
        Scryfall ingester even when the bulk data moved. This is the
        18:00 fallback path."""
        s = StubSupabaseClient()
        s.enqueue(200, [{
            "id": "prev-1", "status": "success",
            "notes": json.dumps({
                "oracle_updated_at":  "2026-09-14T09:00:00Z",
                "default_updated_at": "2026-09-14T09:05:00Z",
                "rulings_updated_at": "2026-09-14T09:00:30Z",
                "errors": 0,
            }),
        }])
        with patch.object(scryfall_delta, "_remote_updated_at", return_value={
            "oracle_cards":  "2026-09-15T09:00:00Z",
            "default_cards": "2026-09-15T09:05:00Z",
            "rulings":       "2026-09-15T09:00:30Z",
        }):
            with patch("scryfall_ingestion.ScryfallCatalogueIngestion") as CtorMock:
                outcome = scryfall_delta.check_and_maybe_invoke(
                    s, dry_run=False, defer_invocation=True,
                )
        self.assertEqual(outcome.action, "changed_deferred")
        self.assertIsNotNone(outcome.reason)
        CtorMock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
