"""
Tests for scripts/run_recent_sales_pilot.py — Block 4B-S-2A.

Coverage focuses on the rate-limit-handling helper and the CLI flag
surface. No live HTTP request is made: every fetch goes through a
``FakeSession`` and ``time.sleep`` is replaced with a recording stub.
"""
from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from scripts import run_recent_sales_pilot as pilot


# ────────────────────────────────────────────────────────────────────────────
# Fake HTTP session
# ────────────────────────────────────────────────────────────────────────────

class _FakeResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class FakeSession:
    """Returns a pre-scripted sequence of HTTP statuses. Falls back to 200
    once the script runs out (so a passing fetch can finish a long retry
    test cleanly)."""

    def __init__(self, status_sequence: list[int] | None = None,
                 default_status: int = 200, default_text: str = "<html>OK</html>",
                 raise_on_first: Exception | None = None):
        self._statuses = list(status_sequence or [])
        self._default_status = default_status
        self._default_text = default_text
        self._raise_on_first = raise_on_first
        self.calls: list[str] = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        if self._raise_on_first is not None:
            exc, self._raise_on_first = self._raise_on_first, None
            raise exc
        if self._statuses:
            status = self._statuses.pop(0)
        else:
            status = self._default_status
        text = self._default_text if status == 200 else ""
        return _FakeResponse(status, text)


class RecordingSleep:
    def __init__(self):
        self.waits: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.waits.append(seconds)


# ────────────────────────────────────────────────────────────────────────────
# argparse defaults
# ────────────────────────────────────────────────────────────────────────────

def test_parse_args_defaults():
    args = pilot._parse_args([])
    assert args.delay_seconds == pilot.DEFAULT_DELAY_SECONDS == 1.5
    assert args.max_retries == pilot.DEFAULT_MAX_RETRIES == 3
    assert args.retry_backoff_seconds == pilot.DEFAULT_RETRY_BACKOFF_SECONDS == 10.0
    assert args.dry_run is False
    assert args.limit is None
    assert args.import_type == "recent_sales_pilot"


def test_parse_args_overrides_propagate():
    args = pilot._parse_args([
        "--delay-seconds", "2.5",
        "--max-retries", "5",
        "--retry-backoff-seconds", "15",
        "--limit", "7",
        "--dry-run",
        "--import-type", "recent_sales_admin_manual",
    ])
    assert args.delay_seconds == 2.5
    assert args.max_retries == 5
    assert args.retry_backoff_seconds == 15.0
    assert args.limit == 7
    assert args.dry_run is True
    assert args.import_type == "recent_sales_admin_manual"


# ────────────────────────────────────────────────────────────────────────────
# _fetch_with_retry — happy path
# ────────────────────────────────────────────────────────────────────────────

def test_fetch_with_retry_returns_ok_on_200():
    sess = FakeSession([200])
    sleep = RecordingSleep()
    html, outcome = pilot._fetch_with_retry(
        "https://example/x",
        max_retries=3, retry_backoff_seconds=10.0,
        session=sess, sleep=sleep,
    )
    assert outcome == pilot.FETCH_OK
    assert html == "<html>OK</html>"
    assert len(sess.calls) == 1
    assert sleep.waits == []  # no retry → no sleep


# ────────────────────────────────────────────────────────────────────────────
# _fetch_with_retry — 429 retry behaviour
# ────────────────────────────────────────────────────────────────────────────

def test_fetch_with_retry_retries_on_429_then_succeeds():
    sess = FakeSession([429, 200])
    sleep = RecordingSleep()
    html, outcome = pilot._fetch_with_retry(
        "https://example/x",
        max_retries=3, retry_backoff_seconds=10.0,
        session=sess, sleep=sleep,
    )
    assert outcome == pilot.FETCH_OK
    assert html == "<html>OK</html>"
    assert len(sess.calls) == 2
    # Linear backoff: attempt 1 → 10s
    assert sleep.waits == [10.0]


def test_fetch_with_retry_backoff_sequence_uses_attempt_multiplier():
    sess = FakeSession([429, 429, 429, 200])
    sleep = RecordingSleep()
    html, outcome = pilot._fetch_with_retry(
        "https://example/x",
        max_retries=3, retry_backoff_seconds=10.0,
        session=sess, sleep=sleep,
    )
    assert outcome == pilot.FETCH_OK
    assert html == "<html>OK</html>"
    # 3 retries: 10, 20, 30
    assert sleep.waits == [10.0, 20.0, 30.0]
    assert len(sess.calls) == 4  # 1 initial + 3 retries


def test_fetch_with_retry_gives_up_after_max_retries():
    sess = FakeSession([429, 429, 429, 429])
    sleep = RecordingSleep()
    html, outcome = pilot._fetch_with_retry(
        "https://example/x",
        max_retries=3, retry_backoff_seconds=10.0,
        session=sess, sleep=sleep,
    )
    assert outcome == pilot.FETCH_SKIPPED_429
    assert html is None
    # 3 backoffs (between the 4 attempts), no 4th sleep after final failure
    assert sleep.waits == [10.0, 20.0, 30.0]
    assert len(sess.calls) == 4  # initial + 3 retries; the loop exits


def test_fetch_with_retry_honours_zero_retries():
    sess = FakeSession([429, 200])
    sleep = RecordingSleep()
    html, outcome = pilot._fetch_with_retry(
        "https://example/x",
        max_retries=0, retry_backoff_seconds=10.0,
        session=sess, sleep=sleep,
    )
    assert outcome == pilot.FETCH_SKIPPED_429
    assert html is None
    assert len(sess.calls) == 1  # single attempt, no retry
    assert sleep.waits == []


# ────────────────────────────────────────────────────────────────────────────
# _fetch_with_retry — other HTTP errors do NOT retry
# ────────────────────────────────────────────────────────────────────────────

def test_fetch_with_retry_skips_404_without_retry():
    sess = FakeSession([404])
    sleep = RecordingSleep()
    html, outcome = pilot._fetch_with_retry(
        "https://example/x",
        max_retries=3, retry_backoff_seconds=10.0,
        session=sess, sleep=sleep,
    )
    assert outcome == pilot.FETCH_SKIPPED_HTTP_ERROR
    assert html is None
    assert len(sess.calls) == 1
    assert sleep.waits == []


def test_fetch_with_retry_skips_500_without_retry():
    sess = FakeSession([500])
    sleep = RecordingSleep()
    html, outcome = pilot._fetch_with_retry(
        "https://example/x",
        max_retries=3, retry_backoff_seconds=10.0,
        session=sess, sleep=sleep,
    )
    assert outcome == pilot.FETCH_SKIPPED_HTTP_ERROR
    assert html is None
    assert len(sess.calls) == 1


def test_fetch_with_retry_skips_403_without_retry():
    sess = FakeSession([403])
    sleep = RecordingSleep()
    html, outcome = pilot._fetch_with_retry(
        "https://example/x",
        max_retries=3, retry_backoff_seconds=10.0,
        session=sess, sleep=sleep,
    )
    assert outcome == pilot.FETCH_SKIPPED_HTTP_ERROR
    assert html is None


# ────────────────────────────────────────────────────────────────────────────
# _fetch_with_retry — network exceptions classify as skipped_no_html
# ────────────────────────────────────────────────────────────────────────────

def test_fetch_with_retry_returns_skipped_no_html_on_network_error():
    sess = FakeSession(raise_on_first=ConnectionError("dns boom"))
    sleep = RecordingSleep()
    html, outcome = pilot._fetch_with_retry(
        "https://example/x",
        max_retries=3, retry_backoff_seconds=10.0,
        session=sess, sleep=sleep,
    )
    assert outcome == pilot.FETCH_SKIPPED_NO_HTML
    assert html is None
    # Network exception is NOT retried (different failure mode than 429)
    assert len(sess.calls) == 1


# ────────────────────────────────────────────────────────────────────────────
# Constant identity — runner exports the tokens the main loop branches on
# ────────────────────────────────────────────────────────────────────────────

def test_fetch_outcome_tokens_are_disjoint_strings():
    tokens = {
        pilot.FETCH_OK,
        pilot.FETCH_SKIPPED_NO_HTML,
        pilot.FETCH_SKIPPED_429,
        pilot.FETCH_SKIPPED_HTTP_ERROR,
    }
    assert len(tokens) == 4
    for t in tokens:
        assert isinstance(t, str) and t


# ────────────────────────────────────────────────────────────────────────────
# Block 4B-S-5A — weekly rotation: --offset, --batch-size, _select_batch
# ────────────────────────────────────────────────────────────────────────────


def _mk_cards(pcids):
    """Build the shape that ``load_cards_from_pc_csvs`` returns."""
    return [{"pc_id": str(p), "url": f"https://example/{p}",
             "product_name": f"Card {p}"} for p in pcids]


# ── argparse: new flags ──────────────────────────────────────────────────────

def test_parse_args_offset_defaults_to_zero():
    args = pilot._parse_args([])
    assert args.offset == 0


def test_parse_args_offset_propagates():
    args = pilot._parse_args(["--offset", "6000"])
    assert args.offset == 6000


def test_parse_args_batch_size_is_alias_for_limit():
    args = pilot._parse_args(["--batch-size", "3000"])
    assert args.limit == 3000


def test_parse_args_limit_still_works_for_back_compat():
    # The 4B-S-4A invocation `--limit 3000` must keep parsing identically.
    args = pilot._parse_args(["--limit", "3000"])
    assert args.limit == 3000
    assert args.offset == 0


def test_parse_args_does_not_introduce_full_catalogue_flag():
    # Belt-and-braces: this block must NOT have added any opt-out of the
    # allow-list. Any such flag would have to land via argparse, so a
    # `--full-catalogue` / `--ignore-allow-list` would surface here.
    with pytest.raises(SystemExit):
        pilot._parse_args(["--full-catalogue"])
    with pytest.raises(SystemExit):
        pilot._parse_args(["--ignore-allow-list"])
    with pytest.raises(SystemExit):
        pilot._parse_args(["--all"])


# ── _sort_allowlist ──────────────────────────────────────────────────────────

def test_sort_allowlist_numeric_when_all_digits():
    # "9" must precede "10" — naive lexicographic sort fails this.
    cards = _mk_cards(["10", "2", "9", "100", "1"])
    out = pilot._sort_allowlist(cards)
    assert [c["pc_id"] for c in out] == ["1", "2", "9", "10", "100"]


def test_sort_allowlist_text_when_any_non_digit():
    # Any non-digit forces lexicographic — still deterministic.
    cards = _mk_cards(["10", "9", "abc"])
    out = pilot._sort_allowlist(cards)
    assert [c["pc_id"] for c in out] == ["10", "9", "abc"]


def test_sort_allowlist_is_pure_returns_new_list():
    cards = _mk_cards(["3", "1", "2"])
    original = [c["pc_id"] for c in cards]
    pilot._sort_allowlist(cards)
    assert [c["pc_id"] for c in cards] == original  # not mutated


def test_sort_allowlist_handles_empty():
    assert pilot._sort_allowlist([]) == []


# ── _select_batch — slicing ──────────────────────────────────────────────────

def test_select_batch_offset_zero_returns_first_batch():
    cards = _mk_cards(range(1, 10))  # ids "1".."9"
    batch, meta = pilot._select_batch(cards, offset=0, limit=3)
    assert [c["pc_id"] for c in batch] == ["1", "2", "3"]
    assert meta["allow_list_total"] == 9
    assert meta["offset"] == 0
    assert meta["effective_offset"] == 0
    assert meta["batch_size"] == 3
    assert meta["selected_start"] == 1
    assert meta["selected_end"] == 3


def test_select_batch_offset_3_returns_second_batch():
    cards = _mk_cards(range(1, 10))
    batch, meta = pilot._select_batch(cards, offset=3, limit=3)
    assert [c["pc_id"] for c in batch] == ["4", "5", "6"]
    assert meta["effective_offset"] == 3
    assert meta["selected_start"] == 4
    assert meta["selected_end"] == 6


def test_select_batch_offset_6_returns_third_batch():
    cards = _mk_cards(range(1, 10))
    batch, meta = pilot._select_batch(cards, offset=6, limit=3)
    assert [c["pc_id"] for c in batch] == ["7", "8", "9"]
    assert meta["selected_start"] == 7
    assert meta["selected_end"] == 9


def test_select_batch_realistic_3000_card_second_window():
    # Mirrors the live Tuesday batch: 17,949-row allow-list, second 3,000.
    cards = _mk_cards(range(1, 17_950))  # 17,949 entries
    batch, meta = pilot._select_batch(cards, offset=3000, limit=3000)
    assert len(batch) == 3000
    assert batch[0]["pc_id"] == "3001"
    assert batch[-1]["pc_id"] == "6000"
    assert meta["allow_list_total"] == 17_949
    assert meta["effective_offset"] == 3000
    assert meta["selected_start"] == 3001
    assert meta["selected_end"] == 6000


def test_select_batch_saturday_partial_tail():
    # offset 15000, total 17949 → 2949 rows, no wrap within the batch.
    cards = _mk_cards(range(1, 17_950))
    batch, meta = pilot._select_batch(cards, offset=15_000, limit=3000)
    assert len(batch) == 2949
    assert batch[0]["pc_id"] == "15001"
    assert batch[-1]["pc_id"] == "17949"
    assert meta["selected_start"] == 15_001
    assert meta["selected_end"] == 17_949


def test_select_batch_offset_overflow_wraps_with_modulo():
    # Day-of-week math could overshoot if the workflow gets it wrong.
    # 18000 % 17949 = 51; the batch starts at sorted[51].
    cards = _mk_cards(range(1, 17_950))
    batch, meta = pilot._select_batch(cards, offset=18_000, limit=3000)
    assert len(batch) == 3000
    assert meta["offset"] == 18_000  # raw value preserved for audit
    assert meta["effective_offset"] == 51
    assert batch[0]["pc_id"] == "52"
    assert batch[-1]["pc_id"] == "3051"


def test_select_batch_offset_exactly_total_wraps_to_zero():
    cards = _mk_cards(range(1, 10))
    batch, meta = pilot._select_batch(cards, offset=9, limit=3)
    assert meta["effective_offset"] == 0
    assert [c["pc_id"] for c in batch] == ["1", "2", "3"]


def test_select_batch_limit_none_returns_tail_from_offset():
    cards = _mk_cards(range(1, 10))
    batch, meta = pilot._select_batch(cards, offset=4, limit=None)
    # 4 → start at sorted index 4 → "5" onward
    assert [c["pc_id"] for c in batch] == ["5", "6", "7", "8", "9"]
    assert meta["batch_size"] is None
    assert meta["selected_start"] == 5
    assert meta["selected_end"] == 9


def test_select_batch_empty_allowlist_returns_empty_with_meta():
    batch, meta = pilot._select_batch([], offset=0, limit=3000)
    assert batch == []
    assert meta["allow_list_total"] == 0
    assert meta["effective_offset"] == 0
    # selected_start/end stay 0 when nothing was selected so dashboard
    # queries can distinguish "this run had nothing" from "this run
    # processed row 0".
    assert meta["selected_start"] == 0
    assert meta["selected_end"] == 0


def test_select_batch_meta_keys_are_stable():
    # The ingestion module forwards this dict verbatim into
    # market_import_runs.notes via add_run_notes(**meta). Lock the
    # key set so a downstream dashboard query never silently breaks.
    cards = _mk_cards(range(1, 10))
    _, meta = pilot._select_batch(cards, offset=3, limit=3)
    assert set(meta.keys()) == {
        "allow_list_total", "offset", "effective_offset",
        "batch_size", "selected_start", "selected_end",
    }


def test_select_batch_uses_numeric_sort_so_ordering_matches_day_offsets():
    # Without numeric sort, "10" would come before "2", which would shift
    # the day-of-week boundaries off by an unknown amount. This guards
    # against a regression in _sort_allowlist that breaks rotation.
    cards = _mk_cards(["100", "20", "3"])
    batch, _ = pilot._select_batch(cards, offset=0, limit=1)
    assert [c["pc_id"] for c in batch] == ["3"]
    batch2, _ = pilot._select_batch(cards, offset=1, limit=1)
    assert [c["pc_id"] for c in batch2] == ["20"]
