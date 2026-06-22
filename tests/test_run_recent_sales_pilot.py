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
