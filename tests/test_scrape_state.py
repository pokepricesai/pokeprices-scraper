"""
tests/test_scrape_state.py — Block 5A-W-49-FIX2

Verifies the scrape_attempt_state buffered writer computes the
streak / result_category / timestamps correctly and produces
idempotent, batch-safe upsert payloads.
"""

import importlib.util
import os
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(name, filename):
    path = os.path.join(REPO, filename)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ss = _load("scrape_state", "scrape_state.py")


class TestValidCategories(unittest.TestCase):
    def test_invalid_category_raises(self):
        buf = ss.ScrapeStateBuffer()
        with self.assertRaises(ValueError):
            buf.record("1", "made_up_category")


class TestStreakLogic(unittest.TestCase):
    def _pending(self, buf, slug):
        return buf._pending[slug]

    def test_priced_resets_streak_to_zero(self):
        buf = ss.ScrapeStateBuffer(state_map={"7777": {"no_price_streak": 5}})
        buf.record("7777", "priced", http_status=200)
        row = self._pending(buf, "7777")
        self.assertEqual(row["consecutive_successful_no_price_count"], 0)
        self.assertEqual(row["last_result_category"], "priced")
        self.assertIn("last_successful_fetch_at", row)

    def test_page_reached_no_price_increments_streak(self):
        buf = ss.ScrapeStateBuffer(state_map={"7777": {"no_price_streak": 1}})
        buf.record("7777", "page_reached_no_price", http_status=200)
        row = self._pending(buf, "7777")
        self.assertEqual(row["consecutive_successful_no_price_count"], 2)
        self.assertIn("last_successful_fetch_at", row)

    def test_one_no_price_from_zero_becomes_streak_1(self):
        buf = ss.ScrapeStateBuffer()   # no prior state
        buf.record("7777", "page_reached_no_price", http_status=200)
        self.assertEqual(self._pending(buf, "7777")["consecutive_successful_no_price_count"], 1)

    def test_three_no_price_from_zero_reaches_streak_3(self):
        buf = ss.ScrapeStateBuffer()
        buf.record("7777", "page_reached_no_price", http_status=200)
        buf.record("7777", "page_reached_no_price", http_status=200)
        buf.record("7777", "page_reached_no_price", http_status=200)
        self.assertEqual(self._pending(buf, "7777")["consecutive_successful_no_price_count"], 3)

    def test_transient_failure_preserves_streak(self):
        buf = ss.ScrapeStateBuffer(state_map={"7777": {"no_price_streak": 2}})
        buf.record("7777", "transient_http_failure", http_status=503)
        row = self._pending(buf, "7777")
        self.assertEqual(row["consecutive_successful_no_price_count"], 2)
        # And does NOT bump last_successful_fetch_at
        self.assertNotIn("last_successful_fetch_at", row)

    def test_timeout_preserves_streak(self):
        buf = ss.ScrapeStateBuffer(state_map={"7777": {"no_price_streak": 2}})
        buf.record("7777", "timeout")
        row = self._pending(buf, "7777")
        self.assertEqual(row["consecutive_successful_no_price_count"], 2)

    def test_not_found_preserves_streak_and_no_success_ts(self):
        buf = ss.ScrapeStateBuffer(state_map={"7777": {"no_price_streak": 2}})
        buf.record("7777", "not_found", http_status=404)
        row = self._pending(buf, "7777")
        self.assertEqual(row["consecutive_successful_no_price_count"], 2)
        self.assertNotIn("last_successful_fetch_at", row)

    def test_incorrect_product_never_touches_streak(self):
        buf = ss.ScrapeStateBuffer(state_map={"7777": {"no_price_streak": 2}})
        buf.record("7777", "incorrect_product_rejected")
        row = self._pending(buf, "7777")
        self.assertEqual(row["consecutive_successful_no_price_count"], 2)

    def test_later_priced_after_streak_resets(self):
        # streak 2 → priced → streak 0. This is what recovers a card
        # from WEEKLY_UNPRICED back into DAILY_VALUE once a raw price
        # reappears.
        buf = ss.ScrapeStateBuffer(state_map={"7777": {"no_price_streak": 2}})
        buf.record("7777", "priced", http_status=200)
        self.assertEqual(self._pending(buf, "7777")["consecutive_successful_no_price_count"], 0)


class TestBufferSemantics(unittest.TestCase):
    def test_duplicate_records_collapse_by_key(self):
        # Two records for the same card in one buffer flush stay
        # coherent (the later one wins, streak continues correctly).
        buf = ss.ScrapeStateBuffer(state_map={"7777": {"no_price_streak": 0}})
        buf.record("7777", "page_reached_no_price", http_status=200)
        # A second call in the same buffer should honour the pending
        # streak of 1, not reset to prior state's 0.
        buf.record("7777", "page_reached_no_price", http_status=200)
        self.assertEqual(buf._pending["7777"]["consecutive_successful_no_price_count"], 2)
        self.assertEqual(len(buf._pending), 1)   # single row per card

    def test_pending_count(self):
        buf = ss.ScrapeStateBuffer()
        self.assertEqual(buf.pending_count(), 0)
        buf.record("a", "priced")
        buf.record("b", "priced")
        buf.record("c", "priced")
        self.assertEqual(buf.pending_count(), 3)

    def test_bare_slug_only(self):
        # The card_slug written to scrape_attempt_state must be BARE
        # (matches cards.card_slug PK). Callers pass bare in; we don't
        # accept pc-prefixed by policy — but the tests below prove
        # the row payload keeps whatever we're given verbatim so the
        # scraper's discipline is what matters.
        buf = ss.ScrapeStateBuffer()
        buf.record("8330138", "priced")
        self.assertEqual(buf._pending["8330138"]["card_slug"], "8330138")


class TestFlushBatchSafety(unittest.TestCase):
    """Prove flush() uses the correct on_conflict + resolution and
    returns True/False cleanly. Uses monkey-patched requests.post."""

    def setUp(self):
        self._real_post = ss.requests.post
        self.calls = []

    def tearDown(self):
        ss.requests.post = self._real_post

    def test_flush_uses_correct_url_and_headers(self):
        class Resp:
            status_code = 204
            text = ""
        def fake_post(url, json=None, headers=None, timeout=None):
            self.calls.append({"url": url, "json": json, "headers": headers})
            return Resp()
        ss.requests.post = fake_post
        buf = ss.ScrapeStateBuffer()
        for i in range(5):
            buf.record(str(i), "priced", http_status=200)
        ok = buf.flush("http://x", "k")
        self.assertTrue(ok)
        self.assertEqual(len(self.calls), 1)   # 5 rows fit in one batch
        self.assertIn("scrape_attempt_state?on_conflict=card_slug", self.calls[0]["url"])
        self.assertIn("resolution=merge-duplicates", self.calls[0]["headers"]["Prefer"])
        self.assertEqual(buf.pending_count(), 0)   # buffer cleared

    def test_flush_batches_at_batch_size(self):
        class Resp:
            status_code = 204
            text = ""
        def fake_post(url, json=None, headers=None, timeout=None):
            self.calls.append(len(json))
            return Resp()
        ss.requests.post = fake_post
        buf = ss.ScrapeStateBuffer()
        for i in range(250):
            buf.record(f"c{i}", "priced")
        buf.flush("http://x", "k", batch_size=100)
        self.assertEqual(self.calls, [100, 100, 50])

    def test_flush_failure_preserves_buffer(self):
        class Resp:
            status_code = 500
            text = "server on fire"
        def fake_post(url, json=None, headers=None, timeout=None):
            return Resp()
        ss.requests.post = fake_post
        buf = ss.ScrapeStateBuffer()
        buf.record("1", "priced")
        buf.record("2", "priced")
        ok = buf.flush("http://x", "k")
        self.assertFalse(ok)
        self.assertEqual(buf.pending_count(), 2)   # buffer preserved for retry
        self.assertEqual(buf.error_count(), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
