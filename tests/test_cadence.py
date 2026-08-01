"""
tests/test_cadence.py
====================
Block 5A-W-49 — cadence classifier tests.

Every test targets one requirement from the block brief. All pure —
no live DB, no HTTP. Bulk-loader tests use an injected fake http_get.
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


cadence = _load("cadence", "cadence.py")


class TestClassifyBoundary(unittest.TestCase):
    """$2.00 exact = daily; $1.99 = weekly."""

    def test_two_dollars_exact_is_daily_value(self):
        self.assertEqual(cadence.classify(200), cadence.Cohort.DAILY_VALUE)

    def test_one_ninety_nine_is_weekly_low(self):
        self.assertEqual(cadence.classify(199), cadence.Cohort.WEEKLY_LOW)

    def test_higher_than_two_is_daily_value(self):
        self.assertEqual(cadence.classify(500), cadence.Cohort.DAILY_VALUE)
        self.assertEqual(cadence.classify(100_000_000), cadence.Cohort.DAILY_VALUE)

    def test_one_cent_is_weekly_low(self):
        self.assertEqual(cadence.classify(1), cadence.Cohort.WEEKLY_LOW)


class TestClassifyLatestRawWins(unittest.TestCase):
    """Cadence uses latest_raw_cents ONLY. PSA/CSV loose price never
    affects the classification."""

    def test_zero_raw_is_discovery(self):
        self.assertEqual(cadence.classify(0), cadence.Cohort.DAILY_DISCOVERY)

    def test_none_raw_is_discovery(self):
        self.assertEqual(cadence.classify(None), cadence.Cohort.DAILY_DISCOVERY)

    def test_psa10_price_does_not_promote_low_raw(self):
        # The classifier signature doesn't even accept PSA prices — if
        # a caller ever passes them, they cannot influence the result.
        self.assertEqual(cadence.classify(150), cadence.Cohort.WEEKLY_LOW)


class TestClassifyDiscoveryVsUnpriced(unittest.TestCase):
    """Newly imported cards + transient failures stay daily.
    Three successful no-price fetches move to weekly."""

    def test_new_card_is_daily_discovery(self):
        self.assertEqual(cadence.classify(None), cadence.Cohort.DAILY_DISCOVERY)

    def test_transient_failure_stays_daily_regardless_of_streak(self):
        # Per brief: a transport error (429/5xx/timeout) is NOT
        # evidence-of-no-price. The card is retried the next day even
        # if the counter happens to be high.
        for cat in ("transient_http_failure", "timeout"):
            self.assertEqual(
                cadence.classify(None, last_result_category=cat, no_price_streak=10),
                cadence.Cohort.DAILY_DISCOVERY,
            )
            self.assertEqual(
                cadence.classify(None, last_result_category=cat, no_price_streak=0),
                cadence.Cohort.DAILY_DISCOVERY,
            )

    def test_streak_below_threshold_is_daily(self):
        for streak in (0, 1, 2):
            self.assertEqual(
                cadence.classify(None, "page_reached_no_price", streak),
                cadence.Cohort.DAILY_DISCOVERY,
            )

    def test_three_successful_no_price_becomes_weekly(self):
        self.assertEqual(
            cadence.classify(None, "page_reached_no_price", 3),
            cadence.Cohort.WEEKLY_UNPRICED,
        )

    def test_later_successful_price_clears_no_price_state(self):
        # Even with a large no_price_streak, a raw price observation
        # takes precedence — that's the whole point of "latest raw wins".
        self.assertEqual(
            cadence.classify(1000, "page_reached_no_price", 5),
            cadence.Cohort.DAILY_VALUE,
        )
        self.assertEqual(
            cadence.classify(100, "page_reached_no_price", 5),
            cadence.Cohort.WEEKLY_LOW,
        )


class TestCadenceMovement(unittest.TestCase):
    """Cards move between daily and weekly cohorts as prices cross $2."""

    def test_weekly_card_moves_to_daily_when_price_rises_past_two(self):
        self.assertEqual(cadence.classify(190), cadence.Cohort.WEEKLY_LOW)
        self.assertEqual(cadence.classify(210), cadence.Cohort.DAILY_VALUE)

    def test_daily_card_moves_to_weekly_when_price_falls_below_two(self):
        self.assertEqual(cadence.classify(210), cadence.Cohort.DAILY_VALUE)
        self.assertEqual(cadence.classify(190), cadence.Cohort.WEEKLY_LOW)


class TestIncludeForCadence(unittest.TestCase):
    def test_daily_selects_daily_cohorts(self):
        for coh in cadence.DAILY_COHORTS:
            self.assertTrue(cadence.include_for_cadence(coh, "daily"))
        for coh in cadence.WEEKLY_COHORTS:
            self.assertFalse(cadence.include_for_cadence(coh, "daily"))

    def test_weekly_selects_weekly_cohorts(self):
        for coh in cadence.WEEKLY_COHORTS:
            self.assertTrue(cadence.include_for_cadence(coh, "weekly"))
        for coh in cadence.DAILY_COHORTS:
            self.assertFalse(cadence.include_for_cadence(coh, "weekly"))

    def test_all_selects_every_cohort(self):
        for coh in cadence.Cohort:
            self.assertTrue(cadence.include_for_cadence(coh, "all"))

    def test_daily_and_weekly_are_disjoint(self):
        self.assertEqual(cadence.DAILY_COHORTS & cadence.WEEKLY_COHORTS, set())

    def test_daily_union_weekly_is_complete(self):
        self.assertEqual(
            cadence.DAILY_COHORTS | cadence.WEEKLY_COHORTS,
            set(cadence.Cohort),
        )

    def test_unknown_cadence_raises(self):
        with self.assertRaises(ValueError):
            cadence.include_for_cadence(cadence.Cohort.DAILY_VALUE, "hourly")


class TestFilterCards(unittest.TestCase):
    """filter_cards_by_cadence walks the CSV list once and returns
    the subset plus a cohort histogram."""

    def _cards(self, *slugs):
        return [{"card_slug": s, "product-name": f"card {s}"} for s in slugs]

    def _state(self, **kw):
        return kw

    def test_all_returns_every_card(self):
        cards = self._cards("1", "2", "3")
        state = {"1": {"latest_raw_cents": 500},
                 "2": {"latest_raw_cents": 50},
                 "3": {}}
        out, hist = cadence.filter_cards_by_cadence(cards, "all", state)
        self.assertEqual(len(out), 3)
        self.assertEqual(hist[cadence.Cohort.DAILY_VALUE], 1)
        self.assertEqual(hist[cadence.Cohort.WEEKLY_LOW], 1)
        self.assertEqual(hist[cadence.Cohort.DAILY_DISCOVERY], 1)

    def test_daily_omits_weekly_cards(self):
        cards = self._cards("1", "2", "3", "4")
        state = {
            "1": {"latest_raw_cents": 250},    # DAILY_VALUE
            "2": {"latest_raw_cents": 99},     # WEEKLY_LOW
            "3": {"latest_raw_cents": None},   # DAILY_DISCOVERY
            "4": {"latest_raw_cents": 199},    # WEEKLY_LOW (boundary)
        }
        out, hist = cadence.filter_cards_by_cadence(cards, "daily", state)
        self.assertEqual({c["card_slug"] for c in out}, {"1", "3"})

    def test_weekly_omits_daily_cards(self):
        cards = self._cards("1", "2", "3", "4")
        state = {
            "1": {"latest_raw_cents": 250},
            "2": {"latest_raw_cents": 99},
            "3": {"latest_raw_cents": None},
            "4": {"latest_raw_cents": 199},
        }
        out, _ = cadence.filter_cards_by_cadence(cards, "weekly", state)
        self.assertEqual({c["card_slug"] for c in out}, {"2", "4"})

    def test_daily_and_weekly_partition_the_catalogue(self):
        # Union of a daily-cadence run + a weekly-cadence run covers
        # every eligible card exactly once.
        cards = self._cards(*[str(i) for i in range(1, 21)])
        state = {}
        # Half priced above $2, half priced below, plus a discovery
        for i in range(1, 11):
            state[str(i)] = {"latest_raw_cents": 200 + i}   # DAILY_VALUE
        for i in range(11, 20):
            state[str(i)] = {"latest_raw_cents": i - 10}    # WEEKLY_LOW
        state["20"] = {}                                     # DAILY_DISCOVERY
        d, _ = cadence.filter_cards_by_cadence(cards, "daily", state)
        w, _ = cadence.filter_cards_by_cadence(cards, "weekly", state)
        d_slugs = {c["card_slug"] for c in d}
        w_slugs = {c["card_slug"] for c in w}
        self.assertEqual(d_slugs & w_slugs, set())
        self.assertEqual(d_slugs | w_slugs, {c["card_slug"] for c in cards})


class TestSlugNormalisationFIX2(unittest.TestCase):
    """Block 5A-W-49-FIX2 — filter_cards_by_cadence must accept both
    bare and pc-prefixed slugs and classify them identically.

    Previously the scraper passed `card_slug=pc-<id>` to a state map
    keyed on `<id>`, so every card fell into DAILY_DISCOVERY. This
    test suite pins the corrected behaviour."""

    def _state(self):
        # 4 cards spanning DAILY_VALUE / WEEKLY_LOW / boundary / no data
        return {
            "1001": {"latest_raw_cents": 500},   # DAILY_VALUE
            "1002": {"latest_raw_cents": 199},   # WEEKLY_LOW
            "1003": {"latest_raw_cents": 200},   # DAILY_VALUE (boundary)
            # 1004: no state → DAILY_DISCOVERY
        }

    def test_bare_and_prefixed_classify_identically(self):
        bare_cards = [
            {"card_slug": "1001"},
            {"card_slug": "1002"},
            {"card_slug": "1003"},
            {"card_slug": "1004"},
        ]
        prefixed_cards = [
            {"card_slug": "pc-1001"},
            {"card_slug": "pc-1002"},
            {"card_slug": "pc-1003"},
            {"card_slug": "pc-1004"},
        ]
        _, hist_bare = cadence.filter_cards_by_cadence(bare_cards, "all", self._state())
        _, hist_pref = cadence.filter_cards_by_cadence(prefixed_cards, "all", self._state())
        self.assertEqual(hist_bare, hist_pref)
        # And each cohort has the expected total
        self.assertEqual(hist_bare[cadence.Cohort.DAILY_VALUE], 2)
        self.assertEqual(hist_bare[cadence.Cohort.WEEKLY_LOW], 1)
        self.assertEqual(hist_bare[cadence.Cohort.DAILY_DISCOVERY], 1)

    def test_prefixed_slug_daily_filter_includes_daily_value(self):
        cards = [{"card_slug": "pc-1001"}]  # DAILY_VALUE
        out, _ = cadence.filter_cards_by_cadence(cards, "daily", self._state())
        self.assertEqual([c["card_slug"] for c in out], ["pc-1001"])

    def test_prefixed_slug_weekly_filter_includes_weekly_low(self):
        cards = [{"card_slug": "pc-1002"}]  # WEEKLY_LOW
        out, _ = cadence.filter_cards_by_cadence(cards, "weekly", self._state())
        self.assertEqual([c["card_slug"] for c in out], ["pc-1002"])

    def test_prefixed_slug_boundary_two_dollars_is_daily(self):
        cards = [{"card_slug": "pc-1003"}]  # exactly $2.00
        out, _ = cadence.filter_cards_by_cadence(cards, "daily", self._state())
        self.assertEqual(len(out), 1)
        out, _ = cadence.filter_cards_by_cadence(cards, "weekly", self._state())
        self.assertEqual(out, [])

    def test_prefixed_slug_one_ninety_nine_is_weekly(self):
        cards = [{"card_slug": "pc-1002"}]   # $1.99
        out, _ = cadence.filter_cards_by_cadence(cards, "weekly", self._state())
        self.assertEqual(len(out), 1)
        out, _ = cadence.filter_cards_by_cadence(cards, "daily", self._state())
        self.assertEqual(out, [])

    def test_no_card_falls_into_discovery_solely_due_to_prefix(self):
        # Every prefixed slug that HAS state must land in the same
        # cohort as its bare equivalent, never DAILY_DISCOVERY-by-mistake.
        state = self._state()
        cards = [{"card_slug": f"pc-{k}"} for k in state]
        out_all, hist = cadence.filter_cards_by_cadence(cards, "all", state)
        # None of the three stateful cards fell into DAILY_DISCOVERY
        self.assertEqual(hist[cadence.Cohort.DAILY_DISCOVERY], 0)

    def test_cadence_all_bypasses_filter_regardless_of_prefix(self):
        cards = [{"card_slug": f"pc-{k}"} for k in ("1001", "1002", "1003", "1004")]
        out, _ = cadence.filter_cards_by_cadence(cards, "all", self._state())
        self.assertEqual(len(out), 4)

    def test_bare_slug_helper(self):
        self.assertEqual(cadence._bare_slug("8330138"), "8330138")
        self.assertEqual(cadence._bare_slug("pc-8330138"), "8330138")
        self.assertEqual(cadence._bare_slug(8330138), "8330138")


class TestBulkLoader(unittest.TestCase):
    """load_state_from_supabase uses paginated bulk queries via
    injected http_get. Missing scrape_attempt_state table is graceful."""

    class FakeResp:
        def __init__(self, status, payload):
            self.status_code = status
            self._payload = payload
        def json(self):
            return self._payload

    def test_missing_state_table_is_graceful(self):
        # card_trends returns one page then empty; scrape_attempt_state
        # returns 404 (migration not yet applied). Loader must still
        # succeed and produce raw-only state.
        calls = []
        def fake_get(url, headers=None, timeout=None):
            calls.append(url)
            if "card_trends" in url and "offset=0" in url:
                return self.FakeResp(200, [
                    {"card_slug": "A", "current_raw": 500},
                    {"card_slug": "B", "current_raw": 99},
                ])
            if "card_trends" in url:
                return self.FakeResp(200, [])
            if "scrape_attempt_state" in url:
                return self.FakeResp(404, {"message": "not found"})
            return self.FakeResp(500, {})

        state = cadence.load_state_from_supabase("http://x", "k", http_get=fake_get)
        self.assertEqual(state["A"]["latest_raw_cents"], 500)
        self.assertEqual(state["B"]["latest_raw_cents"], 99)
        # No streak / category info because the table 404'd — that's fine.
        self.assertNotIn("no_price_streak", state["A"])

    def test_bulk_loader_paginates(self):
        # Two pages of card_trends, one page of scrape_attempt_state.
        def fake_get(url, headers=None, timeout=None):
            if "card_trends" in url and "offset=0" in url:
                return self.FakeResp(200,
                    [{"card_slug": f"c{i:04d}", "current_raw": i}
                     for i in range(1000)])
            if "card_trends" in url and "offset=1000" in url:
                return self.FakeResp(200,
                    [{"card_slug": "z", "current_raw": 999}])
            if "scrape_attempt_state" in url and "offset=0" in url:
                return self.FakeResp(200, [
                    {"card_slug": "c0001",
                     "last_result_category": "page_reached_no_price",
                     "consecutive_successful_no_price_count": 3}
                ])
            return self.FakeResp(200, [])
        state = cadence.load_state_from_supabase("http://x", "k", http_get=fake_get)
        self.assertIn("c0999", state)
        self.assertIn("z", state)
        self.assertEqual(state["z"]["latest_raw_cents"], 999)
        self.assertEqual(state["c0001"]["no_price_streak"], 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
