"""
tests/test_url_builders.py
==========================
Block 5A-W-48B — regression tests for the PriceCharting URL builder
in pokeprices_scraper_v8.py and the site-route slug builder in
seed_set_cards.py.

The bug this file exists to prevent: the scraper's `build_url` used to
strip apostrophes, which caused every card whose name contains an
apostrophe (Hop's Bag, Lillie's Clefairy ex, N's Zoroark ex, and so on)
to receive a 302 redirect from PriceCharting and be counted as
"not found". The Japanese Battle Partners pilot import surfaced 71
such cards on the first run.

The fix preserved apostrophes in the scraper's URL builder while
KEEPING them stripped in the seeder's site-route slug builder, because
the site's own /set/.../card/... routes must remain apostrophe-free.
Both invariants are pinned here.
"""

import importlib.util
import os
import sys
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(module_name: str, filename: str):
    """Import a module by absolute file path, avoiding a full package."""
    path = os.path.join(REPO_ROOT, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class TestScraperUrlBuilder(unittest.TestCase):
    """pokeprices_scraper_v8.build_url — PC product-page URL."""

    @classmethod
    def setUpClass(cls):
        cls.v8 = _load("v8", "pokeprices_scraper_v8.py")

    def test_apostrophe_preserved_in_pc_url(self):
        # The three apostrophe cases from the Battle Partners pilot.
        url = self.v8.build_url("Pokemon Japanese Battle Partners", "Hop's Bag #91")
        self.assertIn("hop's-bag-91", url)
        self.assertTrue(url.startswith("https://www.pricecharting.com/game/pokemon-japanese-battle-partners/"))

        url = self.v8.build_url("Pokemon Japanese Battle Partners", "Lillie's Clefairy ex #126")
        self.assertIn("lillie's-clefairy-ex-126", url)

        url = self.v8.build_url("Pokemon Japanese Battle Partners", "N's Zoroark ex #131")
        self.assertIn("n's-zoroark-ex-131", url)

    def test_ordinary_card_without_punctuation_unchanged(self):
        url = self.v8.build_url("Pokemon Base Set", "Pikachu #58")
        # No apostrophe means the slug is bytewise identical to what
        # the scraper produced before the fix.
        self.assertEqual(
            url,
            "https://www.pricecharting.com/game/pokemon-base-set/pikachu-58",
        )

    def test_english_card_with_bracket_variant(self):
        # English card with a `[…]` bracket variant — the fix
        # regex still strips brackets and preserves apostrophes.
        url = self.v8.build_url("Pokemon Base Set", "Charizard [1st Edition] #4")
        self.assertIn("charizard-1st-edition-4", url)
        # Bracket must be gone from the URL.
        self.assertNotIn("[", url)
        self.assertNotIn("]", url)

    def test_hash_symbol_stripped(self):
        # `#` must never enter the URL — Postgres CGI would break.
        url = self.v8.build_url("Pokemon Base Set", "Pikachu #58")
        self.assertNotIn("#", url)

    def test_ampersand_preserved_for_sets_and_names(self):
        # The scraper preserves `&` because PriceCharting keeps it in
        # both console slugs and product names (e.g. Team Magma & Aqua).
        # We just want to make sure the fix didn't accidentally strip it.
        url = self.v8.build_url("Pokemon Team Magma & Team Aqua", "Blaziken #63")
        self.assertIn("&", url)

    def test_lowercased(self):
        # Regression pin: full URL is lowercase, matching PC's canonical form.
        url = self.v8.build_url("Pokemon Base Set", "CHARIZARD #4")
        self.assertEqual(url, url.lower())

    def test_console_comma_dropped(self):
        # Block 5A-W-48D — three Japanese sets have commas in their
        # console-name. PC drops the comma and collapses the
        # resulting double-dash. Verified live against PC.
        url = self.v8.build_url("Pokemon Japanese Darkness, and to Light", "Chansey")
        self.assertEqual(
            url,
            "https://www.pricecharting.com/game/pokemon-japanese-darkness-and-to-light/chansey",
        )
        self.assertNotIn(",", url)
        self.assertNotIn("--", url)

        url = self.v8.build_url("Pokemon Japanese Gold, Silver, New World", "Aipom #190")
        self.assertEqual(
            url,
            "https://www.pricecharting.com/game/pokemon-japanese-gold-silver-new-world/aipom-190",
        )

        url = self.v8.build_url("Pokemon Japanese Golden Sky, Silvery Ocean", "Aipom #82")
        self.assertEqual(
            url,
            "https://www.pricecharting.com/game/pokemon-japanese-golden-sky-silvery-ocean/aipom-82",
        )

    def test_console_colon_preserved(self):
        # Block 5A-W-48D — "Pokemon Japanese Magma VS Aqua: Two Ambitions"
        # keeps its colon in the URL slug. Verified live against PC.
        url = self.v8.build_url(
            "Pokemon Japanese Magma VS Aqua: Two Ambitions",
            "Aerodactyl ex #55",
        )
        self.assertEqual(
            url,
            "https://www.pricecharting.com/game/pokemon-japanese-magma-vs-aqua:-two-ambitions/aerodactyl-ex-55",
        )
        self.assertIn(":", url)

    def test_console_apostrophe_preserved(self):
        # Block 5A-W-48D — apostrophes in the CONSOLE slug behave the
        # same as in card slugs: PC preserves them. Verified live for
        # "Pokemon Japanese 2002 McDonald's".
        url = self.v8.build_url("Pokemon Japanese 2002 McDonald's", "Bulbasaur #1")
        self.assertIn("mcdonald's", url)

    def test_card_slug_hyphen_preserved(self):
        # Block 5A-W-48D-FIX1 — hyphens INSIDE a card name must be
        # preserved. Verified live against PC:
        #   * "Jangmo-o #69"                -> jangmo-o-69
        #   * "3-Pack Blister"              -> 3-pack-blister
        #   * "Professor Sycamore [Event Organizer] #XY-P"
        #                                   -> professor-sycamore-event-organizer-xy-p
        url = self.v8.build_url("Pokemon Japanese Alter Genesis", "Jangmo-o #69")
        self.assertEqual(
            url,
            "https://www.pricecharting.com/game/pokemon-japanese-alter-genesis/jangmo-o-69",
        )
        url = self.v8.build_url("Pokemon Japanese 20th Anniversary", "3-Pack Blister")
        self.assertEqual(
            url,
            "https://www.pricecharting.com/game/pokemon-japanese-20th-anniversary/3-pack-blister",
        )
        url = self.v8.build_url("Pokemon Japanese Promo",
                                "Professor Sycamore [Event Organizer] #XY-P")
        self.assertEqual(
            url,
            "https://www.pricecharting.com/game/pokemon-japanese-promo/"
            "professor-sycamore-event-organizer-xy-p",
        )

    def test_card_slug_slash_stripped_but_hyphen_kept(self):
        # Block 5A-W-48D-FIX1 — "#79/XY-P" needs the slash dropped and
        # the hyphen kept. Verified live against PC:
        #   * "M Gengar EX #79/XY-P" -> m-gengar-ex-79xy-p
        url = self.v8.build_url("Pokemon Japanese Promo", "M Gengar EX #79/XY-P")
        self.assertEqual(
            url,
            "https://www.pricecharting.com/game/pokemon-japanese-promo/m-gengar-ex-79xy-p",
        )


class TestFetcherRetry(unittest.TestCase):
    """Block 5A-W-48C-FIX1 — fetch_card_page retries on transient
    failures without changing the URL. Verified against a mocked
    requests.Session so no live PriceCharting traffic is generated by
    the test suite."""

    @classmethod
    def setUpClass(cls):
        cls.v8 = _load("v8", "pokeprices_scraper_v8.py")

    def _mock_session(self, responses):
        """Return a session-shaped mock that yields the given HTTP
        responses in order on successive .get() calls."""
        from unittest.mock import MagicMock
        session = MagicMock()
        it = iter(responses)
        def _get(url, timeout=10):
            r = next(it)
            if isinstance(r, Exception):
                raise r
            m = MagicMock()
            m.status_code = r[0]
            m.text = r[1] if len(r) > 1 else ""
            return m
        session.get.side_effect = _get
        return session

    def test_200_first_try_returns_immediately(self):
        original = self.v8.session
        try:
            self.v8.session = self._mock_session([(200, "OK page")])
            html = self.v8.fetch_card_page("https://example.test/card", retries=2, backoff_seconds=0.001)
            self.assertEqual(html, "OK page")
        finally:
            self.v8.session = original

    def test_404_returns_none_without_retry(self):
        original = self.v8.session
        try:
            # Only one response queued — proves no retry on 404.
            self.v8.session = self._mock_session([(404,)])
            html = self.v8.fetch_card_page("https://example.test/card", retries=3, backoff_seconds=0.001)
            self.assertIsNone(html)
        finally:
            self.v8.session = original

    def test_429_retries_then_succeeds(self):
        original = self.v8.session
        try:
            self.v8.session = self._mock_session([(429,), (429,), (200, "recovered")])
            html = self.v8.fetch_card_page("https://example.test/card", retries=2, backoff_seconds=0.001)
            self.assertEqual(html, "recovered")
        finally:
            self.v8.session = original

    def test_500_retries_then_succeeds(self):
        original = self.v8.session
        try:
            self.v8.session = self._mock_session([(503,), (200, "recovered")])
            html = self.v8.fetch_card_page("https://example.test/card", retries=1, backoff_seconds=0.001)
            self.assertEqual(html, "recovered")
        finally:
            self.v8.session = original

    def test_persistent_5xx_gives_up_after_bounded_retries(self):
        original = self.v8.session
        try:
            self.v8.session = self._mock_session([(500,), (500,), (500,)])
            html = self.v8.fetch_card_page("https://example.test/card", retries=2, backoff_seconds=0.001)
            self.assertIsNone(html)
        finally:
            self.v8.session = original

    def test_timeout_then_recovery(self):
        import requests as _r
        original = self.v8.session
        try:
            self.v8.session = self._mock_session([
                _r.exceptions.Timeout(),
                (200, "recovered"),
            ])
            html = self.v8.fetch_card_page("https://example.test/card", retries=2, backoff_seconds=0.001)
            self.assertEqual(html, "recovered")
        finally:
            self.v8.session = original

    def test_persistent_timeout_gives_up(self):
        import requests as _r
        original = self.v8.session
        try:
            self.v8.session = self._mock_session([
                _r.exceptions.Timeout(),
                _r.exceptions.Timeout(),
                _r.exceptions.Timeout(),
            ])
            html = self.v8.fetch_card_page("https://example.test/card", retries=2, backoff_seconds=0.001)
            self.assertIsNone(html)
        finally:
            self.v8.session = original


class TestSeederLanguageCollisionGuard(unittest.TestCase):
    """Block 5A-W-48D-FIX1 — the language-collision guard prevents the
    seeder from silently reclassifying an existing card from one
    language to another via a card_slug (PC ID) match. Any collision
    aborts the batch unless the specific PC ID has been named in
    --allow-language-reclassification.

    These are the exact tests that would have caught the two W48D en->jp
    flips (pc-8330138 and pc-8076785) at seed time."""

    @classmethod
    def setUpClass(cls):
        cls.seeder = _load("seeder", "seed_set_cards.py")

    def _card(self, slug, name="Any Card #1", set_name="Any Set"):
        return {"card_slug": slug, "card_name": name, "set_name": set_name}

    def _existing(self, slug, language, set_name="Prior Set", name="Prior Card"):
        return {slug: {"card_slug": slug, "card_name": name,
                       "set_name": set_name, "language": language}}

    def test_en_to_jp_collision_is_blocked(self):
        csv_cards = [self._card("1001", set_name="Japanese Promo")]
        existing = self._existing("1001", "en", set_name="Promo")
        safe, allowed, blocked = self.seeder.classify_language_collisions(
            csv_cards, existing, target_language="jp", allow_reclass=[])
        self.assertEqual(safe, [])
        self.assertEqual(allowed, [])
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0]["card_slug"], "1001")
        self.assertEqual(blocked[0]["_existing"]["language"], "en")

    def test_jp_to_en_collision_is_blocked(self):
        csv_cards = [self._card("2002", set_name="English Reprint")]
        existing = self._existing("2002", "jp", set_name="Japanese Promo")
        safe, allowed, blocked = self.seeder.classify_language_collisions(
            csv_cards, existing, target_language="en", allow_reclass=[])
        self.assertEqual(safe, [])
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0]["_existing"]["language"], "jp")

    def test_same_language_is_never_a_collision(self):
        csv_cards = [self._card("3003", set_name="Battle Partners")]
        existing = self._existing("3003", "jp", set_name="Battle Partners")
        safe, allowed, blocked = self.seeder.classify_language_collisions(
            csv_cards, existing, target_language="jp", allow_reclass=[])
        self.assertEqual(len(safe), 1)
        self.assertEqual(safe[0]["card_slug"], "3003")
        self.assertEqual(blocked, [])
        self.assertEqual(allowed, [])

    def test_no_existing_row_is_never_a_collision(self):
        csv_cards = [self._card("4004")]
        safe, allowed, blocked = self.seeder.classify_language_collisions(
            csv_cards, {}, target_language="jp", allow_reclass=[])
        self.assertEqual(len(safe), 1)
        self.assertEqual(blocked, [])
        self.assertEqual(allowed, [])

    def test_only_named_pc_id_is_reclassified(self):
        # Two collisions; only ONE is named in the allow-list. The other
        # must still be blocked.
        csv_cards = [self._card("5005"), self._card("5006")]
        existing = {
            "5005": {"card_slug": "5005", "card_name": "A #1", "set_name": "Old", "language": "en"},
            "5006": {"card_slug": "5006", "card_name": "B #1", "set_name": "Old", "language": "en"},
        }
        safe, allowed, blocked = self.seeder.classify_language_collisions(
            csv_cards, existing, target_language="jp", allow_reclass=["5005"])
        self.assertEqual(safe, [])
        self.assertEqual(len(allowed), 1)
        self.assertEqual(allowed[0]["card_slug"], "5005")
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0]["card_slug"], "5006")

    def test_broad_allow_does_not_bypass_unrelated_ids(self):
        # Naming an unrelated PC ID in the allow-list must NOT let a
        # different colliding PC ID slip through.
        csv_cards = [self._card("7777")]
        existing = self._existing("7777", "en")
        safe, allowed, blocked = self.seeder.classify_language_collisions(
            csv_cards, existing, target_language="jp",
            allow_reclass=["9999", "8888"])
        self.assertEqual(safe, [])
        self.assertEqual(allowed, [])
        self.assertEqual(len(blocked), 1)

    def test_report_contains_both_existing_and_proposed_identities(self):
        csv_cards = [self._card("6006", name="Raifort #117/SV-P", set_name="Japanese Promo")]
        existing = {
            "6006": {"card_slug": "6006", "card_name": "OldName #117",
                     "set_name": "Legacy Promos", "language": "en"},
        }
        _, _, blocked = self.seeder.classify_language_collisions(
            csv_cards, existing, target_language="jp", allow_reclass=[])
        report = self.seeder.format_collision_report(blocked, [], target_language="jp")
        # Both sides of the collision must appear in the report.
        self.assertIn("pc-6006", report)
        self.assertIn("'en'", report)
        self.assertIn("'jp'", report)
        self.assertIn("Legacy Promos", report)
        self.assertIn("Japanese Promo", report)
        self.assertIn("Raifort #117/SV-P", report)
        self.assertIn("OldName #117", report)
        # And the report must instruct HOW to authorise the override.
        self.assertIn("--allow-language-reclassification", report)
        self.assertIn("6006", report)

    def test_allowed_reclassification_reported_distinctly(self):
        # When an override is honoured, the report must clearly say so.
        csv_cards = [self._card("8080", name="X #1", set_name="Japanese Wild Blaze")]
        existing = {"8080": {"card_slug": "8080", "card_name": "X #1",
                             "set_name": "Wild Blaze EN", "language": "en"}}
        _, allowed, _ = self.seeder.classify_language_collisions(
            csv_cards, existing, target_language="jp", allow_reclass=["8080"])
        report = self.seeder.format_collision_report([], allowed, target_language="jp")
        self.assertIn("Explicit reclassification approved", report)
        self.assertIn("pc-8080", report)
        self.assertIn("'en'->'jp'", report)


class TestSeederSiteSlug(unittest.TestCase):
    """seed_set_cards.build_card_url_slug — website /card/<slug> route.

    The invariants here are opposite to the scraper: apostrophes MUST be
    stripped from the site's own URL slugs because the /set/.../card/...
    routes are consumed by Next.js and must round-trip through
    encodeURIComponent cleanly. Apostrophes in URLs also complicate
    sharing links.
    """

    @classmethod
    def setUpClass(cls):
        cls.seeder = _load("seeder", "seed_set_cards.py")

    def test_apostrophe_stripped_in_site_slug(self):
        slug = self.seeder.build_card_url_slug("Hop's Bag #91")
        self.assertEqual(slug, "hops-bag-91")
        self.assertNotIn("'", slug)

        slug = self.seeder.build_card_url_slug("Lillie's Clefairy ex #126")
        self.assertEqual(slug, "lillies-clefairy-ex-126")
        self.assertNotIn("'", slug)

        slug = self.seeder.build_card_url_slug("N's Zoroark ex #131")
        self.assertEqual(slug, "ns-zoroark-ex-131")
        self.assertNotIn("'", slug)

    def test_ordinary_card_slug_unchanged(self):
        slug = self.seeder.build_card_url_slug("Pikachu #58")
        self.assertEqual(slug, "pikachu-58")

    def test_bracket_variant_stripped(self):
        slug = self.seeder.build_card_url_slug("Charizard [1st Edition] #4")
        self.assertEqual(slug, "charizard-1st-edition-4")

    def test_multiple_apostrophes(self):
        # Belt and braces: two apostrophes both stripped.
        slug = self.seeder.build_card_url_slug("Iono's Bellibolt's Twin")
        self.assertEqual(slug, "ionos-bellibolts-twin")
        self.assertNotIn("'", slug)


if __name__ == "__main__":
    unittest.main(verbosity=2)
