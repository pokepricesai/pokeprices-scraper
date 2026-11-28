"""
tests/test_detect_new_sets.py
=============================
Unit tests for detect_new_sets.py. No network and no git: the category
HTML is read from a trimmed fixture and the batched-sets map is
constructed in-process.

Covered:
  * parse_category_html prefers title= over short inner text, dedupes
    by URL, and decodes HTML entities (apostrophe in "McDonald's").
  * compare() routes consoles into English / Japanese buckets by name
    prefix, skips Chinese sets, and reports already-batched sets as
    not-missing (case-insensitive, whitespace-collapsed match).
  * compare() flags near-miss renames (same build_url slug, different
    display spelling) separately from genuinely new sets.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
CATEGORY_FIXTURE = os.path.join(FIXTURE_DIR, "pc_category_pokemon_trimmed.html")


def _load(module_name: str, filename: str):
    path = os.path.join(REPO_ROOT, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class TestCategoryParser(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.detect = _load("detect_new_sets", "detect_new_sets.py")
        with open(CATEGORY_FIXTURE, "r", encoding="utf-8") as f:
            cls.html = f.read()

    def test_looks_like_category_page_accepts_title(self):
        self.assertTrue(self.detect.looks_like_category_page(self.html))

    def test_looks_like_category_page_rejects_unrelated(self):
        self.assertFalse(
            self.detect.looks_like_category_page(
                "<title>Random Unrelated Page</title>"
            )
        )

    def test_unique_urls_only(self):
        consoles = self.detect.parse_category_html(self.html)
        urls = [c["url"] for c in consoles]
        self.assertEqual(len(urls), len(set(urls)))

    def test_title_attribute_preferred_over_short_inner_text(self):
        consoles = self.detect.parse_category_html(self.html)
        # The newest-releases sidebar labels the McDonalds 2026 anchor
        # with short inner text "McDonalds 2026" and canonical title
        # "Pokemon McDonalds 2026". The parser must pick the title.
        mcdonalds = next(c for c in consoles
                         if c["url"].endswith("/pokemon-mcdonalds-2026"))
        self.assertEqual(mcdonalds["display_name"], "Pokemon McDonalds 2026")

    def test_html_entity_in_inner_text_is_decoded(self):
        consoles = self.detect.parse_category_html(self.html)
        jp_mcd = next(
            c for c in consoles
            if "pokemon-japanese-2002-mcdonald" in c["url"].lower()
        )
        self.assertEqual(jp_mcd["display_name"],
                         "Pokemon Japanese 2002 McDonald's")

    def test_premium_deck_variant_is_a_distinct_console(self):
        consoles = self.detect.parse_category_html(self.html)
        slugs = {c["slug"] for c in consoles}
        self.assertIn("pokemon-japanese-30th-celebration", slugs)
        self.assertIn("pokemon-japanese-30th-celebration-premium-deck", slugs)


class TestCompareDiff(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.detect = _load("detect_new_sets", "detect_new_sets.py")
        with open(CATEGORY_FIXTURE, "r", encoding="utf-8") as f:
            cls.consoles = cls.detect.parse_category_html(f.read())

    def test_tracked_sets_are_not_reported_as_missing(self):
        # "Pokemon Delta Reign" is in the English batch; it must not
        # appear in the missing lists.
        batches = {"batches/batch6.txt": ["Pokemon Delta Reign"]}
        diff = self.detect.compare(self.consoles, batches)
        missing_names = [c["display_name"] for c in diff["english"]]
        self.assertNotIn("Pokemon Delta Reign", missing_names)

    def test_match_is_case_insensitive_and_whitespace_collapsed(self):
        # The batch line has surprising whitespace / casing; the
        # category page lists the canonical form. The two must still
        # be treated as the same console.
        batches = {"batches/batch6.txt": ["  pokemon   DELTA Reign "]}
        diff = self.detect.compare(self.consoles, batches)
        self.assertNotIn("Pokemon Delta Reign",
                         [c["display_name"] for c in diff["english"]])

    def test_japanese_group_only_contains_jp_prefix(self):
        batches = {"batches/batch6.txt": ["Pokemon Base Set", "Pokemon Delta Reign"]}
        diff = self.detect.compare(self.consoles, batches)
        for c in diff["japanese"]:
            self.assertTrue(c["display_name"].lower().startswith("pokemon japanese"))

    def test_chinese_sets_are_not_reported(self):
        batches = {"batches/batch6.txt": []}
        diff = self.detect.compare(self.consoles, batches)
        all_names = {c["display_name"] for c in diff["english"] + diff["japanese"] + diff["near_miss"]}
        self.assertNotIn("Pokemon Chinese Gem Pack 7", all_names)

    def test_missing_japanese_sets_show_up(self):
        batches = {"batches/batch6.txt": [], "batches/batch-japanese-1.txt": []}
        diff = self.detect.compare(self.consoles, batches)
        names = {c["display_name"] for c in diff["japanese"]}
        self.assertIn("Pokemon Japanese Storm Emeralda", names)
        self.assertIn("Pokemon Japanese 30th Celebration", names)
        # The Premium Deck variant lives at a distinct slug; it is also
        # missing from the batch, so it must surface separately.
        self.assertIn("Pokemon Japanese 30th Celebration Premium Deck", names)

    def test_near_miss_renames_are_separated(self):
        # The batch lists the set with an extra comma that
        # pokeprices_scraper_v8.build_url strips from the slug. Both
        # strings therefore resolve to the same PriceCharting console
        # even though the plain-text spellings differ, so compare()
        # must surface this under near_miss rather than treating it
        # as a brand-new English set.
        batches = {
            "batches/batch6.txt": ["Pokemon, Delta Reign"],
        }
        diff = self.detect.compare(self.consoles, batches)
        near_miss_names = [c["display_name"] for c in diff["near_miss"]]
        self.assertIn("Pokemon Delta Reign", near_miss_names)
        # And the English-missing bucket must NOT list it.
        self.assertNotIn("Pokemon Delta Reign",
                         [c["display_name"] for c in diff["english"]])


if __name__ == "__main__":
    unittest.main(verbosity=2)
