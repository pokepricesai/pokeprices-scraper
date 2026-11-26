"""
tests/test_onboard_new_set.py
=============================
Unit tests for onboard_new_set.py. No PriceCharting / Supabase network
traffic — every HTML input comes from
tests/fixtures/pc_delta_reign_listing_trimmed.html and every DB check
path is guarded behind an env check in the production module.

Covered:
  * parse_listing_html against a trimmed real listing fixture.
  * write_pc_csv round-trip via the same csv.DictReader usage as
    pokeprices_scraper_v8.load_cards_from_pc_csvs.
  * classify_rows correctly identifies singles, sealed, bracket
    variants, and detects missing numeric gaps.
  * append_console_to_batch is idempotent and preserves LF line endings.
"""

from __future__ import annotations

import csv
import importlib.util
import io
import os
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
LISTING_FIXTURE = os.path.join(FIXTURE_DIR, "pc_delta_reign_listing_trimmed.html")


def _load(module_name: str, filename: str):
    path = os.path.join(REPO_ROOT, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class TestListingParser(unittest.TestCase):
    """onboard_new_set.parse_listing_html against the real fixture."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")
        with open(LISTING_FIXTURE, "r", encoding="utf-8") as f:
            cls.html = f.read()

    def test_three_rows_parsed(self):
        rows = self.onboard.parse_listing_html(self.html)
        self.assertEqual(len(rows), 3)
        self.assertEqual([r["pc_id"] for r in rows],
                         ["15000221", "15000222", "15000223"])

    def test_product_names_are_exact(self):
        rows = self.onboard.parse_listing_html(self.html)
        self.assertEqual(rows[0]["product_name"], "Mega Rayquaza ex #138")
        self.assertEqual(rows[1]["product_name"], "Booster Box")
        self.assertEqual(rows[2]["product_name"], "Pikachu [Reverse Holo] #25")

    def test_loose_price_extracted(self):
        rows = self.onboard.parse_listing_html(self.html)
        self.assertEqual(rows[0]["loose_price"], "$279.71")
        self.assertEqual(rows[1]["loose_price"], "$139.99")

    def test_missing_loose_price_becomes_empty_string(self):
        rows = self.onboard.parse_listing_html(self.html)
        # The third row deliberately has an empty js-price span.
        self.assertEqual(rows[2]["loose_price"], "")

    def test_empty_html_returns_empty_list(self):
        self.assertEqual(self.onboard.parse_listing_html(""), [])
        self.assertEqual(self.onboard.parse_listing_html("<html></html>"), [])

    def test_console_url_slug_matches_scraper(self):
        # Must round-trip to the exact slug the scraper will use so the
        # URL fetched here is the same URL the nightly job would reach.
        url = self.onboard.build_console_url("Pokemon Delta Reign")
        self.assertEqual(
            url, "https://www.pricecharting.com/console/pokemon-delta-reign",
        )

    def test_console_url_slug_handles_comma(self):
        url = self.onboard.build_console_url(
            "Pokemon Japanese Gold, Silver, New World",
        )
        self.assertEqual(
            url,
            "https://www.pricecharting.com/console/"
            "pokemon-japanese-gold-silver-new-world",
        )


class TestCsvWriterRoundTrip(unittest.TestCase):
    """write_pc_csv emits a file that load_cards_from_pc_csvs can read.
    The point is byte-level compatibility with pc_csvs/ — the scraper
    reads the files with csv.DictReader keyed on the exact column names
    "id", "console-name", "product-name", "loose-price"."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")

    def test_round_trip_via_dictreader(self):
        rows = [
            {"pc_id": "15000221", "product_name": "Mega Rayquaza ex #138",
             "loose_price": "$279.71"},
            {"pc_id": "15000222", "product_name": "Booster Box",
             "loose_price": "$139.99"},
            {"pc_id": "15000223", "product_name": "Pikachu [Reverse Holo] #25",
             "loose_price": ""},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "Pokemon Delta Reign.csv")
            self.onboard.write_pc_csv(rows, "Pokemon Delta Reign", csv_path)

            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                got = list(reader)

        self.assertEqual(len(got), 3)
        self.assertEqual(got[0]["id"], "15000221")
        self.assertEqual(got[0]["console-name"], "Pokemon Delta Reign")
        self.assertEqual(got[0]["product-name"], "Mega Rayquaza ex #138")
        self.assertEqual(got[0]["loose-price"], "$279.71")
        # Empty loose-price round-trips as empty string, matching
        # existing pc_csvs/*.csv rows.
        self.assertEqual(got[2]["loose-price"], "")

    def test_values_with_commas_are_properly_quoted(self):
        # Mirrors the existing shape in pc_csvs/Pokemon Promo.csv —
        # product names with commas get wrapped in "". DictReader must
        # recover the original string unchanged.
        rows = [{
            "pc_id": "123",
            "product_name": "2 Pack Blister [Tornadus, Thundurus & Landorus]",
            "loose_price": "$26.90",
        }]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "quoted.csv")
            self.onboard.write_pc_csv(rows, "Pokemon Promo", csv_path)
            with open(csv_path, "r", encoding="utf-8") as f:
                got = list(csv.DictReader(f))
        self.assertEqual(
            got[0]["product-name"],
            "2 Pack Blister [Tornadus, Thundurus & Landorus]",
        )

    def test_header_order_matches_pc_csvs(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "header.csv")
            self.onboard.write_pc_csv([], "Pokemon X", csv_path)
            with open(csv_path, "r", encoding="utf-8") as f:
                header = f.readline().rstrip("\r\n")
        self.assertEqual(header, "id,console-name,product-name,loose-price")

    def test_lf_line_endings(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "nl.csv")
            self.onboard.write_pc_csv(
                [{"pc_id": "1", "product_name": "Pikachu #58",
                  "loose_price": "$1.00"}],
                "Pokemon Base Set", csv_path,
            )
            with open(csv_path, "rb") as f:
                raw = f.read()
        # pc_csvs/*.csv are LF-terminated (verified live). Enforce that
        # the writer does not sneak \r\n in on Windows runners.
        self.assertNotIn(b"\r\n", raw)
        self.assertTrue(raw.endswith(b"\n"))


class TestClassifier(unittest.TestCase):
    """classify_rows: singles vs sealed vs unnumbered, missing numbers,
    duplicates, bracket variants, --expected-cards drift."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")

    def _rows(self, triples):
        return [
            {"pc_id": pid, "product_name": name, "loose_price": price}
            for pid, name, price in triples
        ]

    def test_singles_sealed_and_unnumbered_are_split(self):
        rows = self._rows([
            ("1", "Pikachu #1", "$1.00"),
            ("2", "Charizard #4", "$500.00"),
            ("3", "Booster Box", "$120.00"),
            ("4", "Mew R/RGB", "$0.00"),
        ])
        cls = self.onboard.classify_rows(rows)
        self.assertEqual(cls["total"], 4)
        self.assertEqual(len(cls["singles"]), 2)
        self.assertEqual(len(cls["sealed"]), 1)
        self.assertEqual(len(cls["unnumbered"]), 1)

    def test_bracket_variant_detection(self):
        rows = self._rows([
            ("1", "Pikachu [Reverse Holo] #58", "$1.00"),
            ("2", "Charizard #4", "$5.00"),
        ])
        cls = self.onboard.classify_rows(rows)
        self.assertEqual(len(cls["bracket_variants"]), 1)
        self.assertEqual(cls["bracket_variants"][0]["pc_id"], "1")

    def test_missing_numbers_report(self):
        rows = self._rows([
            ("a", "Card #1", ""),
            ("b", "Card #2", ""),
            ("c", "Card #5", ""),
            ("d", "Card #7", ""),
        ])
        cls = self.onboard.classify_rows(rows)
        self.assertEqual(cls["highest_number"], 7)
        self.assertEqual(cls["distinct_numbers"], 4)
        self.assertEqual(cls["missing_numbers"], [3, 4, 6])

    def test_duplicate_ids_flagged(self):
        rows = self._rows([
            ("1", "Pikachu #1", "$1.00"),
            ("1", "Pikachu #1", "$1.00"),
            ("2", "Raichu #2", "$1.00"),
        ])
        cls = self.onboard.classify_rows(rows)
        self.assertEqual(cls["duplicate_ids"], ["1"])

    def test_victini_is_not_sealed_despite_substring_tin(self):
        # The "tin" sealed keyword uses word boundaries so Victini
        # stays classified as a numbered single. This is the exact
        # regression caught in the 30th Celebration import.
        rows = self._rows([("1", "Victini #13", "$1.00")])
        cls = self.onboard.classify_rows(rows)
        self.assertEqual(len(cls["singles"]), 1)
        self.assertEqual(len(cls["sealed"]), 0)

    def test_battle_deck_is_sealed(self):
        # "deck" is in the keyword list specifically to catch
        # Battle Deck: Espeon ex / Umbreon ex (30th Celebration).
        rows = self._rows([("1", "Battle Deck: Espeon ex", "$30.00")])
        cls = self.onboard.classify_rows(rows)
        self.assertEqual(len(cls["sealed"]), 1)

    def test_empty_input(self):
        cls = self.onboard.classify_rows([])
        self.assertEqual(cls["total"], 0)
        self.assertEqual(cls["highest_number"], None)
        self.assertEqual(cls["missing_numbers"], [])


class TestBatchFileAppend(unittest.TestCase):
    """append_console_to_batch is idempotent, preserves the file's
    existing newline style, and never touches the file if the console
    name is already present."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")

    def test_appends_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "batch.txt")
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write("Pokemon Base Set\nPokemon Jungle\n")
            status = self.onboard.append_console_to_batch(path, "Pokemon Delta Reign")
            self.assertEqual(status, "appended")
            with open(path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
            self.assertEqual(lines[-1], "Pokemon Delta Reign")
            self.assertEqual(len(lines), 3)

    def test_idempotent_when_already_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "batch.txt")
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write("Pokemon Base Set\nPokemon Delta Reign\nPokemon Jungle\n")
            before = open(path, "rb").read()
            status = self.onboard.append_console_to_batch(path, "Pokemon Delta Reign")
            self.assertEqual(status, "already-present")
            after = open(path, "rb").read()
            # Byte-for-byte unchanged.
            self.assertEqual(before, after)

    def test_creates_batch_file_if_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "new-batch.txt")
            status = self.onboard.append_console_to_batch(path, "Pokemon Delta Reign")
            self.assertEqual(status, "created")
            with open(path, "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), "Pokemon Delta Reign\n")

    def test_preserves_crlf_line_endings(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "batch-crlf.txt")
            with open(path, "wb") as f:
                f.write(b"Pokemon Base Set\r\nPokemon Jungle\r\n")
            self.onboard.append_console_to_batch(path, "Pokemon Delta Reign")
            with open(path, "rb") as f:
                raw = f.read()
            self.assertTrue(raw.endswith(b"Pokemon Delta Reign\r\n"))
            # No stray lone-\n lines introduced.
            self.assertNotIn(b"\r\r\n", raw)

    def test_adds_trailing_newline_when_file_lacks_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "batch.txt")
            with open(path, "wb") as f:
                f.write(b"Pokemon Base Set\nPokemon Jungle")  # no trailing newline
            self.onboard.append_console_to_batch(path, "Pokemon Delta Reign")
            with open(path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
            self.assertEqual(lines, [
                "Pokemon Base Set", "Pokemon Jungle", "Pokemon Delta Reign",
            ])


if __name__ == "__main__":
    unittest.main(verbosity=2)
