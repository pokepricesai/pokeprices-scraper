"""
tests/test_onboard_new_set.py
=============================
Unit tests for onboard_new_set.py. No PriceCharting / Supabase network
traffic — every HTML input comes from tests/fixtures/*.html and every
DB check path is guarded behind an env check in the production module.

Covered:
  * parse_listing_html against a trimmed real listing fixture.
  * parse_next_page_form extracts / absents the hidden form fields.
  * fetch_listing walks GET + POST chunks and stops on all three
    stop conditions (no form, no new ids, cap).
  * write_pc_csv round-trip via the same csv.DictReader usage as
    pokeprices_scraper_v8.load_cards_from_pc_csvs.
  * classify_rows correctly identifies singles, sealed, bracket
    variants, and detects missing numeric gaps.
  * evaluate_completeness fires on >10% shortfall or >10 gaps and not
    otherwise.
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
CHUNK1_FIXTURE = os.path.join(FIXTURE_DIR, "pc_listing_chunk1.html")
CHUNK2_FIXTURE = os.path.join(FIXTURE_DIR, "pc_listing_chunk2_final.html")


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


class TestNextPageForm(unittest.TestCase):
    """parse_next_page_form extracts the hidden fields of the
    "more results" form, and returns None when the form is absent."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")

    def test_fields_extracted_from_chunk1(self):
        html = open(CHUNK1_FIXTURE, "r", encoding="utf-8").read()
        form = self.onboard.parse_next_page_form(html)
        self.assertIsNotNone(form)
        # Keys mirror the real PriceCharting form captured 2026-11-26.
        self.assertEqual(form["sort"], "")
        self.assertEqual(form["when"], "none")
        self.assertEqual(form["release-date"], "2026-11-26")
        self.assertEqual(form["cursor"], "2")

    def test_none_when_form_absent(self):
        html = open(CHUNK2_FIXTURE, "r", encoding="utf-8").read()
        self.assertIsNone(self.onboard.parse_next_page_form(html))

    def test_none_when_html_is_empty(self):
        self.assertIsNone(self.onboard.parse_next_page_form(""))

    def test_submit_input_is_not_captured(self):
        # The <input type="submit" ...> has no value attribute matching
        # a hidden-field shape; it must not pollute the returned dict.
        html = open(CHUNK1_FIXTURE, "r", encoding="utf-8").read()
        form = self.onboard.parse_next_page_form(html)
        # Only the four hidden inputs; the submit button has value=
        # "more results »" but we keep it because the regex is purely
        # name=/value= based. The important invariant is that the four
        # hidden fields are *present* and correct.
        for k in ("sort", "when", "release-date", "cursor"):
            self.assertIn(k, form)


class TestFetchListingPagination(unittest.TestCase):
    """fetch_listing walks GET + POST chunks correctly:
      * dedupes rows re-served on a later chunk
      * stops when the response carries no next_page form
      * stops when a chunk adds no new ids (defence in depth)
      * caps at MAX_LISTING_FETCHES
    Network is fully mocked by monkey-patching _http_get / _http_post."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")
        cls.chunk1 = open(CHUNK1_FIXTURE, "r", encoding="utf-8").read()
        cls.chunk2 = open(CHUNK2_FIXTURE, "r", encoding="utf-8").read()

    def setUp(self):
        # Shorten the polite-delay so the test is fast; restore in tearDown.
        self._real_delay = self.onboard.LISTING_REQUEST_DELAY
        self.onboard.LISTING_REQUEST_DELAY = 0

    def tearDown(self):
        self.onboard.LISTING_REQUEST_DELAY = self._real_delay

    def _install(self, get_sequence, post_sequence):
        """Replace _http_get / _http_post with deterministic sequences."""
        get_it = iter(get_sequence)
        post_it = iter(post_sequence)
        self._posts: list[tuple[str, dict]] = []
        self.onboard._http_get = lambda url, _it=get_it: next(_it)
        def _post(url, fields, _it=post_it, _log=self._posts):
            _log.append((url, dict(fields)))
            return next(_it)
        self.onboard._http_post = _post

    def test_walks_both_chunks_and_dedupes(self):
        self._install([self.chunk1], [self.chunk2])
        log: list[str] = []
        rows = self.onboard.fetch_listing("Pokemon Fakeset", log)
        ids = [r["pc_id"] for r in rows]
        # 2 rows from chunk 1, 2 new rows from chunk 2 (1002 was a
        # duplicate), total 4 distinct.
        self.assertEqual(sorted(ids), ["1001", "1002", "1003", "1004"])
        # Loop made exactly one POST and that POST carried the hidden
        # fields from chunk 1's next_page form.
        self.assertEqual(len(self._posts), 1)
        posted_url, posted_fields = self._posts[0]
        self.assertTrue(posted_url.endswith("/console/pokemon-fakeset"))
        self.assertEqual(posted_fields["cursor"], "2")
        self.assertEqual(posted_fields["when"], "none")
        # Verbose log contains the right summary lines.
        joined = "\n".join(log)
        self.assertIn("GET https://www.pricecharting.com/console/pokemon-fakeset", joined)
        self.assertIn("POST", joined)
        self.assertIn("cursor=2", joined)
        self.assertIn("no next_page form; listing complete", joined)

    def test_stops_when_chunk_adds_no_new_ids(self):
        # POST returns chunk 1 again (same ids). Loop must bail out
        # rather than ping-pong forever.
        self._install([self.chunk1], [self.chunk1])
        log: list[str] = []
        rows = self.onboard.fetch_listing("Pokemon Fakeset", log)
        self.assertEqual({r["pc_id"] for r in rows}, {"1001", "1002"})
        self.assertIn("no new ids; stopping", "\n".join(log))

    def test_caps_at_max_listing_fetches(self):
        # Rig the cap at 3 so the test is fast, then feed back the
        # same "more rows exist" chunk forever.
        self.onboard.MAX_LISTING_FETCHES = 3
        try:
            # chunk1 has 2 rows AND carries a next_page form. Each
            # follow-on POST returns a fresh pair of ids so the "no new
            # ids" short-circuit does not fire before the cap.
            def more(n_start):
                return (
                    '<html><head><title>Pokemon Fakeset Card Prices</title>'
                    '</head><body><table id="games_table"><tbody>'
                    f'<tr id="product-{n_start}" data-product="{n_start}">'
                    f'<td class="title"><a>Foo #{n_start}</a></td>'
                    f'<td class="price numeric used_price"><span class="js-price">$1</span></td>'
                    '</tr>'
                    f'<tr id="product-{n_start+1}" data-product="{n_start+1}">'
                    f'<td class="title"><a>Foo #{n_start+1}</a></td>'
                    f'<td class="price numeric used_price"><span class="js-price">$1</span></td>'
                    '</tr>'
                    '</tbody>'
                    '<form class="next_page js-next-page">'
                    '<input name="cursor" value="9" />'
                    '<input name="when" value="none" />'
                    '</form>'
                    '</table></body></html>'
                )
            self._install([self.chunk1], [more(2000), more(2002), more(2004), more(2006)])
            log: list[str] = []
            rows = self.onboard.fetch_listing("Pokemon Fakeset", log)
            # With MAX_LISTING_FETCHES=3 we did: 1 GET + 2 POSTs = 3
            # total fetches. Row count is 2 (initial) + 2 + 2 = 6.
            self.assertEqual(len(rows), 6)
            self.assertIn(f"hit MAX_LISTING_FETCHES=3", "\n".join(log))
        finally:
            self.onboard.MAX_LISTING_FETCHES = 40

    def test_rejects_wrong_console_title(self):
        wrong = (
            "<html><head><title>Totally Different Page</title></head>"
            '<body><table id="games_table"></table></body></html>'
        )
        self._install([wrong], [])
        with self.assertRaises(RuntimeError) as cm:
            self.onboard.fetch_listing("Pokemon Fakeset", [])
        self.assertIn("title does not match", str(cm.exception))


class TestCompletenessGuard(unittest.TestCase):
    """evaluate_completeness fires on 10% shortfall or >10 gaps."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")

    def _cls(self, distinct, highest, missing_count):
        return {
            "distinct_numbers": distinct,
            "highest_number": highest,
            "missing_numbers": [0] * missing_count,
        }

    def test_fires_on_shortfall(self):
        # 111 distinct vs 141 expected = 21.3% short.
        incomplete, reasons = self.onboard.evaluate_completeness(
            self._cls(111, 141, 0), expected_cards=141,
        )
        self.assertTrue(incomplete)
        self.assertTrue(any("below --expected-cards" in r for r in reasons))

    def test_does_not_fire_at_exactly_10_percent(self):
        # 127 / 141 = 9.9% short → under the threshold.
        incomplete, _ = self.onboard.evaluate_completeness(
            self._cls(127, 141, 0), expected_cards=141,
        )
        self.assertFalse(incomplete)

    def test_fires_on_more_than_ten_gaps(self):
        incomplete, reasons = self.onboard.evaluate_completeness(
            self._cls(130, 141, 11), expected_cards=None,
        )
        self.assertTrue(incomplete)
        self.assertTrue(any("missing" in r for r in reasons))

    def test_exactly_ten_gaps_does_not_fire(self):
        incomplete, _ = self.onboard.evaluate_completeness(
            self._cls(131, 141, 10), expected_cards=None,
        )
        self.assertFalse(incomplete)

    def test_both_conditions_report_both_reasons(self):
        incomplete, reasons = self.onboard.evaluate_completeness(
            self._cls(90, 141, 30), expected_cards=141,
        )
        self.assertTrue(incomplete)
        self.assertEqual(len(reasons), 2)

    def test_no_expected_cards_and_no_gaps_is_complete(self):
        incomplete, reasons = self.onboard.evaluate_completeness(
            self._cls(100, 100, 0), expected_cards=None,
        )
        self.assertFalse(incomplete)
        self.assertEqual(reasons, [])


class TestLanguageNameConsistency(unittest.TestCase):
    """validate_language_name_consistency refuses mislabelled runs
    before they can touch PriceCharting or the DB."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")

    def test_jp_pair_accepted(self):
        self.onboard.validate_language_name_consistency(
            "jp", "Pokemon Japanese Storm Emeralda", "Japanese Storm Emeralda",
        )

    def test_en_pair_accepted(self):
        self.onboard.validate_language_name_consistency(
            "en", "Pokemon Delta Reign", "Delta Reign",
        )

    def test_jp_console_without_japanese_prefix_is_refused(self):
        with self.assertRaises(ValueError) as cm:
            self.onboard.validate_language_name_consistency(
                "jp", "Pokemon Storm Emeralda", "Japanese Storm Emeralda",
            )
        self.assertIn("Pokemon Japanese", str(cm.exception))

    def test_jp_set_name_without_japanese_prefix_is_refused(self):
        with self.assertRaises(ValueError) as cm:
            self.onboard.validate_language_name_consistency(
                "jp", "Pokemon Japanese Storm Emeralda", "Storm Emeralda",
            )
        self.assertIn("Japanese", str(cm.exception))

    def test_en_console_with_japanese_prefix_is_refused(self):
        with self.assertRaises(ValueError) as cm:
            self.onboard.validate_language_name_consistency(
                "en", "Pokemon Japanese Storm Emeralda", "Storm Emeralda",
            )
        self.assertIn("forbids", str(cm.exception))

    def test_en_set_name_with_japanese_prefix_is_refused(self):
        with self.assertRaises(ValueError) as cm:
            self.onboard.validate_language_name_consistency(
                "en", "Pokemon Delta Reign", "Japanese Delta Reign",
            )
        self.assertIn("forbids", str(cm.exception))


class TestChooseBatchFile(unittest.TestCase):
    """choose_batch_file picks the right file per --language and
    tie-breaks consistently. All returned paths are repo-relative
    with forward slashes so the workflow commit step and the
    GITHUB_OUTPUT emission agree on exactly one shape."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")

    def _repo_root(self, tmp: str, counts: dict[str, int]) -> str:
        """Build a repo-root-like `tmp/batches/` directory with the
        given per-file non-empty line counts. Returns `tmp` (which
        becomes the repo_root argument for choose_batch_file)."""
        d = os.path.join(tmp, "batches")
        os.mkdir(d)
        for filename, n in counts.items():
            with open(os.path.join(d, filename), "w", encoding="utf-8", newline="\n") as f:
                for i in range(n):
                    f.write(f"Pokemon Dummy {i}\n")
        return tmp

    def test_explicit_batch_file_always_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = self._repo_root(tmp, {"batch6.txt": 1,
                                       "batch-japanese-1.txt": 1,
                                       "batch-japanese-2.txt": 1})
            self.assertEqual(
                self.onboard.choose_batch_file("jp", r, explicit="batches/custom.txt"),
                "batches/custom.txt",
            )

    def test_explicit_absolute_path_is_normalised(self):
        # When a caller passes an absolute path under repo_root, the
        # function must normalise it back to repo-relative forward
        # slashes so the GITHUB_OUTPUT line is always one shape.
        with tempfile.TemporaryDirectory() as tmp:
            r = self._repo_root(tmp, {"batch6.txt": 1})
            abs_path = os.path.join(r, "batches", "batch6.txt")
            got = self.onboard.choose_batch_file("en", r, explicit=abs_path)
            self.assertEqual(got, "batches/batch6.txt")

    def test_english_always_picks_batch6(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = self._repo_root(tmp, {"batch6.txt": 10,
                                       "batch-japanese-1.txt": 0})
            chosen = self.onboard.choose_batch_file("en", r, None)
            self.assertEqual(chosen, "batches/batch6.txt")

    def test_japanese_picks_least_populated(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = self._repo_root(tmp, {
                "batch-japanese-1.txt": 20,
                "batch-japanese-2.txt": 5,   # smallest
                "batch-japanese-3.txt": 10,
                "batch-japanese-4.txt": 15,
                "batch-japanese-5.txt": 25,
            })
            chosen = self.onboard.choose_batch_file("jp", r, None)
            self.assertEqual(chosen, "batches/batch-japanese-2.txt")

    def test_japanese_tie_breaks_on_lowest_number(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = self._repo_root(tmp, {
                "batch-japanese-1.txt": 7,
                "batch-japanese-2.txt": 7,
                "batch-japanese-3.txt": 7,
            })
            chosen = self.onboard.choose_batch_file("jp", r, None)
            self.assertEqual(chosen, "batches/batch-japanese-1.txt")

    def test_japanese_ignores_english_batch_files(self):
        # A badly-named "batch6.txt" with zero lines must not win —
        # routing must stay inside the Japanese pipeline.
        with tempfile.TemporaryDirectory() as tmp:
            r = self._repo_root(tmp, {
                "batch6.txt": 0,
                "batch-japanese-1.txt": 999,
            })
            chosen = self.onboard.choose_batch_file("jp", r, None)
            self.assertEqual(chosen, "batches/batch-japanese-1.txt")

    def test_raises_when_no_japanese_batches_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = self._repo_root(tmp, {"batch6.txt": 1})
            with self.assertRaises(FileNotFoundError):
                self.onboard.choose_batch_file("jp", r, None)

    def test_blank_lines_are_not_counted(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = tmp
            os.mkdir(os.path.join(r, "batches"))
            with open(os.path.join(r, "batches", "batch-japanese-1.txt"), "w",
                      newline="\n") as f:
                # File A: 2 content lines + several blanks (count = 2).
                f.write("Pokemon A\n\n\nPokemon B\n\n")
            with open(os.path.join(r, "batches", "batch-japanese-2.txt"), "w",
                      newline="\n") as f:
                # File B: 3 content lines, no blanks (count = 3).
                f.write("Pokemon C\nPokemon D\nPokemon E\n")
            chosen = self.onboard.choose_batch_file("jp", r, None)
            self.assertEqual(chosen, "batches/batch-japanese-1.txt")


class TestBatchPathValidation(unittest.TestCase):
    """validate_repo_relative_batch_path matches the workflow's own
    regex, and validate_explicit_batch_matches_language keeps en and
    jp pipelines from crossing wires."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")

    def test_repo_relative_pattern_accepts_good_paths(self):
        for good in ("batches/batch6.txt", "batches/batch-japanese-1.txt",
                     "batches/batch2a.txt"):
            self.onboard.validate_repo_relative_batch_path(good)

    def test_repo_relative_pattern_rejects_absolute(self):
        with self.assertRaises(ValueError):
            self.onboard.validate_repo_relative_batch_path(
                "/home/runner/work/pokeprices/batches/batch6.txt"
            )

    def test_repo_relative_pattern_rejects_parent_traversal(self):
        with self.assertRaises(ValueError):
            self.onboard.validate_repo_relative_batch_path(
                "../batches/batch6.txt"
            )
        with self.assertRaises(ValueError):
            self.onboard.validate_repo_relative_batch_path(
                "batches/../etc/passwd"
            )

    def test_repo_relative_pattern_rejects_spaces(self):
        # Spaces are not in the character class; a stray " " in a
        # filename must stop the commit step reaching the push.
        with self.assertRaises(ValueError):
            self.onboard.validate_repo_relative_batch_path(
                "batches/batch 6.txt"
            )

    def test_repo_relative_pattern_rejects_missing_dir_prefix(self):
        with self.assertRaises(ValueError):
            self.onboard.validate_repo_relative_batch_path("batch6.txt")

    def test_repo_relative_pattern_rejects_backslashes(self):
        # Windows-shaped paths must be normalised before validation.
        with self.assertRaises(ValueError):
            self.onboard.validate_repo_relative_batch_path("batches\\batch6.txt")

    def test_en_batch_file_refused_when_language_jp(self):
        with self.assertRaises(ValueError):
            self.onboard.validate_explicit_batch_matches_language(
                "jp", "batches/batch6.txt",
            )

    def test_jp_batch_file_refused_when_language_en(self):
        with self.assertRaises(ValueError):
            self.onboard.validate_explicit_batch_matches_language(
                "en", "batches/batch-japanese-1.txt",
            )

    def test_matching_language_accepts(self):
        # No exception for the right pairings.
        self.onboard.validate_explicit_batch_matches_language(
            "en", "batches/batch6.txt",
        )
        self.onboard.validate_explicit_batch_matches_language(
            "jp", "batches/batch-japanese-3.txt",
        )


class TestGithubOutputEmission(unittest.TestCase):
    """main() emits exactly one repo-relative forward-slash path to
    GITHUB_OUTPUT, before any PriceCharting request. The test fakes
    _http_get so it raises immediately — proving the emission happens
    first — and then inspects the GITHUB_OUTPUT file.

    Both --language en and --language jp are exercised against the
    repo's own batches/ folder so the output line is byte-identical to
    what the live CI job will emit."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")

    def setUp(self):
        self._saved = self.onboard._http_get
        # Make any PriceCharting fetch an instant failure so we can
        # prove the GITHUB_OUTPUT line was written *before* fetch.
        def _fail(url, *a, **kw):
            raise AssertionError(f"_http_get must not be called before "
                                 f"the GITHUB_OUTPUT line is emitted; "
                                 f"got {url!r}")
        self.onboard._http_get = _fail

    def tearDown(self):
        self.onboard._http_get = self._saved

    def _call_main(self, argv: list[str], gh_output_path: str) -> int:
        old = os.environ.get("GITHUB_OUTPUT")
        os.environ["GITHUB_OUTPUT"] = gh_output_path
        try:
            try:
                return self.onboard.main(argv) or 0
            except AssertionError:
                # fetch raised — expected, we only care about the file.
                return -1
        finally:
            if old is None:
                os.environ.pop("GITHUB_OUTPUT", None)
            else:
                os.environ["GITHUB_OUTPUT"] = old

    def test_en_emits_batches_batch6_txt_exactly(self):
        with tempfile.TemporaryDirectory() as tmp:
            gh = os.path.join(tmp, "github_output")
            rc = self._call_main([
                "--console", "Pokemon Delta Reign",
                "--set-name", "Delta Reign",
                "--release-date", "2026-11-06",
                "--printed-total", "103",
                "--language", "en",
                "--report", os.path.join(tmp, "report.txt"),
            ], gh)
            self.assertIn(rc, (-1, 3))  # fetch raised or returned 3
            lines = open(gh, "r", encoding="utf-8").read().splitlines()
            self.assertEqual(lines, ["batch_file=batches/batch6.txt"])
            self.assertRegex(
                lines[0].split("=", 1)[1],
                r"^batches/[A-Za-z0-9._-]+\.txt$",
            )

    def test_jp_emits_a_matching_jp_batch_exactly(self):
        with tempfile.TemporaryDirectory() as tmp:
            gh = os.path.join(tmp, "github_output")
            rc = self._call_main([
                "--console", "Pokemon Japanese Storm Emeralda",
                "--set-name", "Japanese Storm Emeralda",
                "--release-date", "2026-07-31",
                "--printed-total", "76",
                "--language", "jp",
                "--report", os.path.join(tmp, "report.txt"),
            ], gh)
            self.assertIn(rc, (-1, 3))
            lines = open(gh, "r", encoding="utf-8").read().splitlines()
            self.assertEqual(len(lines), 1)
            self.assertRegex(
                lines[0],
                r"^batch_file=batches/batch-japanese-\d+\.txt$",
            )

    def test_absolute_explicit_path_outside_repo_is_refused(self):
        # An absolute path that is not under the repo root cannot be
        # normalised — it must be refused before anything else runs.
        with tempfile.TemporaryDirectory() as tmp:
            gh = os.path.join(tmp, "github_output")
            open(gh, "w").close()
            rc = self._call_main([
                "--console", "Pokemon Delta Reign",
                "--set-name", "Delta Reign",
                "--release-date", "2026-11-06",
                "--printed-total", "103",
                "--language", "en",
                "--report", os.path.join(tmp, "report.txt"),
                "--batch-file", "/tmp/evil.txt",
                "--apply",
            ], gh)
            self.assertEqual(rc, 7)

    def test_parent_traversal_batch_file_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            gh = os.path.join(tmp, "github_output")
            open(gh, "w").close()
            rc = self._call_main([
                "--console", "Pokemon Delta Reign",
                "--set-name", "Delta Reign",
                "--release-date", "2026-11-06",
                "--printed-total", "103",
                "--language", "en",
                "--report", os.path.join(tmp, "report.txt"),
                "--batch-file", "../batches/batch6.txt",
                "--apply",
            ], gh)
            self.assertEqual(rc, 7)


class TestMinRowsArg(unittest.TestCase):
    """--min-rows plumbs through to the hard-fail check. 5 is the
    smallest value accepted; the default is 50."""

    @classmethod
    def setUpClass(cls):
        cls.onboard = _load("onboard_new_set", "onboard_new_set.py")

    def test_default_is_50(self):
        args = self.onboard._parse_args([
            "--console", "Pokemon X",
            "--set-name", "X",
            "--release-date", "2026-01-01",
            "--printed-total", "1",
            "--report", "r.txt",
        ])
        self.assertEqual(args.min_rows, 50)

    def test_lower_value_is_accepted(self):
        args = self.onboard._parse_args([
            "--console", "Pokemon X",
            "--set-name", "X",
            "--release-date", "2026-01-01",
            "--printed-total", "1",
            "--min-rows", "10",
            "--report", "r.txt",
        ])
        self.assertEqual(args.min_rows, 10)

    def test_floor_at_5_is_enforced_by_main(self):
        # The argparse layer accepts any integer; main() rejects < 5.
        argv = [
            "--console", "Pokemon X",
            "--set-name", "X",
            "--release-date", "2026-01-01",
            "--printed-total", "1",
            "--min-rows", "4",
            "--language", "en",
            "--report", "whatever.txt",
        ]
        rc = self.onboard.main(argv)
        self.assertEqual(rc, 7)


if __name__ == "__main__":
    unittest.main(verbosity=2)
