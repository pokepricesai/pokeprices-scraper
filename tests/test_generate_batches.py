"""
tests/test_generate_batches.py
==============================
Block 5A-W-48D-FIX1 — isolate the shared batch-generation infrastructure.

Since generate_batches.py can now rewrite any subset of batch files,
these tests pin the invariants that:

  * --language jp NEVER touches English batch files (byte-identical)
  * --language en NEVER touches Japanese batch files (byte-identical)
  * --language is REQUIRED (no accidental invocation with a default)
  * unknown language values fail
  * duplicate console-names across CSV files are rejected with a
    non-zero exit code
  * batch balancing is deterministic on identical input
  * the greedy packer preserves total row counts (no dropped sets)

All tests run against a fresh tmp CSV_DIR / BATCH_DIR — no touching
of the real pc_csvs/ or batches/ directories.
"""

import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GB_PATH = os.path.join(REPO_ROOT, "generate_batches.py")


def _load(module_name: str, filename: str):
    path = os.path.join(REPO_ROOT, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


gb = _load("gb", "generate_batches.py")


def make_csv(dir_: Path, name: str, rows_by_console: dict) -> Path:
    """Write a minimal PriceCharting-style CSV for testing."""
    p = dir_ / name
    with open(p, "w", encoding="utf-8", newline="\n") as f:
        f.write("id,console-name,product-name,loose-price\n")
        for con, n in rows_by_console.items():
            for i in range(n):
                f.write(f"{hash((name, con, i)) & 0xFFFFFF},{con},Card {i},$1.00\n")
    return p


class TestLanguageIsolation(unittest.TestCase):
    """Regenerating JP batches must not modify EN batches, and vice
    versa. This is the invariant that would have prevented a rewrite
    of English batches during W48D."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.csvdir = root / "pc_csvs"
        self.batchdir = root / "batches"
        self.csvdir.mkdir(); self.batchdir.mkdir()

        # Seed 3 EN CSVs
        make_csv(self.csvdir, "Pokemon Base Set.csv",       {"Pokemon Base Set": 100})
        make_csv(self.csvdir, "Pokemon Jungle.csv",         {"Pokemon Jungle": 60})
        make_csv(self.csvdir, "Pokemon Fossil.csv",         {"Pokemon Fossil": 50})
        # Seed 3 JP CSVs (filenames MUST contain "japan" to be classified JP)
        make_csv(self.csvdir, "Pokemon Japanese A.csv",     {"Pokemon Japanese A": 120})
        make_csv(self.csvdir, "Pokemon Japanese B.csv",     {"Pokemon Japanese B": 80})
        make_csv(self.csvdir, "Pokemon Japanese C.csv",     {"Pokemon Japanese C": 40})

        # Pre-write EN batches with a KNOWN payload so we can check byte-
        # identity after a JP regen. Using values that WOULDN'T be produced
        # by the packer, so a bug that overwrote them would fail obviously.
        (self.batchdir / "batch1.txt").write_text("SENTINEL EN 1\n", encoding="utf-8")
        (self.batchdir / "batch2.txt").write_text("SENTINEL EN 2\n", encoding="utf-8")
        (self.batchdir / "batch3.txt").write_text("SENTINEL EN 3\n", encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def _read_all(self, prefix: str):
        return {p.name: p.read_bytes() for p in sorted(self.batchdir.glob(f"{prefix}*.txt"))}

    def test_jp_regen_does_not_touch_en_batches(self):
        before = self._read_all("batch")
        # Filter to just the numeric-EN files (not the JP ones which have hyphens)
        before_en = {k: v for k, v in before.items() if k[len("batch"):-4].isdigit()}
        gb.run("jp", num_en_batches=6, num_jp_batches=2,
               csv_dir=self.csvdir, batch_dir=self.batchdir)
        after = self._read_all("batch")
        after_en = {k: v for k, v in after.items() if k[len("batch"):-4].isdigit()}
        self.assertEqual(before_en, after_en,
                         "JP regen must not modify any EN batch files")
        # But JP files must have been written
        jp_files = list(self.batchdir.glob("batch-japanese-*.txt"))
        self.assertEqual(len(jp_files), 2)

    def test_en_regen_does_not_touch_jp_batches(self):
        # First run: create JP batch files
        gb.run("jp", num_en_batches=6, num_jp_batches=2,
               csv_dir=self.csvdir, batch_dir=self.batchdir)
        before_jp = {p.name: p.read_bytes() for p in sorted(self.batchdir.glob("batch-japanese-*.txt"))}
        # Regenerate EN; JP files must remain byte-identical
        gb.run("en", num_en_batches=3, num_jp_batches=2,
               csv_dir=self.csvdir, batch_dir=self.batchdir)
        after_jp = {p.name: p.read_bytes() for p in sorted(self.batchdir.glob("batch-japanese-*.txt"))}
        self.assertEqual(before_jp, after_jp,
                         "EN regen must not modify any JP batch files")

    def test_stale_glob_does_not_delete_jp_files_on_en_regen(self):
        # Pre-create JP files that MUST survive an EN regen
        (self.batchdir / "batch-japanese-9.txt").write_text("survives\n", encoding="utf-8")
        gb.run("en", num_en_batches=3, num_jp_batches=2,
               csv_dir=self.csvdir, batch_dir=self.batchdir)
        self.assertTrue((self.batchdir / "batch-japanese-9.txt").exists())


class TestDuplicateConsoleDetection(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.csvdir = root / "pc_csvs"
        self.batchdir = root / "batches"
        self.csvdir.mkdir(); self.batchdir.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_duplicate_console_across_csvs_is_rejected(self):
        # Same console-name in two files — a real hazard we hit when a
        # stale CSV lingers alongside its Pokemon-prefixed replacement.
        make_csv(self.csvdir, "Japanese A.csv",         {"Pokemon Japanese A": 10})
        make_csv(self.csvdir, "Pokemon Japanese A.csv", {"Pokemon Japanese A": 100})
        with self.assertRaises(SystemExit) as cm:
            gb.run("jp", num_en_batches=6, num_jp_batches=2,
                   csv_dir=self.csvdir, batch_dir=self.batchdir)
        self.assertEqual(cm.exception.code, 4)

    def test_unique_consoles_pass(self):
        make_csv(self.csvdir, "Pokemon Japanese X.csv", {"Pokemon Japanese X": 30})
        make_csv(self.csvdir, "Pokemon Japanese Y.csv", {"Pokemon Japanese Y": 50})
        # No exception
        gb.run("jp", num_en_batches=6, num_jp_batches=2,
               csv_dir=self.csvdir, batch_dir=self.batchdir)
        files = list(self.batchdir.glob("batch-japanese-*.txt"))
        self.assertEqual(len(files), 2)


class TestArgparseGuards(unittest.TestCase):
    """--language is required; unknown values are rejected. Executed as
    a subprocess so argparse's SystemExit is observable."""

    def test_missing_language_fails(self):
        r = subprocess.run([sys.executable, GB_PATH],
                           capture_output=True, text=True, timeout=10)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("--language", r.stderr)

    def test_unknown_language_fails(self):
        r = subprocess.run([sys.executable, GB_PATH, "--language", "de"],
                           capture_output=True, text=True, timeout=10)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("--language", r.stderr)


class TestDeterminism(unittest.TestCase):
    def test_same_input_yields_same_batches(self):
        with tempfile.TemporaryDirectory() as tmp1, \
             tempfile.TemporaryDirectory() as tmp2:
            for root in (tmp1, tmp2):
                d = Path(root) / "pc_csvs"; d.mkdir()
                make_csv(d, "Pokemon Japanese Alpha.csv", {"Pokemon Japanese Alpha": 100})
                make_csv(d, "Pokemon Japanese Beta.csv",  {"Pokemon Japanese Beta":  60})
                make_csv(d, "Pokemon Japanese Gamma.csv", {"Pokemon Japanese Gamma": 40})
                make_csv(d, "Pokemon Japanese Delta.csv", {"Pokemon Japanese Delta": 80})
                (Path(root) / "batches").mkdir()
                gb.run("jp", num_en_batches=6, num_jp_batches=3,
                       csv_dir=Path(root)/"pc_csvs",
                       batch_dir=Path(root)/"batches")
            files1 = sorted((Path(tmp1)/"batches").glob("batch-japanese-*.txt"))
            files2 = sorted((Path(tmp2)/"batches").glob("batch-japanese-*.txt"))
            self.assertEqual(len(files1), len(files2))
            for f1, f2 in zip(files1, files2):
                self.assertEqual(f1.read_bytes(), f2.read_bytes(),
                                 f"non-deterministic output at {f1.name}")


class TestPackerCoverage(unittest.TestCase):
    def test_packer_preserves_every_set(self):
        counts = {"A": 100, "B": 60, "C": 40, "D": 80, "E": 20}
        bins = gb.pack(counts, num_batches=3)
        placed = {name for b in bins for name, _ in b}
        self.assertEqual(placed, set(counts))
        placed_totals = sum(n for b in bins for _, n in b)
        self.assertEqual(placed_totals, sum(counts.values()))

    def test_packer_balances_reasonably(self):
        counts = {f"Set{i}": (i * 10 + 1) for i in range(20)}
        bins = gb.pack(counts, num_batches=4)
        totals = [sum(n for _, n in b) for b in bins]
        # Greedy packing on this input should keep max within 2x min
        self.assertLess(max(totals) / max(1, min(totals)), 2.0)


class TestDryRun(unittest.TestCase):
    def test_dry_run_writes_no_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp) / "pc_csvs"; d.mkdir()
            (Path(tmp) / "batches").mkdir()
            make_csv(d, "Pokemon Japanese Z.csv", {"Pokemon Japanese Z": 50})
            gb.run("jp", num_en_batches=6, num_jp_batches=2,
                   csv_dir=d, batch_dir=Path(tmp)/"batches", dry_run=True)
            files = list((Path(tmp)/"batches").iterdir())
            self.assertEqual(files, [], "dry-run must not write any files")


if __name__ == "__main__":
    unittest.main(verbosity=2)
