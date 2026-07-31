#!/usr/bin/env python3
"""
Generate balanced batch files for the PokePrices nightly scraper.

Run from the root of the pokeprices-scraper repo. --language is
REQUIRED so a bare `python generate_batches.py` cannot accidentally
regenerate the English batches:

    python generate_batches.py --language en    # regenerate English batches
    python generate_batches.py --language jp    # regenerate Japanese batches
    python generate_batches.py --language all   # regenerate both explicitly

Language segregation is enforced by filename ("japanese" in filename =>
JP; otherwise EN). Japanese and English batch files are ALWAYS
regenerated separately so a batch never mixes languages.

Output:
  * English:  batches/batch1.txt .. batch<N>.txt          (default N=6)
  * Japanese: batches/batch-japanese-1.txt .. -<M>.txt    (default M=4)

Block 5A-W-48D: extended from single-language to multi-language.
Block 5A-W-48D-FIX1: --language is now required, duplicate console
names across files are rejected, batch regeneration reports the exact
prefix it touches, --dry-run prints the plan without writing.
"""

import argparse
import csv
import re
import sys
from pathlib import Path
from collections import Counter, defaultdict

CSV_DIR = Path("pc_csvs")
BATCH_DIR = Path("batches")


def is_japanese_csv(path: Path) -> bool:
    """Filename-based language detection. `japan` (any casing) => JP."""
    return "japan" in path.name.lower()


def read_counts(language: str, csv_dir: Path = CSV_DIR):
    """Return (counts, per_console_files) where counts maps
    console-name -> total row count and per_console_files maps
    console-name -> set of CSV filenames it was seen in.

    Callers use per_console_files to detect duplicate console names
    across files, which would silently double-count during batch
    packing and produce a corrupted schedule."""
    counts: Counter = Counter()
    per_file: dict[str, set[str]] = defaultdict(set)
    for csv_file in sorted(csv_dir.glob("*.csv")):
        if language == "en" and is_japanese_csv(csv_file):
            continue
        if language == "jp" and not is_japanese_csv(csv_file):
            continue
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                con = row.get("console-name", "").strip()
                if con:
                    counts[con] += 1
                    per_file[con].add(csv_file.name)
    return counts, per_file


def check_duplicates(per_file: dict) -> list[str]:
    """Return a list of console-names that appear in more than one CSV
    file. Duplicates silently corrupt batch balancing (a set gets
    scraped twice per night) so callers should treat any non-empty
    return as fatal."""
    return sorted([c for c, files in per_file.items() if len(files) > 1])


def pack(counts, num_batches: int) -> list[list[tuple[str, int]]]:
    """Greedy bin-packing: largest set into lightest current bin.
    Deterministic on identical input."""
    sets = sorted(counts.items(), key=lambda x: (-x[1], x[0]))  # tie-break by name
    bins: list[list[tuple[str, int]]] = [[] for _ in range(num_batches)]
    totals = [0] * num_batches
    for name, n in sets:
        i = totals.index(min(totals))
        bins[i].append((name, n))
        totals[i] += n
    return bins


def _stale_files_for(prefix: str, batch_dir: Path) -> list[Path]:
    """List existing batch files matching this prefix, so a shrinking
    batch count does not leave orphaned files that CI still references.
    English uses `batch<digits>.txt`; Japanese uses
    `batch-japanese-<digits>.txt`. Care is taken not to sweep JP files
    when regenerating EN and vice versa."""
    if prefix == "batch":
        return [p for p in batch_dir.glob("batch*.txt")
                if re.fullmatch(r"batch\d+\.txt", p.name)]
    return [p for p in batch_dir.glob(f"{prefix}*.txt")
            if re.fullmatch(rf"{re.escape(prefix)}\d+\.txt", p.name)]


def write_batches(bins, prefix: str, batch_dir: Path = BATCH_DIR,
                  dry_run: bool = False) -> list[Path]:
    """Regenerate every batch file matching `prefix`. Returns the list
    of paths that were (or would be) written."""
    batch_dir.mkdir(exist_ok=True)
    if not dry_run:
        for old in _stale_files_for(prefix, batch_dir):
            old.unlink()
    written = []
    for i, b in enumerate(bins, 1):
        path = batch_dir / f"{prefix}{i}.txt"
        content = "".join(f"{name}\n" for name, _ in sorted(b))
        if not dry_run:
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write(content)
        written.append(path)
        total = sum(c for _, c in b)
        est_hours = (total * 1.2) / 3600
        tag = " [DRY-RUN]" if dry_run else ""
        print(f"  {path.name}{tag} — {len(b)} sets, {total} rows (~{est_hours:.1f}h)")
    return written


def run(language: str, num_en_batches: int, num_jp_batches: int,
        csv_dir: Path = CSV_DIR, batch_dir: Path = BATCH_DIR,
        dry_run: bool = False) -> None:
    """Regenerate batch files for the given language(s). Aborts with
    exit code 4 if a duplicate console-name is detected across CSV
    files (see check_duplicates)."""
    if not csv_dir.exists():
        print(f"ERROR: {csv_dir} not found. Run from repo root.")
        sys.exit(1)

    def do(lang: str, n_batches: int, prefix: str):
        print(f"== {'English' if lang=='en' else 'Japanese'} ==")
        counts, per_file = read_counts(lang, csv_dir=csv_dir)
        print(f"  {len(counts)} sets, {sum(counts.values())} rows")
        dups = check_duplicates(per_file)
        if dups:
            print(f"\nDUPLICATE console-names detected across {lang.upper()} CSV files:")
            for c in dups[:10]:
                print(f"  {c!r} appears in: {sorted(per_file[c])}")
            print("\nAborting to avoid corrupted batch balancing.")
            sys.exit(4)
        write_batches(pack(counts, n_batches), prefix=prefix,
                      batch_dir=batch_dir, dry_run=dry_run)

    if language == "en":
        do("en", num_en_batches, "batch")
    elif language == "jp":
        do("jp", num_jp_batches, "batch-japanese-")
    elif language == "all":
        do("en", num_en_batches, "batch")
        do("jp", num_jp_batches, "batch-japanese-")
    else:
        # argparse should have blocked this, but keep the guard.
        print(f"ERROR: --language must be one of en, jp, all (got {language!r}).")
        sys.exit(2)


def main():
    p = argparse.ArgumentParser()
    # W48D-FIX1: --language required. No default. `python generate_batches.py`
    # by itself is a parser error so nobody can accidentally rewrite English
    # batches by omitting the flag.
    p.add_argument("--language", choices=("en", "jp", "all"), required=True)
    p.add_argument("--en-batches", type=int, default=6)
    p.add_argument("--jp-batches", type=int, default=4)
    p.add_argument("--dry-run", action="store_true",
                   help="Print the planned batch layout without writing any files.")
    args = p.parse_args()
    run(args.language, args.en_batches, args.jp_batches, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
