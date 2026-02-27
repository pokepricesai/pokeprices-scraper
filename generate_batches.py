#!/usr/bin/env python3
"""
Generate balanced batch files for the PokePrices nightly scraper.

Run this from the root of the pokeprices-scraper repo:
    python generate_batches.py

It reads all CSVs in pc_csvs/, reads the console-name field from each row,
counts cards per set, and splits them into 4 roughly equal batches
by total card count.

Creates: batches/batch1.txt through batches/batch4.txt
"""

import csv
import os
from pathlib import Path
from collections import Counter

CSV_DIR = Path("pc_csvs")
BATCH_DIR = Path("batches")
NUM_BATCHES = 4

def main():
    if not CSV_DIR.exists():
        print(f"ERROR: {CSV_DIR} not found. Run this from the repo root.")
        return

    # Count cards per set by reading console-name from every CSV
    set_counts = Counter()
    
    csv_files = sorted(CSV_DIR.glob("*.csv"))
    print(f"Reading {len(csv_files)} CSV files...\n")
    
    for csv_file in csv_files:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                console_name = row.get("console-name", "").strip()
                if console_name:
                    set_counts[console_name] += 1
    
    sets = [(name, count) for name, count in set_counts.items()]
    sets.sort(key=lambda x: x[0])  # alphabetical for display
    
    print(f"Found {len(sets)} sets, {sum(c for _, c in sets)} total cards:\n")
    for name, count in sets:
        print(f"  {name}: {count} cards")
    
    # Sort by card count descending for greedy bin packing
    sets.sort(key=lambda x: -x[1])

    # Distribute into batches: always add to the lightest batch
    batches: list[list[tuple[str, int]]] = [[] for _ in range(NUM_BATCHES)]
    batch_totals = [0] * NUM_BATCHES

    for set_name, count in sets:
        min_idx = batch_totals.index(min(batch_totals))
        batches[min_idx].append((set_name, count))
        batch_totals[min_idx] += count

    # Write batch files
    BATCH_DIR.mkdir(exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"BATCH ALLOCATION")
    print(f"{'='*60}")
    
    for i, batch in enumerate(batches, 1):
        filepath = BATCH_DIR / f"batch{i}.txt"
        with open(filepath, "w") as f:
            for set_name, _ in sorted(batch):  # alphabetical within batch
                f.write(f"{set_name}\n")

        total = sum(c for _, c in batch)
        est_hours = (total * 1.2) / 3600  # 0.4s delay + ~0.8s fetch
        print(f"\nbatch{i}.txt — {len(batch)} sets, {total} cards (~{est_hours:.1f} hours)")
        for set_name, count in sorted(batch):
            print(f"    {set_name} ({count})")

    print(f"\n{'='*60}")
    print(f"Balance: {batch_totals}")
    print(f"Batch files written to {BATCH_DIR}/")
    print(f"\nNext: commit the batches/ folder and push to GitHub.")

if __name__ == "__main__":
    main()
