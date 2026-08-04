"""
bulk_seed_japanese.py
=====================
Block 5A-W-48D — drive seed_set_cards.py in bulk from a manifest.

The manifest is a JSON list of dicts:
    {"csv": "...", "console_name": "Pokemon Japanese <Set>",
     "set_name": "Japanese <Set>", "release_year": <int|null>,
     "release_date_seeder": "YYYY-MM-DD" | null,
     "total_cards": <int|null>, "row_count": <int>, ...}

Usage:
    # Dry-run every set
    python bulk_seed_japanese.py --manifest manifests/japanese_sets.json --dry-run

    # Real import
    python bulk_seed_japanese.py --manifest manifests/japanese_sets.json

    # Import only a subset by console-name substring (case-insensitive)
    python bulk_seed_japanese.py --manifest manifests/japanese_sets.json --filter "Wild Force"

Each entry's `console_name` is passed to seed_set_cards.py as
--require-console, so a mislabelled row aborts that set individually.

Zero global side effects — every set runs the exact same seeder path.
"""

import argparse, json, os, subprocess, sys, time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SEEDER = os.path.join(SCRIPT_DIR, "seed_set_cards.py")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--manifest", required=True, help="Path to japanese_sets.json")
    p.add_argument("--filter", default=None,
                   help="Case-insensitive substring; only sets whose console_name matches are processed.")
    p.add_argument("--dry-run", action="store_true",
                   help="Passes --dry-run to seed_set_cards.py for every entry.")
    p.add_argument("--insert-only", action="store_true",
                   help="Passes --insert-only to seed_set_cards.py for every entry.")
    p.add_argument("--limit", type=int, default=None,
                   help="Process at most N entries (useful for wave batching).")
    p.add_argument("--skip", type=int, default=0,
                   help="Skip the first N entries (useful for wave batching).")
    # Block 5A-W-51C — safety flag. When a manifest entry does NOT
    # provide `printed_denominator`, refuse the import by default so
    # modern JP sets with secret cards cannot silently repeat the
    # Battle Partners /130 mistake. Pass this flag to opt back into
    # the old (wrong for secret-card sets) behaviour of using
    # total_cards as the printed denominator.
    p.add_argument("--allow-total-cards-as-denominator", action="store_true",
                   help="Fall back to total_cards when printed_denominator is missing (unsafe for modern sets with secret cards).")
    args = p.parse_args()

    manifest = json.load(open(args.manifest, "r", encoding="utf-8"))
    entries = manifest
    if args.filter:
        needle = args.filter.lower()
        entries = [e for e in entries if needle in e["console_name"].lower()]
    if args.skip:
        entries = entries[args.skip:]
    if args.limit:
        entries = entries[:args.limit]

    print(f"bulk_seed: {len(entries)} sets ({'DRY-RUN' if args.dry_run else 'REAL'}), "
          f"est. {sum(e.get('row_count',0) for e in entries)} rows total")

    successes = []
    failures = []
    for i, e in enumerate(entries):
        csv = e["csv"]
        cmd = [
            sys.executable, SEEDER,
            "--csv", f"pc_csvs/{csv}",
            "--set-name", e["set_name"],
            "--language", "jp",
            "--require-console", e["console_name"],
        ]
        if e.get("release_date_seeder"):
            cmd += ["--release-date", e["release_date_seeder"]]
        # Block 5A-W-51C — prefer `printed_denominator` (the number
        # actually printed after the slash on the card) over
        # `total_cards` (catalogue row count). Modern Japanese sets
        # commonly have secret cards numbered above the printed base
        # (Battle Partners: 100 base + 32 secrets = catalogue 132/130).
        # Passing the catalogue count as --printed-total produced the
        # wrong card_number_display for every affected row.
        if e.get("printed_denominator"):
            cmd += ["--printed-total", str(e["printed_denominator"])]
        elif e.get("total_cards"):
            # No printed_denominator supplied. Warn loudly and fall
            # back to total_cards. For production imports of modern
            # sets this fallback should be treated as suspect: pass
            # --allow-total-cards-as-denominator explicitly or add a
            # printed_denominator field to the manifest.
            if not args.allow_total_cards_as_denominator:
                print(
                    f"ERROR: {e['set_name']}: manifest is missing `printed_denominator`. "
                    f"Modern Japanese sets often have a printed base different from the catalogue "
                    f"count. Add printed_denominator to the manifest, or re-run with "
                    f"--allow-total-cards-as-denominator to accept total_cards={e['total_cards']} "
                    f"as the printed denominator.",
                    file=sys.stderr,
                )
                failures.append((e["console_name"], -1, f"printed_denominator missing from manifest for {e['set_name']}"))
                continue
            print(
                f"WARN: {e['set_name']}: no printed_denominator supplied. "
                f"Using total_cards={e['total_cards']} as fallback (may be wrong for sets with secret cards).",
                file=sys.stderr,
            )
            cmd += ["--printed-total", str(e["total_cards"])]
        if args.dry_run:
            cmd.append("--dry-run")
        if args.insert_only:
            cmd.append("--insert-only")

        print(f"\n[{i+1}/{len(entries)}] {e['set_name']}  ({e.get('row_count','?')} rows)")
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600,
                           env={**os.environ, "PYTHONIOENCODING": "utf-8"})
        # Extract key lines
        summary_lines = []
        for line in r.stdout.splitlines():
            if any(k in line for k in [
                "Parsed", "set_metadata upserted", "Wrote batch", "Done.",
                "ERROR", "WARN:", "DRY RUN", "DRY-RUN",
                "DB diff", "Already in DB", "New (CSV only)",
            ]):
                summary_lines.append("  " + line.strip())
        for line in summary_lines[:8]:
            print(line)
        if r.returncode == 0:
            successes.append(e["console_name"])
        else:
            failures.append((e["console_name"], r.returncode, r.stderr[:200]))
            print(f"  FAIL (exit={r.returncode}): {r.stderr[:300]}")

    print(f"\n== SUMMARY ==")
    print(f"Total processed: {len(entries)}")
    print(f"Successes:       {len(successes)}")
    print(f"Failures:        {len(failures)}")
    if failures:
        for c, code, err in failures[:10]:
            print(f"  FAIL {c}: exit={code}, err={err[:150]}")
        sys.exit(1)

if __name__ == "__main__":
    main()
