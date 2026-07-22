"""
import_pitch_black.py
=====================
One-time import of the Pitch Black set into the cards table.
Based on the proven Perfect Order import (April 2026).

Reads:  pc_csvs/Pokemon Pitch Black.csv
Writes: cards table (upsert, safe to re-run)

Set facts (verified July 2026):
  - Release date: 2026-07-17
  - Printed total: 84 (120 cards total incl. 36 secret rares, numbered NN/084)
  - Era: Mega Evolution (5th set)

Usage (PowerShell, from scraper repo root):
  $env:SUPABASE_KEY = "eyJ..."   # legacy service role JWT — NOT sb_publishable_
  python import_pitch_black.py            # dry run preview first
  python import_pitch_black.py --push     # actually push to Supabase
"""

import csv
import os
import re
import sys
import requests

SUPABASE_URL = "https://egidpsrkqvymvioidatc.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

CSV_PATH = "pc_csvs/Pokemon Pitch Black.csv"
SET_NAME = "Pitch Black"          # clean name for the site — NOT the CSV filename
RELEASE_DATE = "2026-07-17"
PRINTED_TOTAL = "84"              # cards are numbered NN/084


def make_url_slug(text):
    slug = text.lower()
    slug = slug.replace("'s", "s").replace("'", "")
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = slug.strip()
    slug = re.sub(r"\s+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug


def extract_card_number(product_name):
    match = re.search(r"#(\w+)", product_name)
    return match.group(1) if match else None


def is_sealed(product_name):
    sealed_keywords = [
        "booster", "pack", "box", "blister", "bundle",
        "collection", "tin", "etb", "elite trainer",
        "build & battle", "build and battle", "case", "display",
    ]
    return any(kw in product_name.lower() for kw in sealed_keywords)


def build_rows():
    rows = []
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pc_id = (row.get("id") or "").strip()
            product_name = (row.get("product-name") or "").strip()
            console_name = (row.get("console-name") or "").strip()

            if not pc_id or not product_name:
                continue

            card_number = extract_card_number(product_name)
            sealed = is_sealed(product_name)

            # pc_url — mirrors the scraper's build_url logic (periods/apostrophes stripped)
            console_slug = console_name.lower().replace(" ", "-")
            name_slug = product_name.lower()
            name_slug = re.sub(r"[\[\]#]", "", name_slug)
            name_slug = re.sub(r"[^a-z0-9\s&]", "", name_slug)
            name_slug = name_slug.strip()
            name_slug = re.sub(r"\s+", "-", name_slug)
            name_slug = re.sub(r"-+", "-", name_slug)
            pc_url = f"https://www.pricecharting.com/game/{console_slug}/{name_slug}"

            # card_number_display: "12/84" for numbered singles, null for sealed
            display = f"{card_number}/{PRINTED_TOTAL}" if (card_number and not sealed) else None

            card_slug_text = make_url_slug(product_name)

            # NOTE: pc_slug is a GENERATED column in the DB — do not include it.
            # NOTE: url_slug is legacy (old /card/[slug] route, deleted Apr 2026) and has
            # a unique index — leave it out entirely; recent sets (Chaos Rising) have NULL.
            # Card pages route on card_url_slug via /set/{set}/card/{card_url_slug}.
            rows.append({
                "card_slug": pc_id,                      # bare numeric — daily_prices uses pc- prefix
                "card_name": product_name,               # keeps the #NN suffix, per convention
                "set_name": SET_NAME,
                "card_number": card_number,
                "pc_url": pc_url,
                "card_url_slug": card_slug_text,
                "is_sealed": sealed,
                "card_number_display": display,
                "set_release_date": RELEASE_DATE,
                "set_printed_total": PRINTED_TOTAL,
            })
    return rows


def main():
    push = "--push" in sys.argv

    if not os.path.exists(CSV_PATH):
        print(f"ERROR: CSV not found at {CSV_PATH}")
        print("Check the filename matches exactly (including 'Pokemon ' prefix).")
        sys.exit(1)

    rows = build_rows()
    singles = [r for r in rows if not r["is_sealed"]]
    sealed = [r for r in rows if r["is_sealed"]]

    print(f"Parsed {len(rows)} rows from {CSV_PATH}")
    print(f"  Singles: {len(singles)}   Sealed products: {len(sealed)}")
    print()
    print("Sample single:", singles[0] if singles else "NONE")
    print()
    print("Sample sealed:", sealed[0] if sealed else "NONE")
    print()
    print("Sealed product names (verify these are all actually sealed):")
    for r in sealed:
        print(f"  - {r['card_name']}")

    if not push:
        print()
        print("DRY RUN complete. Re-run with --push to send to Supabase.")
        return

    if not SUPABASE_KEY:
        print("ERROR: SUPABASE_KEY env var not set.")
        print('Set it first:  $env:SUPABASE_KEY = "eyJ..."  (legacy service role key)')
        sys.exit(1)

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    print(f"\nPushing {len(rows)} cards to Supabase...")
    generated_cols = set()
    i = 0
    while i < len(rows):
        batch = [
            {k: v for k, v in r.items() if k not in generated_cols}
            for r in rows[i:i + 500]
        ]
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/cards",
            json=batch, headers=headers, timeout=30,
        )
        if resp.status_code in (200, 201, 204):
            print(f"  Batch {i // 500 + 1}: {resp.status_code}")
            i += 500
            continue
        # If another column turns out to be DB-generated, strip it and retry this batch
        m = re.search(r'\\?"(\w+)\\?" is a generated column', resp.text)
        if m:
            col = m.group(1)
            print(f"  Column '{col}' is DB-generated — removing from payload and retrying batch...")
            generated_cols.add(col)
            continue
        print(f"  Batch {i // 500 + 1}: {resp.status_code}")
        print(f"  Error: {resp.text[:300]}")
        sys.exit(1)
    if generated_cols:
        print(f"  (skipped DB-generated columns: {', '.join(sorted(generated_cols))})")
    print("Done. Cards are in.")


if __name__ == "__main__":
    main()
