"""
seed_set_cards.py
=================
One-off seeder for the `cards` table when a brand-new set is added.

The nightly scraper (pokeprices_scraper_v8.py) only PATCHes existing
`cards` rows for image_url/pc_url; it does not INSERT. The site renders
set pages off the `cards` table, so we must pre-seed rows or the set
will exist in `daily_prices` but show nothing on the site.

Derives every column from the PriceCharting CSV plus a few hand-supplied
inputs (release date, printed total). Mirrors the column layout seen in
Perfect Order / Ascended Heroes rows.

Usage:
    set SUPABASE_URL=https://...
    set SUPABASE_SERVICE_KEY=eyJ...
    python seed_set_cards.py --csv "pc_csvs/Pokemon Chaos Rising.csv" \
        --set-name "Chaos Rising" --release-date 2026-05-22 --printed-total 83

Add --dry-run to preview without writing.
"""

import argparse
import csv
import os
import re
import sys

import requests

env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_path):
    for line in open(env_path, "r", encoding="utf-8"):
        line = line.strip()
        if line and "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY", "")

CARD_NUMBER_RE = re.compile(r"#(\d+[A-Za-z]*)\s*$")


def build_card_url_slug(product_name: str) -> str:
    """
    Slug for the website route (e.g. /set/Chaos Rising/card/<slug>).
    Strips & entirely to match how existing rows (Perfect Order etc.) are stored.
    """
    slug = product_name.lower()
    slug = slug.replace("[", "").replace("]", "")
    slug = slug.replace("#", "")
    slug = re.sub(r"[^a-z0-9\s]", "", slug)
    slug = re.sub(r"\s+", "-", slug.strip())
    slug = re.sub(r"-+", "-", slug)
    return slug


def build_pc_path_slug(product_name: str) -> str:
    """Mirror pokeprices_scraper_v8.build_url — keeps & so PC URL matches the scraper."""
    slug = product_name.lower()
    slug = slug.replace("[", "").replace("]", "")
    slug = slug.replace("#", "")
    slug = re.sub(r"[^a-z0-9\s&]", "", slug)
    slug = re.sub(r"\s+", "-", slug.strip())
    slug = re.sub(r"-+", "-", slug)
    return slug


def build_pc_url(console_name: str, product_name: str) -> str:
    console_slug = console_name.lower().replace(" ", "-")
    return f"https://www.pricecharting.com/game/{console_slug}/{build_pc_path_slug(product_name)}"


def extract_card_number(product_name: str) -> str | None:
    m = CARD_NUMBER_RE.search(product_name)
    return m.group(1) if m else None


def row_to_card(row, set_name, release_date, printed_total):
    pc_id = row["id"].strip()
    console_name = row["console-name"].strip()
    product_name = row["product-name"].strip()
    if not pc_id or not product_name:
        return None

    card_number = extract_card_number(product_name)
    is_sealed = card_number is None

    return {
        "card_slug": pc_id,
        "card_name": product_name,
        "set_name": set_name,
        "card_number": card_number,
        "card_number_display": f"{card_number}/{printed_total}" if card_number and printed_total else None,
        "set_printed_total": str(printed_total) if printed_total else None,
        "set_release_date": release_date,
        "is_sealed": is_sealed,
        "card_url_slug": build_card_url_slug(product_name),
        "pc_url": build_pc_url(console_name, product_name),
        # pc_slug is a generated column in the DB — don't insert it
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to PriceCharting CSV")
    p.add_argument("--set-name", required=True, help='DB set_name (no "Pokemon " prefix)')
    p.add_argument("--release-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--printed-total", type=int, required=True, help="e.g. 83")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY (or SUPABASE_KEY) must be set.")
        sys.exit(1)

    if not os.path.exists(args.csv):
        print(f"ERROR: CSV not found at {args.csv}")
        sys.exit(1)

    cards = []
    with open(args.csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            card = row_to_card(row, args.set_name, args.release_date, args.printed_total)
            if card:
                cards.append(card)

    print(f"Parsed {len(cards)} rows from CSV")
    numbered = sum(1 for c in cards if c["card_number"])
    sealed = sum(1 for c in cards if c["is_sealed"])
    print(f"  Singles (with #NN): {numbered}")
    print(f"  Sealed (no #NN):    {sealed}")
    print()
    print("Sample rows:")
    for c in cards[:3]:
        print(f"  {c}")

    if args.dry_run:
        print("\nDRY RUN — no DB writes.")
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    url = f"{SUPABASE_URL}/rest/v1/cards?on_conflict=card_slug"

    batch_size = 500
    inserted = 0
    for i in range(0, len(cards), batch_size):
        batch = cards[i:i + batch_size]
        resp = requests.post(url, json=batch, headers=headers, timeout=60)
        if resp.status_code in (200, 201, 204):
            inserted += len(batch)
            print(f"  Upserted batch {i}-{i + len(batch) - 1} ({len(batch)} rows)")
        else:
            print(f"  ERROR {resp.status_code}: {resp.text[:400]}")
            sys.exit(2)

    print(f"\nDone. Upserted {inserted}/{len(cards)} rows.")


if __name__ == "__main__":
    main()
