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

    # New set:
    python seed_set_cards.py --csv "pc_csvs/Pokemon Chaos Rising.csv" \
        --set-name "Chaos Rising" --release-date 2026-05-22 --printed-total 83

    # Promo / open-ended set update — only insert card_slugs not already in DB:
    python seed_set_cards.py --csv "pc_csvs/Pokemon Promo.csv" \
        --set-name "Promo" --release-date 1999-01-01 --insert-only

Add --dry-run to preview without writing. --printed-total is optional for
sets with no fixed denominator (promos).
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
    Slug for the website route (e.g. /set/Promo/card/<slug>).
    Matches the convention of existing rows: preserves `-`, strips `[]`,`#`,`'`,`&`,`,`,
    collapses whitespace + duplicate dashes.
    """
    slug = product_name.lower()
    slug = slug.replace("[", "").replace("]", "")
    slug = slug.replace("#", "")
    slug = re.sub(r"[^a-z0-9\s\-]", "", slug)
    slug = re.sub(r"\s+", "-", slug.strip())
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


def build_pc_path_slug(product_name: str) -> str:
    """
    Mirror existing pc_url convention in DB: preserves `-`, `'`, `&`, strips `[]`, `#`, `,`.
    Spaces → dashes.
    """
    slug = product_name.lower()
    slug = slug.replace("[", "").replace("]", "")
    slug = slug.replace("#", "")
    slug = re.sub(r"[^a-z0-9\s\-&']", "", slug)
    slug = re.sub(r"\s+", "-", slug.strip())
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


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


def fetch_existing_card_slugs(set_name):
    """Page through cards table to return every card_slug already in the given set."""
    from urllib.parse import quote
    slugs = set()
    offset = 0
    page = 1000
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/cards?set_name=eq.{quote(set_name)}&select=card_slug"
            f"&order=card_slug.asc&offset={offset}&limit={page}",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"  WARN: failed to fetch existing slugs at offset {offset}: {r.status_code}")
            break
        batch = r.json()
        if not batch:
            break
        slugs.update(row["card_slug"] for row in batch)
        if len(batch) < page:
            break
        offset += page
    return slugs


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to PriceCharting CSV")
    p.add_argument("--set-name", required=True, help='DB set_name (no "Pokemon " prefix)')
    p.add_argument("--release-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--printed-total", type=int, default=None,
                   help="e.g. 83. Omit for sets without a fixed total (promos).")
    p.add_argument("--insert-only", action="store_true",
                   help="Skip rows whose card_slug already exists; never UPDATE.")
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

    # Diff against the DB so the report makes sense in --insert-only mode
    existing = fetch_existing_card_slugs(args.set_name)
    csv_slugs = {c["card_slug"] for c in cards}
    new_slugs = csv_slugs - existing
    overlap   = csv_slugs & existing
    print(f"\nDB diff for set_name='{args.set_name}':")
    print(f"  Already in DB:   {len(overlap)}")
    print(f"  New (CSV only):  {len(new_slugs)}")
    print(f"  In DB but not in CSV (no-op either way): {len(existing - csv_slugs)}")

    if args.insert_only:
        cards = [c for c in cards if c["card_slug"] in new_slugs]
        print(f"\n--insert-only: will attempt {len(cards)} new rows.")

    if cards:
        print("\nSample rows to write:")
        for c in cards[:3]:
            print(f"  {c}")

    if args.dry_run:
        print("\nDRY RUN — no DB writes.")
        return

    if not cards:
        print("\nNothing to write.")
        return

    # ignore-duplicates is a belt-and-braces guard — even in --insert-only mode
    # a concurrent insert could race in between the diff and the POST.
    resolution = "ignore-duplicates" if args.insert_only else "merge-duplicates"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": f"resolution={resolution},return=minimal",
    }
    url = f"{SUPABASE_URL}/rest/v1/cards?on_conflict=card_slug"

    batch_size = 500
    inserted = 0
    for i in range(0, len(cards), batch_size):
        batch = cards[i:i + batch_size]
        resp = requests.post(url, json=batch, headers=headers, timeout=60)
        if resp.status_code in (200, 201, 204):
            inserted += len(batch)
            print(f"  Wrote batch {i}-{i + len(batch) - 1} ({len(batch)} rows)")
        else:
            print(f"  ERROR {resp.status_code}: {resp.text[:400]}")
            sys.exit(2)

    verb = "Inserted" if args.insert_only else "Upserted"
    print(f"\nDone. {verb} {inserted}/{len(cards)} rows.")


if __name__ == "__main__":
    main()
