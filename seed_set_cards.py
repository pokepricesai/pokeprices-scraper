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

Block 5A-W-48B additions:
    * --language {en,jp}: writes cards.language and set_metadata.language.
      Defaults to 'en'. 'jp' is the only other accepted value today; any
      value outside {en, jp} fails fast before any DB write.
    * Always upserts a set_metadata row for the target set with the same
      language. Existing rows are updated in place (idempotent).
    * A defensive console-name gate: if --require-console <name> is
      passed, ANY row whose console-name differs is skipped and reported
      rather than silently seeded. Prevents a Japanese CSV being seeded
      as English by accident.

Usage:
    set SUPABASE_URL=https://...
    set SUPABASE_SERVICE_KEY=eyJ...

    # New English set:
    python seed_set_cards.py --csv "pc_csvs/Pokemon Chaos Rising.csv" \
        --set-name "Chaos Rising" --release-date 2026-05-22 --printed-total 83

    # New Japanese pilot set (W48B):
    python seed_set_cards.py \
        --csv "pc_csvs/Pokemon Japanese Battle Partners.csv" \
        --set-name "Japanese Battle Partners" \
        --release-date 2025-05-30 --printed-total 120 \
        --language jp \
        --require-console "Pokemon Japanese Battle Partners"

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
ALLOWED_LANGUAGES = ("en", "jp")


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


def row_to_card(row, set_name, release_date, printed_total, language):
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
        # W48B — language passthrough. Defaults to 'en' when the flag
        # is not supplied so existing English seeds continue to work.
        "language": language,
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


def upsert_set_metadata(set_name, release_date, printed_total, language, dry_run=False):
    """W48B (fixed) — seed / update the set_metadata row for this set.

    The site's sitemap and set-existence check read from set_metadata,
    so a Japanese pilot set needs a row here in addition to the cards
    rows. Idempotent: on_conflict on set_name.

    Live schema introspection (2026-07-29) confirms set_metadata
    columns: id, set_name, release_year, total_cards,
    has_first_edition, print_run_era, pc_set_slug, notes, updated_at.
    We derive release_year from the CLI --release-date argument and
    total_cards from --printed-total. Anything else is left NULL / at
    default so existing values are not overwritten across re-runs.
    The language column is added by the W48B foundation migration; if
    the migration has not yet been applied the language field is
    silently ignored by PostgREST — safe pre-migration."""
    year: int | None = None
    if release_date and len(release_date) >= 4 and release_date[:4].isdigit():
        year = int(release_date[:4])
    row = {
        "set_name":    set_name,
        "language":    language,
    }
    if year is not None:
        row["release_year"] = year
    if printed_total:
        row["total_cards"] = printed_total
    if dry_run:
        print(f"  DRY-RUN: would upsert set_metadata row: {row}")
        return
    url = f"{SUPABASE_URL}/rest/v1/set_metadata?on_conflict=set_name"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    resp = requests.post(url, json=[row], headers=headers, timeout=30)
    if resp.status_code in (200, 201, 204):
        print(f"  set_metadata upserted: set_name={set_name!r} language={language!r} "
              f"release_year={year} total_cards={printed_total}")
    else:
        # Most likely failure: language column missing because the
        # W48B foundation migration has not been applied. Retry once
        # without the language field so English English seeds continue
        # to work pre-migration.
        print(f"  WARN: set_metadata upsert failed ({resp.status_code}): {resp.text[:200]}")
        row.pop("language", None)
        print(f"  Retrying without language field (pre-migration path)…")
        resp2 = requests.post(url, json=[row], headers=headers, timeout=30)
        if resp2.status_code in (200, 201, 204):
            print(f"  Retry OK. Foundation migration is NOT yet applied — the")
            print(f"  language field remains at its column default (or NULL if")
            print(f"  the migration is not applied) and must be set by hand:")
            print(f"    UPDATE set_metadata SET language='{language}' WHERE set_name='{set_name}';")
        else:
            print(f"  Retry ALSO failed ({resp2.status_code}): {resp2.text[:300]}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to PriceCharting CSV")
    p.add_argument("--set-name", required=True, help='DB set_name (no "Pokemon " prefix)')
    p.add_argument("--release-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--printed-total", type=int, default=None,
                   help="e.g. 83. Omit for sets without a fixed total (promos).")
    p.add_argument("--language", choices=list(ALLOWED_LANGUAGES), default="en",
                   help="Language marker for cards.language + set_metadata.language. "
                        "Defaults to 'en' — English CSVs need no change. Use 'jp' for "
                        "Japanese sets (W48B pilot).")
    p.add_argument("--require-console", default=None,
                   help="Refuse to seed any CSV row whose console-name column does not "
                        "match this exact string. Prevents a mislabelled CSV silently "
                        "flowing into the wrong set. Recommended for Japanese imports.")
    p.add_argument("--insert-only", action="store_true",
                   help="Skip rows whose card_slug already exists; never UPDATE.")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if args.language not in ALLOWED_LANGUAGES:
        # argparse.choices already blocks this, but keep the guard so
        # a future refactor cannot regress it.
        print(f"ERROR: --language must be one of {ALLOWED_LANGUAGES}, got {args.language!r}.")
        sys.exit(1)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY (or SUPABASE_KEY) must be set.")
        sys.exit(1)

    if not os.path.exists(args.csv):
        print(f"ERROR: CSV not found at {args.csv}")
        sys.exit(1)

    # ── Read CSV, gate on console-name, and build the card rows ───
    cards = []
    console_counts: dict[str, int] = {}
    skipped_console = 0
    with open(args.csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            console_name = row.get("console-name", "").strip()
            console_counts[console_name] = console_counts.get(console_name, 0) + 1
            if args.require_console and console_name != args.require_console:
                skipped_console += 1
                continue
            card = row_to_card(row, args.set_name, args.release_date,
                               args.printed_total, args.language)
            if card:
                cards.append(card)

    print(f"Parsed {len(cards)} rows from CSV (language={args.language!r})")
    numbered = sum(1 for c in cards if c["card_number"])
    sealed = sum(1 for c in cards if c["is_sealed"])
    print(f"  Singles (with #NN): {numbered}")
    print(f"  Sealed  (no #NN):   {sealed}")

    if console_counts:
        print(f"  console-name distribution in the CSV:")
        for k, v in sorted(console_counts.items(), key=lambda kv: -kv[1]):
            print(f"    {v:>6}  {k!r}")
    if args.require_console:
        print(f"  --require-console: rows kept must match {args.require_console!r}. "
              f"Skipped: {skipped_console}.")

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

    # Always upsert the set_metadata row (idempotent), regardless of
    # --dry-run or --insert-only — it's just one row and the pages need
    # it. Skipped only in dry-run mode.
    upsert_set_metadata(args.set_name, args.release_date, args.printed_total,
                        args.language, dry_run=args.dry_run)

    if args.dry_run:
        print("\nDRY RUN — no cards writes.")
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
    print(f"\nDone. {verb} {inserted}/{len(cards)} rows (language={args.language!r}).")


if __name__ == "__main__":
    main()
