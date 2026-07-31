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

    row_out = {
        "card_slug": pc_id,
        "card_name": product_name,
        "set_name": set_name,
        "card_number": card_number,
        "card_number_display": f"{card_number}/{printed_total}" if card_number and printed_total else None,
        "set_printed_total": str(printed_total) if printed_total else None,
        "is_sealed": is_sealed,
        "card_url_slug": build_card_url_slug(product_name),
        "pc_url": build_pc_url(console_name, product_name),
        # W48B — language passthrough. Defaults to 'en' when the flag
        # is not supplied so existing English seeds continue to work.
        "language": language,
        # pc_slug is a generated column in the DB — don't insert it
    }
    # W48D — set_release_date is optional; only include it when caller
    # provides a date. PostgREST tolerates a missing key on insert
    # (column stays NULL) but silently overwrites a real value if the
    # key is present with None. So we conditionally attach it.
    if release_date:
        row_out["set_release_date"] = release_date
    return row_out


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


def fetch_existing_cards_by_slug(slugs):
    """Block 5A-W-48D-FIX1 — fetch every existing cards row for any of
    the given card_slugs, REGARDLESS of set_name. Used by the language-
    collision guard to detect a cross-set PC-ID collision (e.g. a
    Japanese CSV whose card_slug already exists as an English row in a
    different set). Returns {card_slug: {'card_slug','card_name',
    'set_name','language'}}."""
    out = {}
    slug_list = sorted(slugs)
    chunk = 200
    for i in range(0, len(slug_list), chunk):
        ids = slug_list[i:i + chunk]
        in_list = ",".join([f'"{s}"' for s in ids])
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/cards?card_slug=in.({in_list})"
            f"&select=card_slug,card_name,set_name,language",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            timeout=45,
        )
        if r.status_code == 200:
            for row in r.json():
                out[row["card_slug"]] = row
        else:
            print(f"  WARN: cross-slug fetch failed at chunk {i}: {r.status_code}")
    return out


def classify_language_collisions(csv_cards, existing_by_slug, target_language, allow_reclass):
    """Block 5A-W-48D-FIX1 — pure classifier. Splits the CSV rows into
    three buckets based on existing DB state and the caller's allow-list:

      * `safe`: no existing row, OR existing row's language matches
        target_language. Free to upsert.
      * `allowed_reclass`: existing row's language differs from
        target_language AND the card_slug is in `allow_reclass`. May be
        upserted after explicit human authorisation.
      * `blocked`: existing row's language differs and the card_slug
        is NOT in `allow_reclass`. Must NEVER be upserted — the caller
        is expected to abort the batch (or drop these rows) rather than
        silently reclassify.

    Returns (safe, allowed_reclass, blocked) — each a list of dicts.
    `allowed_reclass` and `blocked` items are enriched with an
    '_existing' key containing the pre-existing row for reporting.

    Pure: no I/O, no globals, no mutation of inputs. Suitable for
    unit testing against a fake `existing_by_slug` mapping."""
    allow_set = set(allow_reclass or ())
    safe, allowed_bucket, blocked = [], [], []
    for card in csv_cards:
        slug = card["card_slug"]
        existing = existing_by_slug.get(slug)
        if not existing or existing.get("language") == target_language:
            safe.append(card)
            continue
        enriched = dict(card)
        enriched["_existing"] = existing
        if slug in allow_set:
            allowed_bucket.append(enriched)
        else:
            blocked.append(enriched)
    return safe, allowed_bucket, blocked


def format_collision_report(blocked, allowed_reclass, target_language):
    """Build the human-readable collision report. Kept separate from
    classify_language_collisions so tests can exercise both."""
    lines = []
    if blocked:
        lines.append(
            f"\nLANGUAGE COLLISION — {len(blocked)} row(s) blocked "
            f"(target language={target_language!r}):"
        )
        for card in blocked:
            ex = card["_existing"]
            lines.append(f"  pc-{card['card_slug']}:")
            lines.append(
                f"    EXISTING: language={ex.get('language')!r}, "
                f"set={ex.get('set_name')!r}, name={ex.get('card_name')!r}"
            )
            lines.append(
                f"    PROPOSED: language={target_language!r}, "
                f"set={card.get('set_name')!r}, name={card.get('card_name')!r}"
            )
        allow_flag = " ".join(sorted({c["card_slug"] for c in blocked}))
        lines.append(
            "\nRefusing to silently reclassify. To allow specific PC IDs, "
            "re-run with:\n"
            f"  --allow-language-reclassification {allow_flag}"
        )
    if allowed_reclass:
        lines.append(
            f"\nExplicit reclassification approved for {len(allowed_reclass)} row(s):"
        )
        for card in allowed_reclass:
            ex = card["_existing"]
            lines.append(
                f"  pc-{card['card_slug']}: language "
                f"{ex.get('language')!r}->{target_language!r}, "
                f"set {ex.get('set_name')!r}->{card.get('set_name')!r}"
            )
    return "\n".join(lines)


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
    p.add_argument("--release-date", default=None,
                   help="YYYY-MM-DD. Optional as of W48D — omit for sets whose "
                        "release date is unverifiable. When omitted, "
                        "set_release_date + release_year are left null on the "
                        "seeded rows.")
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
    p.add_argument("--allow-language-reclassification", nargs="+", default=[],
                   metavar="PC_ID",
                   help="Block 5A-W-48D-FIX1 — explicit override for the "
                        "language-collision guard. Naming a PC ID here "
                        "authorises the seeder to change that ONE existing "
                        "row's language to the current --language value. "
                        "Any collision NOT named in this list still aborts "
                        "the batch. Deliberately narrow: no broad --force "
                        "flag that would allow silent language reclassification "
                        "across an entire file.")
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

    # ── Block 5A-W-48D-FIX1 — language-collision guard ─────────────
    # Fetch every existing cards row that matches any card_slug we are
    # about to write, regardless of set_name. Split into safe / allowed
    # / blocked. Anything BLOCKED aborts the seed for this file. Blocks
    # are the exact protection that would have caught the two W48D
    # en->jp flips (pc-8330138 and pc-8076785) at seed time.
    csv_slug_set = {c["card_slug"] for c in cards}
    existing_full = fetch_existing_cards_by_slug(csv_slug_set) if csv_slug_set else {}
    safe, allowed_reclass, blocked = classify_language_collisions(
        cards, existing_full, args.language,
        args.allow_language_reclassification,
    )
    if blocked or allowed_reclass:
        print(format_collision_report(blocked, allowed_reclass, args.language))
    if blocked and not args.dry_run:
        print(f"\nAborting seed for {args.csv}: {len(blocked)} language collision(s). "
              "No rows written for this file.")
        sys.exit(3)
    if blocked and args.dry_run:
        print(f"\nDRY-RUN: {len(blocked)} language collision(s) would block a real run.")
    # Only the safe rows and the explicitly-allowed reclassifications
    # continue past this point.
    cards = safe + allowed_reclass
    for card in cards:
        card.pop("_existing", None)

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
