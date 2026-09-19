"""
import_30th_celebration.py
==========================
One-time import of the Pokemon 30th Celebration set into the cards table.
Based on import_pitch_black.py but with a per-row classifier so this set's
four sub-populations (main set, Classic Collection, retailer variants,
RGB Mew, sealed) are handled correctly.

Reads:  pc_csvs/Pokemon 30th Celebration.csv (227 rows)
Writes: cards table (upsert, safe to re-run)

Set facts (verified by Luke 2026-09-19):
  - Release date:                2026-09-16
  - Main-set numbered slots:     128 regular (#001–#128)
  - Main-set secret rares:       29 numbered #129–#158  (denominator STILL 128,
                                 e.g. Alolan Exeggutor #129 renders as "129/128")
  - Total main-set numbered:     158 (128 + 30 secret rares — including one
                                 additional entry that the CSV shape puts at 158
                                 exactly per the expected-broad-source-shape check)
  - Classic Collection subset:   30 reprints with historic numbering (no reliable
                                 shared denominator; card_number_display is NULL,
                                 the site's formatCardNumber fallback renders "#4"
                                 for Charizard #4 etc.)
  - Retailer variants:           3 rows — Mewtwo [Gamestop] #63, Mewtwo [Best Buy]
                                 #63, Eevee [Knockout Collection] #116. Distinct
                                 pc_id (→ distinct card_slug PK) and distinct
                                 card_url_slug (bracketed retailer tag survives
                                 into the slug), so no collision with plain
                                 main-set #63 / #116.
  - RGB Mew promos:              3 rows without a #NN suffix — Mew R/RGB, Mew G/
                                 RGB, Mew B/RGB.
  - Sealed products:             33 rows.

Classification rules (priority-ordered):
  1. PriceCharting id ∈ [14470806, 14470836]           → classic_collection
  2. PriceCharting id ∈ {14470837, 14470838, 14470839} → retailer_variant
  3. product-name contains "#NN"                       → main_set   (Victini #13
                                                        also lands here — the old
                                                        substring-"tin" bug is
                                                        gone; see is_sealed())
  4. product-name matches a WORD-BOUNDARY sealed kw    → sealed     (Battle Deck
                                                        lands here — the keyword
                                                        list now includes "deck")
  5. otherwise                                         → unnumbered_promo (RGB Mew)

Per-class DB shape:
                            card_number    is_sealed    card_number_display
  main_set                  extracted      false        "{n}/128"
  classic_collection        extracted      false        NULL (site fallback → "#{n}")
  retailer_variant          extracted      false        "{n}/128"
  sealed                    NULL           true         NULL
  unnumbered_promo          NULL           false        NULL

set_printed_total is written as "128" on every row — it's a set-level fact and
matches the actual printed denominator on 158/158 main-set cards. The web app's
formatCardNumber uses card_number_display when present (main-set + variants),
and falls back to "#{card_number}" when card_number_display is NULL (Classic
Collection).

Usage (PowerShell, from scraper repo root):
  $env:SUPABASE_KEY = "eyJ..."   # legacy service role JWT — NOT sb_publishable_
  python import_30th_celebration.py            # dry run preview first
  python import_30th_celebration.py --push     # actually push to Supabase
"""

import csv
import os
import re
import sys
import requests

SUPABASE_URL = "https://egidpsrkqvymvioidatc.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

CSV_PATH = "pc_csvs/Pokemon 30th Celebration.csv"
SET_NAME = "30th Celebration"
RELEASE_DATE = "2026-09-16"
PRINTED_TOTAL = "128"

# Classic Collection PriceCharting IDs (verified by Luke 2026-09-19 — 30 rows).
CLASSIC_COLLECTION_MIN_ID = 14470806
CLASSIC_COLLECTION_MAX_ID = 14470836
# Retailer / product variants (3 rows immediately after CC).
RETAILER_VARIANT_IDS = {14470837, 14470838, 14470839}

# Word-boundary sealed keywords. Substring "tin" would catch "Victini";
# word-boundary "\btin\b" catches only real Tin products. "deck" is
# included to catch Battle Deck: Espeon ex / Battle Deck: Umbreon ex.
SEALED_KEYWORDS = [
    "booster", "pack", "box", "blister", "bundle",
    "collection", "tin", "etb", "elite trainer",
    "build & battle", "build and battle", "case", "display", "deck",
]
_SEALED_PATTERNS = [re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE) for kw in SEALED_KEYWORDS]


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


def is_sealed_by_name(product_name):
    """Word-boundary keyword match. Only consulted for rows that do NOT
    have a #NN suffix (any product with a card number is a card, not a
    sealed product)."""
    for pat in _SEALED_PATTERNS:
        if pat.search(product_name):
            return True
    return False


def classify(pc_id_str, product_name):
    """Priority-ordered classifier. Returns one of:
      main_set | classic_collection | retailer_variant | sealed | unnumbered_promo
    """
    try:
        pc_id_int = int(pc_id_str)
    except (TypeError, ValueError):
        pc_id_int = None

    if pc_id_int is not None:
        if CLASSIC_COLLECTION_MIN_ID <= pc_id_int <= CLASSIC_COLLECTION_MAX_ID:
            return "classic_collection"
        if pc_id_int in RETAILER_VARIANT_IDS:
            return "retailer_variant"

    if extract_card_number(product_name) is not None:
        return "main_set"

    if is_sealed_by_name(product_name):
        return "sealed"

    return "unnumbered_promo"


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

            klass = classify(pc_id, product_name)
            card_number = extract_card_number(product_name)

            # Per-class shape.
            if klass == "main_set":
                is_sealed = False
                display = f"{card_number}/{PRINTED_TOTAL}"
            elif klass == "retailer_variant":
                is_sealed = False
                display = f"{card_number}/{PRINTED_TOTAL}" if card_number else None
            elif klass == "classic_collection":
                is_sealed = False
                display = None   # historic numbering — no reliable shared denom
            elif klass == "sealed":
                is_sealed = True
                card_number = None
                display = None
            else:  # unnumbered_promo
                is_sealed = False
                card_number = None
                display = None

            # pc_url — mirrors the scraper's build_url logic (periods/apostrophes stripped)
            console_slug = console_name.lower().replace(" ", "-")
            name_slug = product_name.lower()
            name_slug = re.sub(r"[\[\]#]", "", name_slug)
            name_slug = re.sub(r"[^a-z0-9\s&]", "", name_slug)
            name_slug = name_slug.strip()
            name_slug = re.sub(r"\s+", "-", name_slug)
            name_slug = re.sub(r"-+", "-", name_slug)
            pc_url = f"https://www.pricecharting.com/game/{console_slug}/{name_slug}"

            card_slug_text = make_url_slug(product_name)

            rows.append({
                # DB shape (do not include pc_slug — GENERATED — or legacy url_slug)
                "card_slug": pc_id,
                "card_name": product_name,
                "set_name": SET_NAME,
                "card_number": card_number,
                "pc_url": pc_url,
                "card_url_slug": card_slug_text,
                "is_sealed": is_sealed,
                "card_number_display": display,
                "set_release_date": RELEASE_DATE,
                "set_printed_total": PRINTED_TOTAL,
                # Kept out of the DB payload — internal only, for reporting.
                "_class": klass,
            })
    return rows


def report_classification(rows):
    counts = {}
    for r in rows:
        counts[r["_class"]] = counts.get(r["_class"], 0) + 1
    print("Classification counts:")
    for name in ("main_set", "classic_collection", "retailer_variant", "sealed", "unnumbered_promo"):
        print(f"  {name:22s} {counts.get(name, 0):4d}")
    total = sum(counts.values())
    numbered = sum(1 for r in rows if r["card_number"] is not None)
    unnumbered = total - numbered
    print(f"  {'TOTAL':22s} {total:4d}")
    print(f"  numeric card numbers   {numbered:4d}   (expected 191: 158 main + 30 CC + 3 variants)")
    print(f"  without numeric number {unnumbered:4d}   (expected  36: 33 sealed + 3 RGB Mew)")

    # Expected shape from Luke's spec.
    expected = {
        "main_set": 158, "classic_collection": 30, "retailer_variant": 3,
        "sealed": 33, "unnumbered_promo": 3,
    }
    problems = []
    for k, v in expected.items():
        if counts.get(k, 0) != v:
            problems.append(f"  ✘ {k}: got {counts.get(k, 0)}, expected {v}")
    if numbered != 191:
        problems.append(f"  ✘ numeric-count: got {numbered}, expected 191")
    if unnumbered != 36:
        problems.append(f"  ✘ non-numeric-count: got {unnumbered}, expected 36")
    if problems:
        print("\nEXPECTED-SHAPE MISMATCH:")
        for p in problems:
            print(p)
    else:
        print("\nAll classification counts match Luke's expected shape.")


def sample(rows, predicate, label):
    match = next((r for r in rows if predicate(r)), None)
    if match is None:
        print(f"  {label:38s} NOT FOUND")
        return
    display = match["card_number_display"] if match["card_number_display"] is not None else "NULL"
    print(f"  {label:38s} id={match['card_slug']} class={match['_class']:18s} card_number={str(match['card_number']):>5s} is_sealed={str(match['is_sealed']):5s} display={display}")


def main():
    push = "--push" in sys.argv

    if not os.path.exists(CSV_PATH):
        print(f"ERROR: CSV not found at {CSV_PATH}")
        print("Check the filename matches exactly (including 'Pokemon ' prefix).")
        sys.exit(1)

    rows = build_rows()

    print(f"Parsed {len(rows)} rows from {CSV_PATH}\n")
    report_classification(rows)

    print("\nSample transformations Luke requested:")
    sample(rows, lambda r: r["_class"] == "main_set" and r["card_number"] == "1", "normal card #1 (main set)")
    sample(rows, lambda r: r["_class"] == "main_set" and r["card_number"] == "129", "main-set secret rare #129")
    sample(rows, lambda r: r["_class"] == "main_set" and r["card_number"] == "158", "main-set secret rare #158")
    sample(rows, lambda r: r["card_slug"] == "14470836", "Magikarp CC #203 (id 14470836)")
    sample(rows, lambda r: r["card_slug"] == "14470806", "Charizard CC #4 (id 14470806)")
    sample(rows, lambda r: "R/RGB" in r["card_name"], "Mew R/RGB")
    sample(rows, lambda r: r["card_name"].startswith("Victini"), "Victini #13")
    sample(rows, lambda r: "Espeon" in r["card_name"] and "Battle Deck" in r["card_name"], "Battle Deck: Espeon ex")
    sample(rows, lambda r: r["card_slug"] == "14470837", "Mewtwo [Gamestop] #63")

    # Strip internal-only field before push.
    for r in rows:
        r.pop("_class", None)

    if not push:
        print("\nDRY RUN complete. Re-run with --push to send to Supabase.")
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
