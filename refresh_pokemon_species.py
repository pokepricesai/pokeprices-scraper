"""
refresh_pokemon_species.py
==========================
1. Seeds pokemon_species table from PokeAPI (1025 species) — skips if already seeded
2. Computes card_count + max_raw_usd per species from cards + daily_prices
3. Upserts results into pokemon_species_stats

Run nightly after scraping. Add to GitHub Actions refresh-and-analytics job.
"""

import os
import re
import requests
from datetime import datetime

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}
POST_HEADERS = {**HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"}


def fetch_all(endpoint):
    rows = []
    offset = 0
    while True:
        sep = "&" if "?" in endpoint else "?"
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{endpoint}{sep}offset={offset}&limit=1000",
            headers=HEADERS, timeout=30,
        )
        if r.status_code != 200:
            print(f"  WARN: {endpoint} offset={offset}: {r.status_code}")
            break
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        offset += 1000
        if len(batch) < 1000:
            break
    return rows


def upsert_batch(table, rows, conflict_col="species_name", batch_size=500):
    pushed = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conflict_col}",
            json=batch, headers=POST_HEADERS, timeout=30,
        )
        if r.status_code in (200, 201):
            pushed += len(batch)
        else:
            print(f"  ERROR upserting {table} at {i}: {r.status_code} {r.text[:200]}")
    return pushed


# ── Step 1: Seed pokemon_species if empty ────────────────────────────

print("Checking pokemon_species table...")
existing = fetch_all("pokemon_species?select=id&limit=1")
if not existing:
    print("Seeding pokemon_species from PokeAPI...")
    resp = requests.get("https://pokeapi.co/api/v2/pokemon-species?limit=1025&offset=0", timeout=30)
    species_list = resp.json()["results"]
    rows = [{"id": i + 1, "name": s["name"]} for i, s in enumerate(species_list)]
    pushed = upsert_batch("pokemon_species", rows, conflict_col="id")
    print(f"  Seeded {pushed} species")
else:
    print(f"  Already seeded, skipping")

# ── Step 2: Load all species ──────────────────────────────────────────

print("Loading species list...")
all_species = fetch_all("pokemon_species?select=id,name&order=id.asc")
print(f"  {len(all_species)} species loaded")

# ── Step 3: Load all card names + slugs ──────────────────────────────

# FIX: correct PostgREST filter syntax — was &eq.is_sealed=false (wrong)
print("Loading card names...")
all_cards = fetch_all("cards?select=card_slug,card_name&is_sealed=eq.false")
print(f"  {len(all_cards)} cards loaded")

# ── Step 4: Load latest prices ────────────────────────────────────────

print("Loading latest prices...")
all_prices_raw = fetch_all(
    "daily_prices?select=card_slug,raw_usd&raw_usd=gt.0&order=date.desc"
)
# Deduplicate — keep first (most recent) price per slug
price_map: dict[str, int] = {}
for row in all_prices_raw:
    slug = row["card_slug"]
    if slug not in price_map and row["raw_usd"]:
        price_map[slug] = row["raw_usd"]
print(f"  {len(price_map)} cards with prices")

# ── Step 5: Match species to cards ───────────────────────────────────

print("Matching species to cards...")

# Build regex patterns for each species
# Hyphenated names like "mr-mime" match "mr mime" or "mr-mime" in card names
species_patterns = []
for s in all_species:
    name = s["name"].lower()
    escaped = name.replace("-", "[- ]").replace(".", "\\.")
    pattern = re.compile(rf'(?<![a-z]){escaped}(?![a-z])', re.IGNORECASE)
    species_patterns.append((s["id"], s["name"], pattern))

# card_slug from cards table is bare number e.g. 11069060
# daily_prices uses pc- prefix e.g. pc-11069060
# price_map keys are pc-prefixed, so lookup must add the prefix
card_list = [(c["card_slug"], c["card_name"].lower()) for c in all_cards if c.get("card_name")]

stats: dict[str, dict] = {}

for species_id, species_name, pattern in species_patterns:
    count = 0
    max_price = None
    for card_slug, card_name_lower in card_list:
        if pattern.search(card_name_lower):
            count += 1
            # Add pc- prefix to match daily_prices keys
            price = price_map.get(f"pc-{card_slug}")
            if price and (max_price is None or price > max_price):
                max_price = price
    if count > 0:
        stats[species_name] = {
            "species_name": species_name,
            "species_id":   species_id,
            "card_count":   count,
            "max_raw_usd":  max_price,
            "updated_at":   datetime.utcnow().isoformat(),
        }

print(f"  {len(stats)} species matched to cards")

# ── Step 6: Upsert stats ──────────────────────────────────────────────

print("Upserting pokemon_species_stats...")
rows = list(stats.values())
pushed = upsert_batch("pokemon_species_stats", rows, conflict_col="species_name")
print(f"  Pushed {pushed} rows")
print("refresh_pokemon_species complete!")
