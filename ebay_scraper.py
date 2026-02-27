"""
PokePrices eBay Scraper v3
==========================
Searches eBay for Pokemon card listings, then uses the card matcher to verify
each result against the correct variant in our database.

Flow:
  1. Load top cards by value from card_trends
  2. Search eBay for each card
  3. For each eBay result, load ALL variants of that card from our DB
  4. Use card_matcher to find the BEST matching variant
  5. Store with correct card_slug, match_confidence, and matched_fair_value

This means if we search for "Charizard Gold Star" but eBay returns a plain
Charizard, the matcher will:
  a) Detect the Gold Star variant is missing from the title
  b) Re-match it to the correct plain Charizard in our DB
  c) Compare against the plain Charizard price ($336) not Gold Star ($2,150)

Usage:
  python ebay_scraper.py              # Top 2000 cards
  python ebay_scraper.py --test       # 5 cards only
  python ebay_scraper.py --limit 500  # Custom limit

Environment variables:
  EBAY_APP_ID, EBAY_CERT_ID, SUPABASE_URL, SUPABASE_KEY
"""

import requests
import base64
import re
import os
import sys
import time
from datetime import datetime, timezone

# Import the card matcher
from card_matcher import (
    parse_ebay_title, parse_card_name, parse_card_identity,
    find_best_match, get_fair_value,
    VALUE_VARIANTS, COSMETIC_VARIANTS
)

# ============================================
# CONFIGURATION
# ============================================

EBAY_APP_ID = os.environ.get("EBAY_APP_ID", "")
EBAY_CERT_ID = os.environ.get("EBAY_CERT_ID", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://egidpsrkqvymvioidatc.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))

EBAY_AUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_BROWSE_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
POKEMON_CATEGORY_ID = "183454"
LISTINGS_PER_CARD = 5
REQUEST_DELAY = 0.3
MARKETPLACES = ["EBAY_GB", "EBAY_US"]

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

SEALED_KEYWORDS = [
    "booster box", "booster pack", "blister pack", "elite trainer box",
    "etb", "theme deck", "starter deck", "tin ", "collection box",
    "premium collection", "vstar universe", "build & battle",
    "2-pack blister", "booster bundle", "garchomp c lv", "dialga lv",
    "sylveon collection",
]

JUNK_KEYWORDS = [
    "mystery", "repack", "custom", "proxy", "fake", "replica",
    "lot of", "bundle of", "bulk ", "empty box", "read descrip",
    "keyring", "keychain", "key ring", "key chain",
    "pin badge", "pin ", "sticker", "magnet",
    "playmat", "play mat", "sleeves", "deck box",
    # Foreign language cards — PriceCharting prices are English only
    "italian", "italiano", "ita ", " ita",
    "german", "deutsch", "deu ", " deu", "glurak",
    "french", "français", "francais",
    "spanish", "español", "espanol",
    "japanese", "japan", " jpn", "jpn ",
    "korean", " kor", "kor ",
    "chinese", "china",
    "portuguese", "portugues",
    "dutch", "nederlands",
    "polish", "polski",
]


# ============================================
# EBAY OAUTH
# ============================================

def get_ebay_token():
    if not EBAY_APP_ID or not EBAY_CERT_ID:
        print("ERROR: EBAY_APP_ID and EBAY_CERT_ID required")
        sys.exit(1)
    credentials = base64.b64encode(f"{EBAY_APP_ID}:{EBAY_CERT_ID}".encode()).decode()
    resp = requests.post(EBAY_AUTH_URL, headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {credentials}",
    }, data={
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }, timeout=15)
    if resp.status_code != 200:
        print(f"ERROR: OAuth failed: {resp.status_code} - {resp.text[:300]}")
        sys.exit(1)
    token = resp.json().get("access_token")
    print(f"eBay OAuth token obtained (expires in {resp.json().get('expires_in', '?')}s)")
    return token


# ============================================
# SUPABASE DATA LOADING
# ============================================

def fetch_all(endpoint):
    rows = []
    offset = 0
    while True:
        sep = "&" if "?" in endpoint else "?"
        url = f"{SUPABASE_URL}/rest/v1/{endpoint}{sep}offset={offset}&limit=1000"
        resp = requests.get(url, headers=SUPABASE_HEADERS, timeout=30)
        if resp.status_code != 200:
            break
        batch = resp.json()
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        offset += 1000
        if len(batch) < 1000:
            break
    return rows


def load_top_cards(limit=2000):
    cards = []
    offset = 0
    while len(cards) < limit:
        batch_size = min(1000, limit - len(cards))
        url = (
            f"{SUPABASE_URL}/rest/v1/card_trends"
            f"?select=card_slug,card_name,set_name,current_raw,current_psa10,current_psa9"
            f"&current_raw=not.is.null"
            f"&order=current_raw.desc"
            f"&offset={offset}&limit={batch_size}"
        )
        resp = requests.get(url, headers=SUPABASE_HEADERS, timeout=30)
        if resp.status_code != 200:
            break
        batch = resp.json()
        if not batch:
            break
        cards.extend(batch)
        offset += len(batch)
        if len(batch) < batch_size:
            break
    print(f"Loaded {len(cards)} cards from card_trends")
    return cards


def load_all_card_trends():
    """Load ALL card trends into memory, indexed by base_name + set_name.
    
    This lets us quickly find candidate variants when matching eBay listings.
    e.g. all_by_set["Base Set"] = [Charizard #4, Charizard [1st Ed] #4, ...]
    """
    print("Loading all card trends for matching...")
    all_cards = fetch_all(
        "card_trends?select=card_slug,card_name,set_name,current_raw,current_psa10,current_psa9"
        "&current_raw=not.is.null"
    )
    
    # Index by set_name for fast candidate lookup
    by_set = {}
    for card in all_cards:
        set_name = card.get("set_name", "")
        if set_name not in by_set:
            by_set[set_name] = []
        by_set[set_name].append(card)
    
    print(f"  Indexed {len(all_cards)} cards across {len(by_set)} sets")
    return by_set


def find_candidates(card, all_by_set):
    """Find all card variants that could match an eBay search result.
    
    If we searched for "Charizard [1st Edition] #4" from "Base Set",
    this returns ALL Charizard #4 variants from Base Set:
      - Charizard [1st Edition] #4
      - Charizard [Shadowless] #4  
      - Charizard #4 (unlimited)
      - Charizard [1999-2000] #4
      - Charizard [Black Dot Error] #4
    
    This is the candidate pool the matcher scores against.
    """
    card_name = card.get("card_name", "")
    set_name = card.get("set_name", "")
    
    # Extract base name (without variants/number)
    parsed = parse_card_name(card_name)
    if not parsed:
        return []
    
    base_name_lower = parsed["base_name"].lower()
    
    # Get all cards in this set
    set_cards = all_by_set.get(set_name, [])
    
    # Filter to cards with the same base name
    candidates = []
    for c in set_cards:
        c_parsed = parse_card_name(c.get("card_name", ""))
        if c_parsed and c_parsed["base_name"].lower() == base_name_lower:
            candidates.append(c)
    
    return candidates


# ============================================
# SEARCH QUERY BUILDING
# ============================================

def build_search_query(card_name, set_name):
    """Build eBay search query. Includes important variants for better results."""
    if not card_name:
        return None
    
    identity = parse_card_identity(card_name, set_name)
    if not identity:
        return None
    return identity["search_query"]


# ============================================
# EBAY SEARCH
# ============================================

def search_ebay(token, query, marketplace, limit=5):
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": marketplace,
        "Content-Type": "application/json",
    }
    params = {
        "q": query,
        "category_ids": POKEMON_CATEGORY_ID,
        "limit": limit,
        "sort": "price",
        "filter": "buyingOptions:{FIXED_PRICE}",
    }
    try:
        resp = requests.get(EBAY_BROWSE_URL, headers=headers, params=params, timeout=15)
        if resp.status_code == 429:
            time.sleep(5)
            resp = requests.get(EBAY_BROWSE_URL, headers=headers, params=params, timeout=15)
        if resp.status_code != 200:
            return []
        return resp.json().get("itemSummaries", [])
    except:
        return []


# ============================================
# LISTING PROCESSING
# ============================================

def process_listing(item, original_card, marketplace, candidates):
    """Process a single eBay listing with full matching.
    
    1. Parse the eBay title
    2. Match against ALL candidate variants
    3. Return listing with correct card_slug and fair value
    """
    title = item.get("title", "")
    title_lower = title.lower()
    
    # Skip junk
    if any(kw in title_lower for kw in JUNK_KEYWORDS):
        return None, "junk"
    
    # Parse eBay title
    ebay_parsed = parse_ebay_title(title)
    if not ebay_parsed:
        return None, "parse_fail"
    
    # Find best matching card variant
    best_card, score, breakdown, confidence = find_best_match(title, candidates)
    
    if not best_card or confidence == "none":
        return None, "no_match"
    
    # Get correct fair value for the matched card + grade
    fair_value, value_type = get_fair_value(best_card, ebay_parsed)
    
    # Price
    try:
        price_cents = int(float(item.get("price", {}).get("value", "0")) * 100)
    except (ValueError, TypeError):
        return None, "bad_price"
    
    # Shipping
    shipping_cents = 0
    shipping_options = item.get("shippingOptions", [])
    if shipping_options:
        try:
            shipping_cents = int(float(
                shipping_options[0].get("shippingCost", {}).get("value", "0")
            ) * 100)
        except (ValueError, TypeError):
            pass
    total_cost = price_cents + shipping_cents
    
    # Condition string
    if ebay_parsed["is_graded"] and ebay_parsed["grading_company"] and ebay_parsed["grade_number"]:
        condition_str = f"{ebay_parsed['grading_company']} {ebay_parsed['grade_number']}"
    elif ebay_parsed["is_graded"]:
        condition_str = "Graded"
    else:
        condition_str = "Ungraded"
    
    # Seller
    seller = item.get("seller", {})
    
    # Image
    image = item.get("image", {}).get("imageUrl", "")
    if image:
        image = re.sub(r's-l\d+\.', 's-l500.', image)
    
    # eBay item ID
    item_id = item.get("itemId", "")
    id_parts = item_id.split("|")
    ebay_item_id = id_parts[1] if len(id_parts) >= 2 else item_id
    
    listing = {
        # Use the MATCHED card_slug, not the original search card
        "card_slug": best_card["card_slug"],
        "marketplace": marketplace,
        "ebay_item_id": ebay_item_id,
        "title": title[:500] if title else None,
        "price_cents": price_cents,
        "currency": item.get("price", {}).get("currency", "USD"),
        "shipping_cents": shipping_cents,
        "total_cost_cents": total_cost,
        "condition": condition_str,
        "buying_option": (item.get("buyingOptions") or ["UNKNOWN"])[0],
        "seller_username": seller.get("username", ""),
        "seller_feedback_score": seller.get("feedbackScore"),
        "seller_feedback_pct": seller.get("feedbackPercentage"),
        "seller_country": item.get("itemLocation", {}).get("country", ""),
        "item_image_url": image,
        "item_web_url": item.get("itemWebUrl", ""),
        "affiliate_url": None,
        "listed_date": item.get("itemCreationDate"),
        "match_confidence": confidence,
    }
    
    return listing, confidence


# ============================================
# SUPABASE PUSH
# ============================================

def push_listings_batch(listings):
    if not listings:
        return 0
    pushed = 0
    url = f"{SUPABASE_URL}/rest/v1/ebay_listings?on_conflict=card_slug,marketplace,ebay_item_id"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    for i in range(0, len(listings), 200):
        batch = listings[i:i + 200]
        try:
            resp = requests.post(url, json=batch, headers=headers, timeout=30)
            if resp.status_code in (200, 201):
                pushed += len(batch)
            else:
                print(f"  Supabase push error: {resp.status_code} - {resp.text[:200]}")
        except Exception as e:
            print(f"  Supabase push error: {e}")
    return pushed


def clear_old_listings():
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    try:
        url = f"{SUPABASE_URL}/rest/v1/ebay_listings?scraped_at=lt.{cutoff}"
        resp = requests.delete(url, headers=SUPABASE_HEADERS, timeout=30)
        if resp.status_code in (200, 204):
            print(f"Cleared old listings (before {cutoff[:10]})")
        else:
            print(f"Warning: couldn't clear old listings: {resp.status_code}")
    except:
        pass


# ============================================
# MAIN
# ============================================

def main():
    test_mode = "--test" in sys.argv
    limit = 2000
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            try:
                limit = int(sys.argv[idx + 1])
            except ValueError:
                pass
    if test_mode:
        limit = 5

    print("=" * 70)
    print("PokePrices eBay Scraper v3 (with card matching)")
    print("=" * 70)

    token = get_ebay_token()
    
    # Load cards to search
    cards = load_top_cards(limit)
    if not cards:
        print("No cards found.")
        sys.exit(1)

    # Filter sealed
    original_count = len(cards)
    cards = [
        c for c in cards
        if not any(kw in (c.get("card_name") or "").lower() for kw in SEALED_KEYWORDS)
    ]
    sealed_skipped = original_count - len(cards)
    if sealed_skipped:
        print(f"Skipped {sealed_skipped} sealed products")

    # Load ALL card trends for matching candidates
    all_by_set = load_all_card_trends()

    total_calls = len(cards) * len(MARKETPLACES)
    print(f"\nPlan: {len(cards)} cards × {len(MARKETPLACES)} marketplaces = {total_calls} API calls")
    print(f"API limit: 5,000/day — using {total_calls/5000*100:.0f}%")
    print(f"Est time: ~{total_calls * REQUEST_DELAY / 60:.0f} minutes")
    print("=" * 70 + "\n")

    clear_old_listings()

    all_listings = []
    api_calls = 0
    cards_with_results = 0
    match_stats = {"high": 0, "medium": 0, "low": 0, "none": 0, "junk": 0}
    rematched = 0  # Times a listing matched to a DIFFERENT card than searched

    for i, card in enumerate(cards):
        card_slug = card["card_slug"]
        card_name = card.get("card_name", "")
        set_name = card.get("set_name", "")
        current_raw = card.get("current_raw", 0)

        query = build_search_query(card_name, set_name)
        if not query:
            continue

        # Get ALL variants of this card for matching
        candidates = find_candidates(card, all_by_set)
        if not candidates:
            candidates = [card]  # Fallback: just use the original card

        price_str = f"${current_raw / 100:.2f}" if current_raw else "N/A"
        num_variants = len(candidates)
        print(f"[{i + 1}/{len(cards)}] {card_name} ({set_name}) — PC: {price_str} [{num_variants} variants]")

        card_found_any = False

        for marketplace in MARKETPLACES:
            items = search_ebay(token, query, marketplace, limit=LISTINGS_PER_CARD)
            api_calls += 1

            if items:
                card_found_any = True
                best_conf = "none"
                best_price = None
                rematch_note = ""

                for item in items[:LISTINGS_PER_CARD]:
                    listing, conf = process_listing(item, card, marketplace, candidates)
                    
                    if listing:
                        all_listings.append(listing)
                        match_stats[conf] = match_stats.get(conf, 0) + 1
                        
                        # Track if it matched to a different card than searched
                        if listing["card_slug"] != card_slug:
                            rematched += 1
                            rematch_note = f" → re-matched to {listing['card_slug']}"
                        
                        if conf in ("high", "medium"):
                            if best_conf not in ("high",):
                                best_conf = conf
                                best_price = listing["total_cost_cents"]
                    else:
                        match_stats[conf] = match_stats.get(conf, 0) + 1

                # Log
                cp = items[0].get("price", {})
                cs = items[0].get("shippingOptions", [{}])[0].get("shippingCost", {})
                price_str = f"{cp.get('currency', '?')}{cp.get('value', '?')}"
                ship_str = f"+{cs.get('value', '0')}" if cs.get("value", "0") != "0" else "+free"
                conf_icon = {"high": "✓", "medium": "~", "low": "✗", "none": "✗"}.get(best_conf, "?")
                print(f"    {marketplace}: {price_str} {ship_str} [{best_conf} {conf_icon}]{rematch_note} ({len(items)} results)")
            else:
                print(f"    {marketplace}: no results")

            time.sleep(REQUEST_DELAY)

        if card_found_any:
            cards_with_results += 1

        if len(all_listings) >= 500:
            pushed = push_listings_batch(all_listings)
            print(f"  → Pushed {pushed} listings")
            all_listings = []

    if all_listings:
        pushed = push_listings_batch(all_listings)
        print(f"\n→ Pushed final {pushed} listings")

    print(f"\n{'=' * 70}")
    print(f"EBAY SCRAPE v3 COMPLETE")
    print(f"{'=' * 70}")
    print(f"Cards searched:      {len(cards)}")
    print(f"Cards with results:  {cards_with_results}")
    print(f"API calls made:      {api_calls}")
    print(f"API calls remaining: ~{5000 - api_calls}")
    print(f"Re-matched listings: {rematched} (eBay result matched a different variant)")
    print(f"")
    print(f"Match confidence breakdown:")
    print(f"  HIGH:   {match_stats.get('high', 0)} (correct card, verified)")
    print(f"  MEDIUM: {match_stats.get('medium', 0)} (likely correct)")
    print(f"  LOW:    {match_stats.get('low', 0)} (probably wrong card)")
    print(f"  NONE:   {match_stats.get('none', 0)} (no match)")
    print(f"  JUNK:   {match_stats.get('junk', 0)} (mystery/repack/custom)")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
