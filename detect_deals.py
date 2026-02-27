"""
PokePrices Deal Detector v2
============================
Compares eBay listings against PriceCharting fair values to find genuinely
underpriced cards. Only considers listings with HIGH or MEDIUM match confidence
from the eBay scraper to prevent false deals from search mismatches.

Usage:
  python detect_deals.py          # Full run, push to DB
  python detect_deals.py --test   # Show deals without pushing

Environment variables:
  SUPABASE_URL, SUPABASE_KEY
"""

import requests
import re
import os
import sys
from datetime import date

# ============================================
# CONFIGURATION
# ============================================

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://egidpsrkqvymvioidatc.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# ——— Deal thresholds ———
MIN_DISCOUNT_PCT = 15          # At least 15% below fair value
MIN_FAIR_VALUE_CENTS = 1000    # Card worth at least $10
MIN_SELLER_FEEDBACK = 50       # Seller has 50+ feedback
MAX_PRICE_RATIO = 0.85         # Listing ≤ 85% of fair value
MIN_PRICE_RATIO = 0.30         # Listing ≥ 30% of fair value (below = wrong card)

# Only trust these match confidence levels
TRUSTED_CONFIDENCE = ["high", "medium"]

# USD to GBP conversion for comparing UK listings
USD_TO_GBP = 0.79


# ============================================
# DATA LOADING
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


def load_ebay_listings():
    listings = fetch_all(
        "ebay_listings?select=card_slug,marketplace,ebay_item_id,title,"
        "price_cents,currency,shipping_cents,total_cost_cents,condition,"
        "buying_option,seller_username,seller_feedback_score,seller_feedback_pct,"
        "item_web_url,affiliate_url,item_image_url,match_confidence"
    )
    print(f"Loaded {len(listings)} eBay listings")
    return listings


def load_card_trends():
    trends = fetch_all(
        "card_trends?select=card_slug,card_name,set_name,current_raw,current_psa10,current_psa9"
        "&current_raw=not.is.null&current_raw=gt.0"
    )
    trend_map = {t["card_slug"]: t for t in trends}
    print(f"Loaded {len(trend_map)} card trends")
    return trend_map


# ============================================
# DEAL DETECTION
# ============================================

def convert_to_usd_cents(price_cents, currency):
    if currency == "GBP":
        return int(price_cents / USD_TO_GBP)
    return price_cents


def get_fair_value_for_condition(trend, condition_str):
    """Get the right fair value based on the listing condition.
    
    Rules:
      - PSA/CGC/BGS 10        → compare against current_psa10
      - PSA/CGC/BGS 9 or 9.5  → compare against current_psa9
      - PSA/CGC/BGS 1-8        → compare against current_raw (we don't have lower grade prices,
                                  and low-grade slabs are often worth LESS than raw, so raw is
                                  the safest comparison — these should NOT surface as deals)
      - Ungraded               → compare against current_raw
      - Unknown                → compare against current_raw
    """
    condition_str = (condition_str or "").strip()
    condition_lower = condition_str.lower()
    
    # Check if it's graded at all — must contain a grading company name
    grading_companies = ["psa", "cgc", "bgs", "sgc", "ace", "ags", "tag", "gma", "mnt"]
    is_graded = any(co in condition_lower for co in grading_companies) or condition_lower == "graded"
    
    if not is_graded:
        # Ungraded — always use raw price
        raw = trend.get("current_raw", 0)
        return raw, "Raw"
    
    # Extract grade number from condition string like "PSA 10", "CGC 9.5"
    grade_match = re.search(r'(\d+\.?\d*)', condition_str)
    grade_num = float(grade_match.group(1)) if grade_match else None
    
    if grade_num is not None:
        if grade_num >= 10:
            psa10 = trend.get("current_psa10")
            if psa10 and psa10 > 0:
                return psa10, f"PSA 10"
        
        if grade_num >= 9:
            psa9 = trend.get("current_psa9")
            if psa9 and psa9 > 0:
                return psa9, f"PSA 9"
        
        # Grade 1-8: we don't have price data for these grades.
        # Low-grade slabs (PSA 1-5) are often worth LESS than raw near-mint.
        # Using raw as the comparison means these won't falsely appear as deals.
        raw = trend.get("current_raw", 0)
        return raw, f"Raw (no data for grade {grade_num})"
    
    # Graded but no grade number — use raw as conservative fallback
    raw = trend.get("current_raw", 0)
    return raw, "Raw (grade unknown)"


def detect_deals(listings, trend_map):
    deals = []
    stats = {
        "checked": 0,
        "no_trend": 0,
        "low_value": 0,
        "low_confidence": 0,
        "low_feedback": 0,
        "wrong_card": 0,
        "not_cheap": 0,
    }

    for listing in listings:
        card_slug = listing["card_slug"]

        # Must have trend data
        trend = trend_map.get(card_slug)
        if not trend:
            stats["no_trend"] += 1
            continue

        # Only trust high/medium confidence matches
        confidence = listing.get("match_confidence", "none")
        if confidence not in TRUSTED_CONFIDENCE:
            stats["low_confidence"] += 1
            continue

        # Get the right fair value for this condition
        condition = listing.get("condition", "Ungraded")
        fair_value_cents, value_type = get_fair_value_for_condition(trend, condition)
        
        if not fair_value_cents or fair_value_cents < MIN_FAIR_VALUE_CENTS:
            stats["low_value"] += 1
            continue

        # Skip low-grade slabs (PSA 1-6) — these are damaged cards worth less
        # than raw, so comparing against raw creates false "deals"
        grade_match = re.search(r'(\d+\.?\d*)', condition or "")
        if grade_match:
            grade_num = float(grade_match.group(1))
            is_graded = any(co in (condition or "").lower() for co in 
                          ["psa", "cgc", "bgs", "sgc", "ace", "ags"])
            if is_graded and grade_num < 7:
                stats["low_value"] += 1  # reuse this counter
                continue

        # Seller check
        feedback = listing.get("seller_feedback_score") or 0
        if feedback < MIN_SELLER_FEEDBACK:
            stats["low_feedback"] += 1
            continue

        stats["checked"] += 1

        # Convert to USD for comparison
        total_cost_cents = listing.get("total_cost_cents", 0)
        currency = listing.get("currency", "USD")
        total_usd = convert_to_usd_cents(total_cost_cents, currency)

        if total_usd <= 0:
            continue

        price_ratio = total_usd / fair_value_cents

        # Sanity: too cheap = wrong card
        if price_ratio < MIN_PRICE_RATIO:
            stats["wrong_card"] += 1
            continue

        # Not cheap enough = not a deal
        if price_ratio > MAX_PRICE_RATIO:
            stats["not_cheap"] += 1
            continue

        discount_pct = round((1 - price_ratio) * 100, 1)

        deals.append({
            "card_slug": card_slug,
            "card_name": trend.get("card_name"),
            "set_name": trend.get("set_name"),
            "ebay_item_id": listing.get("ebay_item_id"),
            "marketplace": listing.get("marketplace"),
            "listing_price_cents": listing.get("price_cents"),
            "shipping_cents": listing.get("shipping_cents"),
            "total_cost_cents": total_cost_cents,
            "currency": currency,
            "fair_value_cents": fair_value_cents,
            "discount_pct": discount_pct,
            "confidence": confidence,
            "volume_label": None,
            "seller_username": listing.get("seller_username"),
            "seller_feedback_score": feedback,
            "item_web_url": listing.get("item_web_url"),
            "affiliate_url": listing.get("affiliate_url"),
            "item_image_url": listing.get("item_image_url"),
            "condition": condition,
            "detected_at": date.today().isoformat(),
            # Extra context for display
            "_value_type": value_type,
            "_title": listing.get("title", ""),
        })

    deals.sort(key=lambda d: d["discount_pct"], reverse=True)

    print(f"\n{'=' * 60}")
    print(f"DEAL DETECTION RESULTS")
    print(f"{'=' * 60}")
    print(f"Listings checked:           {stats['checked']}")
    print(f"Skipped (no trend data):    {stats['no_trend']}")
    print(f"Skipped (low value):        {stats['low_value']}")
    print(f"Skipped (low confidence):   {stats['low_confidence']}")
    print(f"Skipped (low feedback):     {stats['low_feedback']}")
    print(f"Skipped (wrong card <30%):  {stats['wrong_card']}")
    print(f"Skipped (not cheap enough): {stats['not_cheap']}")
    print(f"DEALS FOUND:                {len(deals)}")
    print(f"{'=' * 60}\n")

    return deals


# ============================================
# OUTPUT
# ============================================

def print_deals(deals, limit=20):
    if not deals:
        print("No deals found today.\n")
        return

    print(f"TOP {min(limit, len(deals))} DEALS:\n")
    for i, deal in enumerate(deals[:limit]):
        sym = "£" if deal["currency"] == "GBP" else "$"
        total = deal["total_cost_cents"] / 100
        fair = deal["fair_value_cents"] / 100
        print(
            f"  {i+1}. {deal['card_name']} ({deal['set_name']})\n"
            f"     {deal['marketplace']}: {sym}{total:.2f} total\n"
            f"     Fair value ({deal['_value_type']}): ${fair:.2f}\n"
            f"     Discount: {deal['discount_pct']}% off | "
            f"Condition: {deal['condition']} | "
            f"Confidence: {deal['confidence']}\n"
            f"     Seller: {deal['seller_username']} ({deal['seller_feedback_score']} feedback)\n"
            f"     eBay title: {deal['_title'][:80]}\n"
        )


def push_deals(deals):
    if not deals:
        return 0

    # Clear today's deals
    today = date.today().isoformat()
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/daily_deals?detected_at=eq.{today}",
        headers={**SUPABASE_HEADERS, "Prefer": "return=minimal"},
        timeout=15,
    )

    # Strip internal fields before pushing
    clean_deals = []
    for d in deals:
        clean = {k: v for k, v in d.items() if not k.startswith("_")}
        clean_deals.append(clean)

    push_headers = {**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates"}
    pushed = 0
    for i in range(0, len(clean_deals), 200):
        batch = clean_deals[i:i + 200]
        try:
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/daily_deals",
                json=batch, headers=push_headers, timeout=30,
            )
            if resp.status_code in (200, 201):
                pushed += len(batch)
            else:
                print(f"  Push error: {resp.status_code} - {resp.text[:200]}")
        except Exception as e:
            print(f"  Push error: {e}")

    print(f"Pushed {pushed} deals to daily_deals table")
    return pushed


# ============================================
# MAIN
# ============================================

def main():
    test_mode = "--test" in sys.argv

    print("=" * 60)
    print("PokePrices Deal Detector v2")
    print("=" * 60 + "\n")

    listings = load_ebay_listings()
    trend_map = load_card_trends()

    if not listings:
        print("No eBay listings. Run ebay_scraper.py first.")
        sys.exit(1)
    if not trend_map:
        print("No card trends. Run nightly scraper first.")
        sys.exit(1)

    deals = detect_deals(listings, trend_map)
    print_deals(deals)

    if test_mode:
        print("TEST MODE: Not pushing to database.")
    else:
        push_deals(deals)

    print("\nDone!")


if __name__ == "__main__":
    main()
