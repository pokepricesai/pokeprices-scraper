"""
PokePrices Daily Twitter Automation
Runs daily via GitHub Actions at 9am UK time.
Queries Supabase for real data, generates tweet via Claude, posts via Buffer.
"""

import os
import json
import random
import requests
from datetime import datetime, timezone, timedelta
from supabase import create_client

# ── ENV ───────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
CLAUDE_API_KEY = os.environ["CLAUDE_API_KEY"]
BUFFER_TOKEN = os.environ["BUFFER_ACCESS_TOKEN"]
BUFFER_ORG_ID = os.environ["BUFFER_ORGANIZATION_ID"]
BUFFER_CHANNEL_ID = os.environ["BUFFER_CHANNEL_ID"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://www.pokeprices.io"

# ── TWEET CATEGORIES (rotates by day of week) ─────────────────────────────────
# 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun
CATEGORIES = {
    0: "top_mover",
    1: "psa_pop_insight",
    2: "set_release",
    3: "underpriced_deal",
    4: "grading_tip",
    5: "market_trend",
    6: "data_fact",
}

# ── DATA FETCHERS ─────────────────────────────────────────────────────────────

def get_top_mover():
    """Get the best genuine mover — filters out sealed product and thin volume."""
    try:
        result = supabase.rpc("get_top_risers_filtered", {
            "time_period": "7d",
            "min_price": 2000,   # min $20 raw to filter junk
            "card_filter": None
        }).execute()

        data = result.data
        if isinstance(data, str):
            data = json.loads(data)

        results = data.get("results", []) if isinstance(data, dict) else []

        # Filter: must have real volume, exclude sealed, exclude crazy % gains (single sale)
        filtered = [
            r for r in results
            if r.get("pct_change") is not None
            and 5 <= r["pct_change"] <= 150  # 5-150% — genuine moves only
            and r.get("card_name")
            and not any(x in r["card_name"].lower() for x in [
                "booster box", "etb", "elite trainer", "collection box",
                "tin", "bundle", "pack", "sealed"
            ])
        ]

        if not filtered:
            return None

        card = filtered[0]
        raw_usd = card.get("current_raw", 0) / 100
        raw_gbp = raw_usd / 1.27
        pct = card["pct_change"]

        return {
            "type": "top_mover",
            "card_name": card["card_name"],
            "set_name": card.get("set_name", ""),
            "pct_change": round(pct, 1),
            "raw_usd": round(raw_usd, 2),
            "raw_gbp": round(raw_gbp, 2),
            "period": "7 days",
            "url": f"{BASE_URL}/browse",
        }
    except Exception as e:
        print(f"get_top_mover error: {e}")
        return None


def get_psa_pop_insight():
    """Find a card with interesting PSA population stats."""
    try:
        # Get cards with very low gem rates — interesting rarity angle
        result = supabase.from_("psa_population") \
            .select("card_name, set_name, psa_10, total_graded, gem_rate, card_number") \
            .gt("total_graded", 500) \
            .lt("gem_rate", 3) \
            .gt("psa_10", 10) \
            .order("gem_rate", desc=False) \
            .limit(20) \
            .execute()

        rows = result.data or []
        if not rows:
            return None

        # Pick a random one from top 10 to vary content
        card = random.choice(rows[:10])

        return {
            "type": "psa_pop_insight",
            "card_name": card["card_name"],
            "set_name": card.get("set_name", "").replace("Pokemon ", ""),
            "total_graded": card["total_graded"],
            "psa_10_count": card["psa_10"],
            "gem_rate": round(card["gem_rate"], 2),
            "url": f"{BASE_URL}/browse",
        }
    except Exception as e:
        print(f"get_psa_pop_insight error: {e}")
        return None


def get_set_release():
    """Get upcoming or very recent set release."""
    try:
        today = datetime.now(timezone.utc).date().isoformat()
        # Look 60 days ahead
        future = (datetime.now(timezone.utc) + timedelta(days=60)).date().isoformat()

        result = supabase.from_("release_calendar") \
            .select("*") \
            .gte("release_date", today) \
            .lte("release_date", future) \
            .order("release_date", desc=False) \
            .limit(3) \
            .execute()

        releases = result.data or []

        if not releases:
            # Fall back to recent releases
            recent = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
            result2 = supabase.from_("release_calendar") \
                .select("*") \
                .gte("release_date", recent) \
                .lte("release_date", today) \
                .order("release_date", desc=False) \
                .limit(3) \
                .execute()
            releases = result2.data or []

        if not releases:
            return None

        next_release = releases[0]
        release_date = datetime.fromisoformat(next_release["release_date"])
        days_away = (release_date.date() - datetime.now(timezone.utc).date()).days

        return {
            "type": "set_release",
            "set_name": next_release["set_name"],
            "release_date": release_date.strftime("%d %B %Y"),
            "days_away": days_away,
            "confirmed": next_release.get("confirmed", True),
            "url": f"{BASE_URL}/browse",
            "all_upcoming": [r["set_name"] for r in releases[1:3]],
        }
    except Exception as e:
        print(f"get_set_release error: {e}")
        return None


def get_underpriced_deal():
    """Get a genuine underpriced deal from daily_deals table."""
    try:
        result = supabase.from_("daily_deals") \
            .select("*") \
            .gte("discount_pct", 15) \
            .order("discount_pct", desc=True) \
            .limit(20) \
            .execute()

        deals = result.data or []
        if not deals:
            return None

        # Filter out sealed product and very cheap cards
        filtered = [
            d for d in deals
            if d.get("card_name")
            and d.get("listing_price_cents", 0) > 500  # min $5
            and not any(x in (d.get("card_name") or "").lower() for x in [
                "booster box", "etb", "elite trainer", "tin", "bundle"
            ])
        ]

        if not filtered:
            return None

        deal = filtered[0]
        listing_price = deal.get("listing_price_cents", 0) / 100
        fair_value = deal.get("fair_value_cents", 0) / 100
        listing_gbp = listing_price / 1.27
        url = deal.get("item_web_url") or deal.get("listing_url") or f"{BASE_URL}/browse"

        return {
            "type": "underpriced_deal",
            "card_name": deal["card_name"],
            "set_name": deal.get("set_name", ""),
            "listing_usd": round(listing_price, 2),
            "listing_gbp": round(listing_gbp, 2),
            "fair_value_usd": round(fair_value, 2),
            "discount_pct": round(deal.get("discount_pct", 0), 1),
            "url": url,
        }
    except Exception as e:
        print(f"get_underpriced_deal error: {e}")
        return None


def get_grading_tip():
    """Returns a rotating grading tip — no DB query needed, pure knowledge."""
    tips = [
        {
            "tip": "centering",
            "fact": "PSA 10 requires 60/40 centering front, 75/25 back. A card that looks slightly off-centre can still gem if the back is clean.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "whitening",
            "fact": "Minor corner whitening on the back is one of the most common reasons a card gets PSA 9 instead of 10 — it does NOT mean the card can't grade well. PSA 9 is still excellent.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "graders_uk",
            "fact": "UK grading costs via consolidator: PSA ~£17.50 (45+ days), CGC ~£10 (15 days), ACE ~£12 (days not months). PSA carries the biggest resale premium for vintage.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "gem_rates",
            "fact": "Gem rates by era: vintage 1999-2003 = 1-5%, mid era 2004-2016 = 5-15%, modern 2017+ = 30-60%. Lower gem rate = scarcer PSA 10s = higher premiums.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "psa_vs_cgc",
            "fact": "CGC is cheaper and faster than PSA, but PSA 10s command a bigger resale premium on vintage cards. For modern cards the gap is smaller — CGC is often better value.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "grade_value",
            "fact": "If the PSA 10 is more than 3x the PSA 9 price, the 9 is usually better value. If PSA 9 is less than 2x raw, just buy raw — grading cost doesn't justify the premium.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "fake_detection",
            "fact": "Quick fake card check: hold it to light — real cards have a black inner layer. Check font, holo pattern, and card stock thickness. If the price seems too good to be true, verify the PSA cert at psacard.com.",
            "url": f"{BASE_URL}/browse",
        },
    ]

    # Rotate by day of year so it's consistent but changes daily
    day_of_year = datetime.now().timetuple().tm_yday
    tip = tips[day_of_year % len(tips)]
    return {"type": "grading_tip", **tip}


def get_market_trend():
    """Get overall market trend data."""
    try:
        result = supabase.from_("market_index") \
            .select("*") \
            .order("date", desc=True) \
            .limit(2) \
            .execute()

        rows = result.data or []
        if len(rows) < 2:
            return None

        latest = rows[0]
        prev = rows[1]

        pct_7d = latest.get("raw_pct_7d")
        pct_30d = latest.get("raw_pct_30d")
        cards_tracked = latest.get("total_cards_tracked", 0)
        median_raw = latest.get("median_raw_usd", 0) / 100

        if pct_7d is None:
            return None

        direction = "up" if pct_7d > 0 else "down"

        return {
            "type": "market_trend",
            "pct_7d": round(pct_7d, 1),
            "pct_30d": round(pct_30d, 1) if pct_30d else None,
            "direction": direction,
            "cards_tracked": cards_tracked,
            "median_raw_usd": round(median_raw, 2),
            "url": f"{BASE_URL}/browse",
        }
    except Exception as e:
        print(f"get_market_trend error: {e}")
        return None


def get_data_fact():
    """Interesting data fact — most graded card, rarest gem, etc."""
    try:
        # Most graded card in our PSA population data
        result = supabase.from_("psa_population") \
            .select("card_name, set_name, total_graded, psa_10, gem_rate") \
            .order("total_graded", desc=True) \
            .limit(10) \
            .execute()

        rows = result.data or []
        if not rows:
            return None

        day_of_year = datetime.now().timetuple().tm_yday
        card = rows[day_of_year % len(rows)]

        return {
            "type": "data_fact",
            "card_name": card["card_name"],
            "set_name": card.get("set_name", "").replace("Pokemon ", ""),
            "total_graded": card["total_graded"],
            "psa_10_count": card.get("psa_10", 0),
            "gem_rate": round(card.get("gem_rate", 0), 1),
            "url": f"{BASE_URL}/browse",
        }
    except Exception as e:
        print(f"get_data_fact error: {e}")
        return None


# ── DATA DISPATCHER ───────────────────────────────────────────────────────────

def get_data_for_today():
    """Get the right data based on day of week. Falls back if no data."""
    day = datetime.now(timezone.utc).weekday()
    category = CATEGORIES[day]

    fetchers = {
        "top_mover": get_top_mover,
        "psa_pop_insight": get_psa_pop_insight,
        "set_release": get_set_release,
        "underpriced_deal": get_underpriced_deal,
        "grading_tip": get_grading_tip,
        "market_trend": get_market_trend,
        "data_fact": get_data_fact,
    }

    data = fetchers[category]()

    # Fallback chain if primary fails
    if not data:
        print(f"Primary category {category} returned no data, trying fallbacks")
        fallback_order = ["grading_tip", "market_trend", "psa_pop_insight", "top_mover", "data_fact"]
        for fallback in fallback_order:
            if fallback != category:
                data = fetchers[fallback]()
                if data:
                    print(f"Using fallback: {fallback}")
                    break

    return data


# ── CLAUDE TWEET GENERATOR ────────────────────────────────────────────────────

TWEET_SYSTEM_PROMPT = """You write daily tweets for @pokepricesio — a free Pokemon TCG price intelligence platform for UK collectors.

RULES:
- Max 250 characters (leaving room for URL)
- No hashtags unless they add real value — maximum 2 if used
- No emojis unless genuinely useful — maximum 2
- No promotional language, no "check us out", no "we built"
- Write like a knowledgeable collector sharing a useful fact, not a brand
- Always include the URL provided in the data at the end
- Be specific with numbers — vague tweets get ignored
- Tone: direct, useful, occasionally opinionated. Never corporate.
- Do not start with "Did you know" — it's weak
- Do not use exclamation marks

GOOD examples:
"Umbreon VMAX alt art up 12% in 7 days — now at £1,380 raw. Volume is real: 18 UK sales this month. pokeprices.io/set/Evolving%20Skies"
"PSA 10 gem rate on Base Set Charizard: under 2%. Of 4,200+ graded, only 81 are tens. That's why they command what they do. pokeprices.io"
"Grading tip: minor corner whitening doesn't kill a PSA 9. It's one of the most common reasons cards get 9 instead of 10 — still excellent. pokeprices.io"

BAD examples:
"Check out our amazing price tracker! 🎉🎉 #Pokemon #TCG"
"Did you know Charizard is valuable? Find out more at our website!"
"""

def generate_tweet(data: dict) -> str:
    """Call Claude to generate a tweet from the data."""
    data_str = json.dumps(data, indent=2)

    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "Content-Type": "application/json",
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 200,
            "system": TWEET_SYSTEM_PROMPT,
            "messages": [
                {
                    "role": "user",
                    "content": f"Write a tweet using this data:\n\n{data_str}"
                }
            ],
        },
        timeout=30,
    )

    result = response.json()
    tweet = result["content"][0]["text"].strip()

    # Safety check — truncate if over limit
    if len(tweet) > 280:
        tweet = tweet[:277] + "..."

    return tweet


# ── BUFFER POSTER ─────────────────────────────────────────────────────────────

def post_to_buffer(tweet_text: str) -> bool:
    """Post tweet to Buffer via GraphQL API."""

    # Schedule for 9am UK time tomorrow (or today if run early)
    now_uk = datetime.now(timezone(timedelta(hours=1)))  # BST approximation
    scheduled = now_uk.replace(hour=9, minute=0, second=0, microsecond=0)
    if scheduled <= now_uk:
        scheduled += timedelta(days=1)

    due_at = scheduled.isoformat()

    query = """
    mutation CreatePost($input: CreatePostInput!) {
      createPost(input: $input) {
        ... on Post {
          id
          text
          dueAt
          status
        }
        ... on CoreApiError {
          message
          type
        }
      }
    }
    """

    variables = {
        "input": {
            "channelId": BUFFER_CHANNEL_ID,
            "text": tweet_text,
            "schedulingType": "SCHEDULED",
            "dueAt": due_at,
            "mode": "PUBLISH",
            "aiAssisted": False,
        }
    }

    response = requests.post(
        "https://api.buffer.com/graphql",
        headers={
            "Authorization": f"Bearer {BUFFER_TOKEN}",
            "Content-Type": "application/json",
        },
        json={"query": query, "variables": variables},
        timeout=30,
    )

    result = response.json()
    print(f"Buffer response: {json.dumps(result, indent=2)}")

    if "errors" in result:
        print(f"Buffer GraphQL errors: {result['errors']}")
        return False

    post_data = result.get("data", {}).get("createPost", {})
    if post_data.get("id"):
        print(f"Posted successfully! Buffer post ID: {post_data['id']}")
        print(f"Scheduled for: {post_data.get('dueAt')}")
        return True
    elif post_data.get("message"):
        print(f"Buffer error: {post_data['message']}")
        return False

    return False


def log_to_supabase(tweet_text: str, data: dict, success: bool):
    """Log the tweet attempt to Supabase for tracking."""
    try:
        supabase.from_("twitter_posts").insert({
            "tweet_text": tweet_text,
            "category": data.get("type", "unknown"),
            "data_snapshot": json.dumps(data),
            "posted": success,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        print(f"Logging failed (non-critical): {e}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Running PokePrices Twitter automation — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    # 1. Get data
    data = get_data_for_today()
    if not data:
        print("ERROR: No data available for any category. Exiting.")
        return

    print(f"Category: {data['type']}")
    print(f"Data: {json.dumps(data, indent=2)}")

    # 2. Generate tweet
    tweet = generate_tweet(data)
    print(f"\nGenerated tweet ({len(tweet)} chars):\n{tweet}")

    # 3. Post to Buffer
    success = post_to_buffer(tweet)

    # 4. Log
    log_to_supabase(tweet, data, success)

    if success:
        print("\nDone. Tweet scheduled in Buffer.")
    else:
        print("\nFailed to post to Buffer.")
        exit(1)


if __name__ == "__main__":
    main()
