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
CLAUDE_API_KEY = os.environ["ANTHROPIC_API_KEY"]
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
    """Get the best genuine mover - filters out sealed product and thin volume."""
    try:
        result = supabase.rpc("get_top_risers_filtered", {
            "time_period": "7d",
            "min_price": 2000,
            "card_filter": None
        }).execute()

        data = result.data
        if isinstance(data, str):
            data = json.loads(data)

        results = data.get("results", []) if isinstance(data, dict) else []

        filtered = [
            r for r in results
            if r.get("pct_change") is not None
            and 5 <= r["pct_change"] <= 150
            and r.get("card_name")
            and not any(x in r["card_name"].lower() for x in [
                "booster box", "etb", "elite trainer", "collection box",
                "tin", "bundle", "pack", "sealed"
            ])
        ]

        if not filtered:
            return None

        # Vary which card we pick - not always the top one
        day_of_year = datetime.now().timetuple().tm_yday
        pick = filtered[day_of_year % min(3, len(filtered))]

        raw_usd = pick.get("current_raw", 0) / 100
        raw_gbp = raw_usd / 1.27
        pct = pick["pct_change"]

        return {
            "type": "top_mover",
            "card_name": pick["card_name"],
            "set_name": pick.get("set_name", ""),
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
    """Find a card with interesting PSA population stats - varies the angle."""
    try:
        day_of_year = datetime.now().timetuple().tm_yday
        angle = day_of_year % 3  # rotate between 3 different angles

        if angle == 0:
            # Very low gem rate - rarity angle
            result = supabase.from_("psa_population") \
                .select("card_name, set_name, psa_10, total_graded, gem_rate, card_number") \
                .gt("total_graded", 500) \
                .lt("gem_rate", 3) \
                .gt("psa_10", 10) \
                .order("gem_rate", desc=False) \
                .limit(20) \
                .execute()
            angle_label = "low_gem_rate"
        elif angle == 1:
            # High total graded - most popular grading targets
            result = supabase.from_("psa_population") \
                .select("card_name, set_name, psa_10, total_graded, gem_rate, card_number") \
                .gt("total_graded", 5000) \
                .order("total_graded", desc=True) \
                .limit(20) \
                .execute()
            angle_label = "most_graded"
        else:
            # High gem rate - modern easy grades
            result = supabase.from_("psa_population") \
                .select("card_name, set_name, psa_10, total_graded, gem_rate, card_number") \
                .gt("total_graded", 1000) \
                .gt("gem_rate", 50) \
                .order("gem_rate", desc=True) \
                .limit(20) \
                .execute()
            angle_label = "high_gem_rate"

        rows = result.data or []
        if not rows:
            return None

        card = random.choice(rows[:10])

        return {
            "type": "psa_pop_insight",
            "angle": angle_label,
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

        filtered = [
            d for d in deals
            if d.get("card_name")
            and d.get("listing_price_cents", 0) > 500
            and not any(x in (d.get("card_name") or "").lower() for x in [
                "booster box", "etb", "elite trainer", "tin", "bundle"
            ])
        ]

        if not filtered:
            return None

        # Rotate through top deals for variety
        day_of_year = datetime.now().timetuple().tm_yday
        deal = filtered[day_of_year % min(5, len(filtered))]

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
    """Returns a rotating grading tip - varied angles, human tone."""
    tips = [
        {
            "tip": "centering",
            "fact": "PSA 10 needs 60/40 centering front, 75/25 back. A card that looks slightly off-centre can still gem if the back is tight. Always check both sides before deciding not to submit.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "whitening",
            "fact": "Minor corner whitening on the back is the most common reason cards get PSA 9 instead of 10. It does not prevent a 9. Factory whitening from packs grades 9 all the time.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "graders_uk",
            "fact": "UK grading costs via consolidator: PSA around 17.50 pounds (45+ days), CGC around 10 pounds (15 days), ACE around 12 pounds (days not months). PSA carries the biggest resale premium on vintage.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "gem_rates",
            "fact": "Gem rates by era: vintage 1999-2003 is 1-5%, mid era 2004-2016 is 5-15%, modern 2017+ is 30-60%. Lower gem rate means scarcer PSA 10s and higher premiums when they do exist.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "psa_vs_cgc",
            "fact": "CGC is cheaper and faster than PSA but PSA 10s command a bigger resale premium on vintage. For modern the gap is smaller. Match your grader to your exit strategy.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "grade_value",
            "fact": "If the PSA 10 is more than 3x the PSA 9 price, the 9 is usually better value. If PSA 9 is under 2x raw, just buy raw. Grading only makes sense when the premium justifies the cost and wait.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "fake_detection",
            "fact": "Quick fake card check: hold to light and look for the black inner layer. Real cards have it, fakes usually do not. Also check font consistency, holo pattern, and card stock thickness.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "what_to_grade",
            "fact": "Not everything is worth grading. Rule of thumb: the PSA 10 sale price needs to exceed raw price plus grading cost plus your time. For most modern bulk, it does not.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "surface_scratches",
            "fact": "Holo scratches are one of the hardest things to see under normal light. Check your holos at an angle under a bright lamp before submitting. What looks mint flat often has scratches when angled.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "pack_fresh",
            "fact": "Pack fresh does not mean PSA 10. Base Set cards came out of packs with print lines, whitening, and centering issues. Vintage gem rates are low for a reason.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "ace_grader",
            "fact": "ACE Grading is the fastest option in the UK right now - turnaround in days not months, around 12 pounds via consolidator. Growing resale acceptance but PSA still dominates the premium end.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "buy_or_grade",
            "fact": "Buying a PSA 9 is often smarter than grading raw. You skip the risk of getting an 8, the wait, and the cost. Only grade yourself if you have high confidence in the card condition.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "shadowless",
            "fact": "Shadowless Base Set cards have no drop shadow on the right side of the artwork box. They are rarer than unlimited but not 1st Edition. The stamp is what makes a 1st Ed - shadowless without it is still unlimited print run.",
            "url": f"{BASE_URL}/browse",
        },
        {
            "tip": "japanese_grading",
            "fact": "Japanese cards have better print quality and gem at higher rates than English equivalents. If you are grading for a collection rather than UK resale, Japanese PSA 10s are easier and cheaper to achieve.",
            "url": f"{BASE_URL}/browse",
        },
    ]

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
    """Interesting data fact - rotates between different angles."""
    try:
        day_of_year = datetime.now().timetuple().tm_yday
        angle = day_of_year % 3

        if angle == 0:
            # Most graded cards
            result = supabase.from_("psa_population") \
                .select("card_name, set_name, total_graded, psa_10, gem_rate") \
                .order("total_graded", desc=True) \
                .limit(10) \
                .execute()
            fact_type = "most_graded"
        elif angle == 1:
            # Cards with most PSA 10s
            result = supabase.from_("psa_population") \
                .select("card_name, set_name, total_graded, psa_10, gem_rate") \
                .gt("psa_10", 100) \
                .order("psa_10", desc=True) \
                .limit(10) \
                .execute()
            fact_type = "most_tens"
        else:
            # Cards with fewest PSA 10s despite high submission volume
            result = supabase.from_("psa_population") \
                .select("card_name, set_name, total_graded, psa_10, gem_rate") \
                .gt("total_graded", 1000) \
                .lt("psa_10", 20) \
                .order("total_graded", desc=True) \
                .limit(10) \
                .execute()
            fact_type = "fewest_tens"

        rows = result.data or []
        if not rows:
            return None

        card = rows[day_of_year % len(rows)]

        return {
            "type": "data_fact",
            "fact_type": fact_type,
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

TONE AND STYLE:
- Sound like a knowledgeable collector, not a brand or a bot
- Direct and confident. Occasionally opinionated. Never corporate.
- Vary your sentence structure — do not always lead with the card name
- Sometimes start with the insight, sometimes with a question, sometimes with a stat
- Human voice — someone who actually collects and cares about this stuff

HARD RULES:
- Max 250 characters excluding the URL
- Always include the URL from the data at the very end on its own line
- No hashtags at all — they look spammy
- No emojis unless one genuinely adds meaning (max 1 if used)
- No exclamation marks
- Never start with "Did you know"
- No promotional language ("check us out", "we built", "our platform")
- Be specific — exact numbers beat vague claims every time
- Never repeat the same tweet structure two days in a row

VARIED OPENINGS — mix these up:
- Start with a number: "Only 47 PSA 10s exist for..."
- Start with a question: "Why is Chilling Reign holding better than Vivid Voltage?"
- Start with a fact: "Gem rate on Base Set Charizard is under 2%..."
- Start with an observation: "The gap between PSA 9 and PSA 10 on Moonbreon is narrowing..."
- Start with advice: "If you are grading Base Set, check your centering on the back first..."

GOOD tweet examples:
"Only 81 PSA 10s exist for Base Set Charizard out of 4,200+ graded. That 1.9% gem rate is why they hold value the way they do. pokeprices.io"
"Umbreon VMAX alt art up 12% this week — now at £1,380 raw. 18 sales in 30 days, so this is real demand not a single spike. pokeprices.io"
"Minor corner whitening does not kill a PSA 9. It is the most common reason cards get 9 instead of 10. Do not talk yourself out of submitting. pokeprices.io"
"First Partner Illustration Collection drops in 2 days. Singles will spike then correct — wait 6-8 weeks after release to buy at better prices. pokeprices.io"
"The overall market is up 8.2% in 7 days across 38,000+ tracked cards. Broad moves like this usually mean institutional buying, not retail hype. pokeprices.io"

BAD tweet examples:
"Check out our amazing price tracker! Great deals available now!"
"Did you know Charizard is one of the most valuable cards? #Pokemon #TCG"
"We have data on 40000 cards. Visit our website to learn more!"
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
                    "content": f"Write a tweet using this data. Make it feel human and vary the structure from yesterday. Data:\n\n{data_str}"
                }
            ],
        },
        timeout=30,
    )

    result = response.json()

    if "content" not in result:
        print(f"Claude API error response: {result}")
        raise Exception(f"Claude API failed: {result.get('error', result)}")

    tweet = result["content"][0]["text"].strip()

    if len(tweet) > 280:
        tweet = tweet[:277] + "..."

    return tweet


# ── BUFFER POSTER ─────────────────────────────────────────────────────────────

def post_to_buffer(tweet_text: str) -> bool:
    """Post tweet to Buffer via GraphQL API."""

    # Schedule for tomorrow at 9am UTC
    now_utc = datetime.now(timezone.utc)
    scheduled = (now_utc + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    # Format as ISO 8601 with Z suffix - what Buffer expects
    due_at = scheduled.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"Scheduling for: {due_at}")

    query = """
    mutation CreatePost($input: CreatePostInput!) {
      createPost(input: $input) {
        __typename
      }
    }
    """

    variables = {
        "input": {
            "channelId": BUFFER_CHANNEL_ID,
            "text": tweet_text,
            "schedulingType": "automatic",
            "dueAt": due_at,
            "mode": "shareNow",
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

    if "errors" not in result and result.get("data", {}).get("createPost") is not None:
        typename = result["data"]["createPost"].get("__typename", "unknown")
        if "Error" in typename or "error" in typename.lower():
            print(f"Buffer returned error type: {typename}")
            print(f"Full response: {result}")
            return False
        print(f"Posted successfully. Buffer response type: {typename}")
        return True

    print(f"Buffer did not confirm success: {result}")
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

    data = get_data_for_today()
    if not data:
        print("ERROR: No data available for any category. Exiting.")
        return

    print(f"Category: {data['type']}")
    print(f"Data: {json.dumps(data, indent=2)}")

    tweet = generate_tweet(data)
    print(f"\nGenerated tweet ({len(tweet)} chars):\n{tweet}")

    success = post_to_buffer(tweet)

    log_to_supabase(tweet, data, success)

    if success:
        print("\nDone. Tweet scheduled in Buffer.")
    else:
        print("\nFailed to post to Buffer.")
        exit(1)


if __name__ == "__main__":
    main()
