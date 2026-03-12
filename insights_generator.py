#!/usr/bin/env python3
"""
PokePrices Insights Generator
Runs daily via GitHub Actions — picks today's theme, pulls live data,
calls Claude Haiku to write the article, inserts into Supabase insights table.
"""

import os
import json
import re
import sys
import random
from datetime import datetime, timezone, timedelta
from typing import Optional

import anthropic
import requests
from supabase import create_client, Client

# ── CONFIG ────────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# Theme rotation by weekday (0=Mon, 6=Sun)
THEMES = {
    0: ("movers",    "The Movers"),
    1: ("grading",   "Grading Desk"),
    2: ("set_watch", "Set Watch"),
    3: ("sleepers",  "Sleeper Picks"),
    4: ("pulse",     "Market Pulse"),
    5: ("collector", "Collector's Corner"),
    6: ("history",   "History Lesson"),
}

# ── DATA FETCHERS ─────────────────────────────────────────────────────────────

def fetch_movers_data() -> dict:
    """Top risers and fallers from card_trends."""
    risers = supabase.rpc("get_top_movers", {"direction": "up", "lim": 8}).execute()
    fallers = supabase.rpc("get_top_movers", {"direction": "down", "lim": 8}).execute()

    # Fallback: query card_trends directly if RPC doesn't exist yet
    if not risers.data:
        risers_raw = (
            supabase.table("card_trends")
            .select("card_slug, card_name, set_name, current_raw, raw_pct_7d, raw_pct_30d")
            .gt("raw_pct_7d", 0)
            .gt("current_raw", 200)  # > $2 raw
            .order("raw_pct_7d", desc=True)
            .limit(8)
            .execute()
        )
        risers_data = risers_raw.data or []
    else:
        risers_data = risers.data

    if not fallers.data:
        fallers_raw = (
            supabase.table("card_trends")
            .select("card_slug, card_name, set_name, current_raw, raw_pct_7d, raw_pct_30d")
            .lt("raw_pct_7d", 0)
            .gt("current_raw", 200)
            .order("raw_pct_7d", desc=False)
            .limit(8)
            .execute()
        )
        fallers_data = fallers_raw.data or []
    else:
        fallers_data = fallers.data

    return {"risers": risers_data, "fallers": fallers_data}


def fetch_grading_data() -> dict:
    """Cards where PSA 10 premium is significant and pop is interesting."""
    # Cards with both raw and PSA 10 prices
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, current_psa10")
        .gt("current_raw", 500)       # > $5 raw
        .gt("current_psa10", 2000)    # > $20 PSA 10
        .order("current_psa10", desc=True)
        .limit(20)
        .execute()
    )
    cards = res.data or []

    # Calculate grade multiplier
    for card in cards:
        raw = card.get("current_raw") or 0
        psa10 = card.get("current_psa10") or 0
        card["grade_multiplier"] = round(psa10 / raw, 2) if raw > 0 else None
        card["raw_usd"] = round(raw / 100, 2)
        card["psa10_usd"] = round(psa10 / 100, 2)

    # Sort by multiplier
    cards.sort(key=lambda x: x.get("grade_multiplier") or 0, reverse=True)

    # Also grab some PSA pop data
    pop_res = (
        supabase.table("psa_population")
        .select("card_name, set_name, psa10_count, psa9_count, total_graded")
        .gt("psa10_count", 0)
        .lt("psa10_count", 300)  # low pop
        .order("psa10_count", desc=False)
        .limit(10)
        .execute()
    )

    return {"premium_cards": cards[:10], "low_pop": pop_res.data or []}


def fetch_set_watch_data() -> dict:
    """Pick the set with the most price movement this week and pull all its card data."""
    # Find most active set by count of cards with significant 7d movement
    res = (
        supabase.table("card_trends")
        .select("set_name, card_slug, card_name, current_raw, raw_pct_7d, raw_pct_30d, raw_pct_90d")
        .not_.is_("raw_pct_7d", "null")
        .gt("current_raw", 100)
        .execute()
    )
    cards = res.data or []

    if not cards:
        return {"set_name": "Unknown", "cards": [], "set_stats": {}}

    # Group by set and find most volatile
    from collections import defaultdict
    set_activity = defaultdict(list)
    for card in cards:
        if card.get("raw_pct_7d") is not None:
            set_activity[card["set_name"]].append(abs(card["raw_pct_7d"]))

    # Pick set with highest average movement and at least 5 moving cards
    best_set = max(
        {k: sum(v) / len(v) for k, v in set_activity.items() if len(v) >= 5}.items(),
        key=lambda x: x[1],
        default=("Scarlet & Violet 151", 0)
    )[0]

    set_cards = [c for c in cards if c["set_name"] == best_set]
    set_cards.sort(key=lambda x: abs(x.get("raw_pct_7d") or 0), reverse=True)

    # Set-level stats
    prices = [c["current_raw"] / 100 for c in set_cards if c.get("current_raw")]
    risers = [c for c in set_cards if (c.get("raw_pct_7d") or 0) > 0]
    fallers = [c for c in set_cards if (c.get("raw_pct_7d") or 0) < 0]

    return {
        "set_name": best_set,
        "cards": set_cards[:15],
        "set_stats": {
            "total_cards": len(set_cards),
            "avg_price_usd": round(sum(prices) / len(prices), 2) if prices else 0,
            "risers_count": len(risers),
            "fallers_count": len(fallers),
        }
    }


def fetch_sleepers_data() -> dict:
    """Hidden gems — rising price, low pop, low awareness."""
    res = supabase.rpc("get_hidden_gems", {"lim": 10}).execute()
    gems = res.data or []

    # Fallback query
    if not gems:
        res2 = (
            supabase.table("card_trends")
            .select("card_slug, card_name, set_name, current_raw, raw_pct_30d, raw_pct_90d")
            .gt("raw_pct_30d", 5)
            .gt("current_raw", 100)
            .lt("current_raw", 5000)  # under $50 — genuinely under the radar
            .order("raw_pct_30d", desc=True)
            .limit(10)
            .execute()
        )
        gems = res2.data or []

    return {"gems": gems}


def fetch_pulse_data() -> dict:
    """Overall market index — macro view of the week."""
    res = (
        supabase.table("market_index")
        .select("date, total_raw_usd, median_raw_usd, raw_pct_7d, raw_pct_30d")
        .order("date", desc=True)
        .limit(35)
        .execute()
    )
    rows = res.data or []
    rows.reverse()  # chronological

    if not rows:
        return {"market_rows": [], "summary": {}}

    latest = rows[-1]
    week_ago = rows[-8] if len(rows) >= 8 else rows[0]

    return {
        "market_rows": rows,
        "summary": {
            "current_total_usd": round((latest.get("total_raw_usd") or 0) / 100, 0),
            "median_card_usd": round((latest.get("median_raw_usd") or 0) / 100, 2),
            "pct_7d": latest.get("raw_pct_7d"),
            "pct_30d": latest.get("raw_pct_30d"),
            "date": latest.get("date"),
        }
    }


def fetch_collector_data() -> dict:
    """Sealed product prices and pack/box value analysis."""
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, raw_pct_30d, raw_pct_90d")
        .gt("current_raw", 1000)  # $10+ cards — viable pulls
        .order("current_raw", desc=True)
        .limit(30)
        .execute()
    )
    cards = res.data or []

    # Group by set for pack EV context
    from collections import defaultdict
    by_set = defaultdict(list)
    for c in cards:
        by_set[c["set_name"]].append(c)

    # Pick set with most valuable pulls
    best_set = max(by_set.items(), key=lambda x: sum(c["current_raw"] for c in x[1]), default=("", []))

    return {
        "set_name": best_set[0],
        "notable_pulls": best_set[1][:10] if best_set[1] else [],
        "all_sets_summary": [
            {
                "set_name": k,
                "top_pull_usd": round(max(c["current_raw"] for c in v) / 100, 2),
                "cards_over_10": len([c for c in v if c["current_raw"] >= 1000]),
            }
            for k, v in list(by_set.items())[:8]
        ]
    }


def fetch_history_data() -> dict:
    """Pick a notable card with long price history and pull its full chart data."""
    # Cards with the most daily_prices entries (longest history)
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, raw_pct_90d, raw_pct_1y")
        .gt("current_raw", 2000)   # $20+ — worth writing about
        .not_.is_("raw_pct_1y", "null")
        .order("current_raw", desc=True)
        .limit(20)
        .execute()
    )
    candidates = res.data or []

    if not candidates:
        return {"card": None, "history": []}

    # Pick a random one from top 10 so articles vary week to week
    card = random.choice(candidates[:10])
    slug = card["card_slug"]

    # Pull full price history
    history_res = (
        supabase.table("daily_prices")
        .select("date, raw_usd")
        .eq("card_slug", slug)
        .order("date", desc=False)
        .limit(365)
        .execute()
    )
    history = history_res.data or []
    for row in history:
        row["price_usd"] = round((row.get("raw_usd") or 0) / 100, 2)

    return {"card": card, "history": history}


# ── PROMPT BUILDER ────────────────────────────────────────────────────────────

def build_prompt(theme: str, theme_label: str, data: dict, today_str: str) -> str:
    data_json = json.dumps(data, indent=2, default=str)

    theme_instructions = {
        "movers": """
Write about the biggest price movers in the Pokémon TCG this week.
Lead with the most dramatic riser. Explain possible reasons (new tournament play, content creator attention, set rotation, low supply).
Cover 3-4 risers and 2-3 fallers in depth. Be analytical, not just descriptive.
The headline should be specific — name the top card and its % move.
SEO focus: card name + "price" + "2026" in title. Target collectors searching for specific card price movements.""",

        "grading": """
Write about grading economics — where the PSA 10 premium is most interesting right now.
Pick 2-3 cards where the data tells an interesting story (high multiplier, low pop, or surprising grade premium).
Give honest grading advice. Don't just say "grade everything" — be specific about when it makes sense.
The headline should frame it as a question or a reveal. E.g. "Is X Worth Grading Right Now?"
SEO focus: "should I grade [card name]", "PSA 10 value", "grading worth it 2026".""",

        "set_watch": """
Write a deep dive on the set identified in the data — its current market position, which cards are moving, and what's driving it.
Cover the set's age, print run context (if inferable), standout cards, and overall trend direction.
Be specific — use the actual card names and prices from the data.
The headline should name the set and hint at something surprising or noteworthy.
SEO focus: "[set name] card prices 2026", "[set name] investment", "best cards from [set name]".""",

        "sleepers": """
Write about cards that are quietly building momentum — the ones serious collectors notice before everyone else.
Be specific about why each card is interesting (low pop, rising trend, undervalued vs grade premium).
Don't oversell — write like a knowledgeable collector talking to another collector, not a hype piece.
The headline should create intrigue without being clickbait. Use specific card names.
SEO focus: "undervalued pokemon cards 2026", "pokemon cards to watch", "sleeper picks TCG".""",

        "pulse": """
Write the week's market overview. What happened to the overall market? Up or down? What drove it?
Connect macro trends to specific cards where possible.
Include a "what to watch next week" section.
Tone: like a weekly market newsletter — informed, direct, no fluff.
The headline should state the market direction and hint at what's behind it.
SEO focus: "pokemon TCG market 2026", "pokemon card prices this week", "TCG market update".""",

        "collector": """
Write about the sealed product side — specifically whether a current set is worth buying sealed vs singles.
Use the pull rates and top card values from the data to calculate approximate pack EV.
Be honest about the maths. Most packs lose money — say so, but explain when sealed still makes sense (nostalgia, sealed grading, long hold).
The headline should frame as a practical question. "Should You Open [Set]? We Did The Maths."
SEO focus: "pokemon booster box worth it 2026", "[set name] pack EV", "should I open pokemon packs".""",

        "history": """
Write a long-term price analysis of the card identified in the data. Use its full price history.
Identify key moments in the chart — peaks, crashes, recoveries. Explain what drove each.
Draw a conclusion about where it sits now relative to its history.
Tone: analytical but accessible. Like a collector who's watched the market for years.
The headline should reference the card, a timeframe, and hint at insight. "X Card, Two Years Later: What the Chart Shows"
SEO focus: "[card name] price history", "[card name] investment", "[card name] value over time".""",
    }

    return f"""You are writing for PokePrices.io — a free Pokémon TCG price intelligence platform.
Tone: knowledgeable collector talking to other collectors. Direct, data-led, no hype, no fluff.
Never use marketing language. Never say "delve", "dive in", "it's worth noting", "in conclusion".
Write like a person, not an AI. Vary sentence length. Use the actual numbers from the data.

Today's date: {today_str}
Theme: {theme_label}

{theme_instructions.get(theme, "")}

DATA:
{data_json}

Return ONLY valid JSON. No markdown fences, no preamble. Exactly this structure:

{{
  "headline": "Attention-grabbing, SEO-optimised headline. Specific. Under 70 chars ideally.",
  "meta_title": "SEO title tag — include card/set name + year. Under 60 chars.",
  "meta_description": "Meta description — 140-160 chars. Include target keyword naturally.",
  "hero_image_query": "3-5 word image search query for the thumbnail. E.g. 'Charizard holo card close up'",
  "intro": "2-3 sentence intro. Hook the reader with the most interesting data point immediately.",
  "slug_suffix": "url-friendly-suffix-no-date e.g. 'umbreon-vmax-price-surge-january'",
  "sections": [
    {{
      "type": "text",
      "content": "Paragraph of article text. 80-150 words. Reference specific cards and prices."
    }},
    {{
      "type": "chart",
      "title": "Chart title",
      "description": "One sentence explaining what this chart shows and why it matters.",
      "card_slug": "pc-XXXXXXX or null if market chart",
      "chart_kind": "line"
    }},
    {{
      "type": "card_grid",
      "heading": "Optional heading for this group of cards",
      "card_slugs": ["pc-XXXXXXX", "pc-XXXXXXX"]
    }},
    {{
      "type": "text",
      "content": "More article text."
    }}
  ],
  "card_refs": ["pc-XXXXXXX"],
  "set_refs": ["Set Name"],
  "tags": ["keyword1", "keyword2", "keyword3"]
}}

Rules:
- sections array: aim for 5-8 sections, mix of text, chart, card_grid
- Use real card_slugs from the data where available (they start with pc-)
- card_grid sections: include 2-6 cards
- chart sections: only include if there is a real card_slug or use null for market index chart
- Every text section must reference specific prices and % changes from the data
- Vary the structure — not every article should follow the same pattern
- Word count target: 400-600 words across all text sections combined
"""


# ── ARTICLE GENERATOR ─────────────────────────────────────────────────────────

def generate_article(theme: str, theme_label: str, data: dict, today: datetime) -> Optional[dict]:
    today_str = today.strftime("%B %d, %Y")
    prompt = build_prompt(theme, theme_label, data, today_str)

    print(f"  Calling Claude Haiku for theme: {theme_label}...")

    try:
        response = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response.content[0].text.strip()

        # Strip markdown fences if model adds them anyway
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        article = json.loads(raw)
        return article

    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        print(f"  Raw response: {raw[:500]}")
        return None
    except Exception as e:
        print(f"  Claude API error: {e}")
        return None


# ── SLUG BUILDER ──────────────────────────────────────────────────────────────

def build_slug(theme: str, suffix: str, today: datetime) -> str:
    date_str = today.strftime("%Y-%m-%d")
    # Clean the suffix
    suffix = re.sub(r"[^a-z0-9-]", "", suffix.lower().replace(" ", "-"))
    suffix = re.sub(r"-+", "-", suffix).strip("-")
    return f"{date_str}-{theme}-{suffix}"


# ── DUPLICATE CHECK ───────────────────────────────────────────────────────────

def already_published_today(theme: str, today: datetime) -> bool:
    start = today.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    res = (
        supabase.table("insights")
        .select("id")
        .eq("theme", theme)
        .gte("published_at", start.isoformat())
        .lt("published_at", end.isoformat())
        .limit(1)
        .execute()
    )
    return bool(res.data)


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    today = datetime.now(timezone.utc)
    weekday = today.weekday()  # 0=Mon
    theme, theme_label = THEMES[weekday]

    print(f"PokePrices Insights Generator")
    print(f"Date: {today.strftime('%A %B %d, %Y')}")
    print(f"Theme: {theme_label} ({theme})")
    print()

    # Check if already run today
    if already_published_today(theme, today):
        print("Article already published for this theme today. Skipping.")
        sys.exit(0)

    # Fetch theme data
    print("Fetching data...")
    fetchers = {
        "movers":    fetch_movers_data,
        "grading":   fetch_grading_data,
        "set_watch": fetch_set_watch_data,
        "sleepers":  fetch_sleepers_data,
        "pulse":     fetch_pulse_data,
        "collector": fetch_collector_data,
        "history":   fetch_history_data,
    }
    data = fetchers[theme]()
    print(f"  Data fetched. Keys: {list(data.keys())}")

    # Generate article
    article = generate_article(theme, theme_label, data, today)
    if not article:
        print("Article generation failed.")
        # Insert error row so we know it ran but failed
        supabase.table("insights").insert({
            "slug": f"{today.strftime('%Y-%m-%d')}-{theme}-error",
            "theme": theme,
            "theme_label": theme_label,
            "published_at": today.isoformat(),
            "headline": f"[Generation failed — {theme_label}]",
            "intro": "",
            "meta_title": "",
            "meta_description": "",
            "hero_image_query": "",
            "body_json": [],
            "card_refs": [],
            "set_refs": [],
            "status": "error",
            "generation_log": "Claude returned invalid JSON or API error",
        }).execute()
        sys.exit(1)

    # Build slug
    slug = build_slug(theme, article.get("slug_suffix", theme), today)
    print(f"  Slug: {slug}")

    # Insert into Supabase
    print("Inserting into Supabase...")
    try:
        supabase.table("insights").insert({
            "slug": slug,
            "theme": theme,
            "theme_label": theme_label,
            "published_at": today.isoformat(),
            "headline": article.get("headline", ""),
            "intro": article.get("intro", ""),
            "meta_title": article.get("meta_title", ""),
            "meta_description": article.get("meta_description", ""),
            "hero_image_query": article.get("hero_image_query", ""),
            "body_json": article.get("sections", []),
            "card_refs": article.get("card_refs", []),
            "set_refs": article.get("set_refs", []),
            "status": "published",
            "generation_log": f"Generated {today.isoformat()}. Tags: {article.get('tags', [])}",
        }).execute()
        print(f"  Inserted successfully.")
        print(f"  Headline: {article.get('headline')}")
    except Exception as e:
        print(f"  Supabase insert error: {e}")
        sys.exit(1)

    print()
    print("Done.")


if __name__ == "__main__":
    main()
