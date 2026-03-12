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

# ── SET ALLOWLIST ─────────────────────────────────────────────────────────────
# Mainstream TCG sets only — excludes Topps, promo oddities, Japanese exclusives etc.

ALLOWED_SETS = {
    # Wizards of the Coast era
    "Base Set", "Base Set 2", "Jungle", "Fossil", "Team Rocket",
    "Gym Heroes", "Gym Challenge",
    "Neo Genesis", "Neo Discovery", "Neo Revelation", "Neo Destiny",
    "Legendary Collection",
    "Expedition Base Set", "Aquapolis", "Skyridge",
    # EX era
    "EX Ruby & Sapphire", "EX Sandstorm", "EX Dragon", "EX FireRed & LeafGreen",
    "EX Team Magma vs Team Aqua", "EX Hidden Legends", "EX Emerald",
    "EX Unseen Forces", "EX Delta Species", "EX Legend Maker",
    "EX Holon Phantoms", "EX Crystal Guardians", "EX Dragon Frontiers",
    "EX Power Keepers",
    # Diamond & Pearl era
    "Diamond & Pearl", "Mysterious Treasures", "Secret Wonders",
    "Great Encounters", "Majestic Dawn", "Legends Awakened", "Stormfront",
    "Platinum", "Rising Rivals", "Supreme Victors", "Arceus",
    # HeartGold & SoulSilver era
    "HeartGold & SoulSilver", "Unleashed", "Undaunted", "Triumphant",
    "Call of Legends",
    # Black & White era
    "Black & White", "Emerging Powers", "Noble Victories", "Next Destinies",
    "Dark Explorers", "Dragons Exalted", "Boundaries Crossed",
    "Plasma Storm", "Plasma Freeze", "Plasma Blast", "Legendary Treasures",
    # XY era
    "XY", "Flashfire", "Furious Fists", "Phantom Forces", "Primal Clash",
    "Roaring Skies", "Ancient Origins", "BREAKthrough", "BREAKpoint",
    "Fates Collide", "Steam Siege", "Evolutions",
    # Sun & Moon era
    "Sun & Moon", "Guardians Rising", "Burning Shadows", "Shining Legends",
    "Crimson Invasion", "Ultra Prism", "Forbidden Light", "Celestial Storm",
    "Dragon Majesty", "Lost Thunder", "Team Up", "Unbroken Bonds",
    "Unified Minds", "Hidden Fates", "Cosmic Eclipse",
    # Sword & Shield era
    "Sword & Shield", "Rebel Clash", "Darkness Ablaze", "Champion's Path",
    "Vivid Voltage", "Shining Fates", "Battle Styles", "Chilling Reign",
    "Evolving Skies", "Celebrations", "Fusion Strike", "Brilliant Stars",
    "Astral Radiance", "Pokemon GO", "Lost Origin", "Silver Tempest",
    "Crown Zenith",
    # Scarlet & Violet era
    "Scarlet & Violet", "Paldea Evolved", "Obsidian Flames", "151",
    "Paradox Rift", "Paldean Fates", "Temporal Forces",
    "Twilight Masquerade", "Shrouded Fable", "Stellar Crown", "Surging Sparks",
    "Prismatic Evolutions",
}


def is_allowed_set(set_name: str) -> bool:
    return set_name in ALLOWED_SETS


# ── PRICE HELPERS ─────────────────────────────────────────────────────────────

def cents_to_dollars(cents) -> float:
    """Safely convert cent integer to dollar float."""
    if not cents:
        return 0.0
    return round(int(cents) / 100, 2)


def normalise_card(card: dict) -> dict:
    """Convert price fields from cents to dollars in-place. Returns card."""
    for field in ("current_raw", "current_psa10"):
        if card.get(field) is not None:
            card[field] = cents_to_dollars(card[field])
    return card


# ── DATA FETCHERS ─────────────────────────────────────────────────────────────

def fetch_movers_data() -> dict:
    """Top risers and fallers from card_trends."""
    risers_res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, raw_pct_7d, raw_pct_30d")
        .gt("raw_pct_7d", 0)
        .gt("current_raw", 200)
        .order("raw_pct_7d", desc=True)
        .limit(30)
        .execute()
    )
    fallers_res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, raw_pct_7d, raw_pct_30d")
        .lt("raw_pct_7d", 0)
        .gt("current_raw", 200)
        .order("raw_pct_7d", desc=False)
        .limit(30)
        .execute()
    )

    risers = [normalise_card(c) for c in (risers_res.data or []) if is_allowed_set(c.get("set_name", ""))][:8]
    fallers = [normalise_card(c) for c in (fallers_res.data or []) if is_allowed_set(c.get("set_name", ""))][:8]

    return {"risers": risers, "fallers": fallers}


def fetch_grading_data() -> dict:
    """Cards where PSA 10 premium is significant."""
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, current_psa10")
        .gt("current_raw", 500)
        .gt("current_psa10", 2000)
        .order("current_psa10", desc=True)
        .limit(50)
        .execute()
    )
    cards = []
    for c in (res.data or []):
        if not is_allowed_set(c.get("set_name", "")):
            continue
        raw_usd = cents_to_dollars(c.get("current_raw") or 0)
        psa10_usd = cents_to_dollars(c.get("current_psa10") or 0)
        c["current_raw"] = raw_usd
        c["current_psa10"] = psa10_usd
        c["grade_multiplier"] = round(psa10_usd / raw_usd, 2) if raw_usd > 0 else None
        cards.append(c)

    cards.sort(key=lambda x: x.get("grade_multiplier") or 0, reverse=True)

    pop_res = (
        supabase.table("psa_population")
        .select("card_name, set_name, psa10_count, psa9_count, total_graded")
        .gt("psa10_count", 0)
        .lt("psa10_count", 300)
        .order("psa10_count", desc=False)
        .limit(10)
        .execute()
    )

    return {"premium_cards": cards[:10], "low_pop": pop_res.data or []}


def fetch_set_watch_data() -> dict:
    """Pick the most active allowed set this week."""
    res = (
        supabase.table("card_trends")
        .select("set_name, card_slug, card_name, current_raw, raw_pct_7d, raw_pct_30d, raw_pct_90d")
        .not_.is_("raw_pct_7d", "null")
        .gt("current_raw", 100)
        .execute()
    )
    all_cards = [c for c in (res.data or []) if is_allowed_set(c.get("set_name", ""))]

    if not all_cards:
        return {"set_name": "Unknown", "cards": [], "set_stats": {}}

    from collections import defaultdict
    set_activity = defaultdict(list)
    for card in all_cards:
        if card.get("raw_pct_7d") is not None:
            set_activity[card["set_name"]].append(abs(card["raw_pct_7d"]))

    scored = {k: sum(v) / len(v) for k, v in set_activity.items() if len(v) >= 5}
    best_set = max(scored.items(), key=lambda x: x[1], default=("Scarlet & Violet", 0))[0]

    set_cards = [normalise_card(c) for c in all_cards if c["set_name"] == best_set]
    set_cards.sort(key=lambda x: abs(x.get("raw_pct_7d") or 0), reverse=True)

    prices = [c["current_raw"] for c in set_cards if c.get("current_raw")]
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
    """Hidden gems — rising quietly, mainstream sets only."""
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, raw_pct_30d, raw_pct_90d")
        .gt("raw_pct_30d", 5)
        .gt("current_raw", 200)
        .lt("current_raw", 5000)
        .order("raw_pct_30d", desc=True)
        .limit(50)
        .execute()
    )
    gems = [normalise_card(c) for c in (res.data or []) if is_allowed_set(c.get("set_name", ""))][:10]
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
    rows = list(reversed(res.data or []))

    if not rows:
        return {"market_rows": [], "summary": {}}

    latest = rows[-1]
    total = latest.get("total_raw_usd") or 0
    median = latest.get("median_raw_usd") or 0
    # market_index stores values in cents (large numbers) — detect and convert
    total_usd = round(total / 100, 0) if total > 1_000_000 else round(float(total), 0)
    median_usd = round(median / 100, 2) if median > 10000 else round(float(median), 2)

    return {
        "market_rows": rows,
        "summary": {
            "current_total_usd": total_usd,
            "median_card_usd": median_usd,
            "pct_7d": latest.get("raw_pct_7d"),
            "pct_30d": latest.get("raw_pct_30d"),
            "date": latest.get("date"),
        }
    }


def fetch_collector_data() -> dict:
    """Top pull values by set — pack EV context."""
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, raw_pct_30d, raw_pct_90d")
        .gt("current_raw", 1000)
        .order("current_raw", desc=True)
        .limit(80)
        .execute()
    )
    cards = [normalise_card(c) for c in (res.data or []) if is_allowed_set(c.get("set_name", ""))]

    from collections import defaultdict
    by_set = defaultdict(list)
    for c in cards:
        by_set[c["set_name"]].append(c)

    best_set_name, best_set_cards = max(
        by_set.items(),
        key=lambda x: sum(c["current_raw"] for c in x[1]),
        default=("", [])
    )

    return {
        "set_name": best_set_name,
        "notable_pulls": best_set_cards[:10],
        "all_sets_summary": [
            {
                "set_name": k,
                "top_pull_usd": round(max(c["current_raw"] for c in v), 2),
                "cards_over_10_usd": len([c for c in v if c["current_raw"] >= 10.0]),
            }
            for k, v in list(by_set.items())[:8]
        ]
    }


def fetch_history_data() -> dict:
    """Pick a notable card from an allowed set and pull its full price history."""
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, raw_pct_90d")
        .gt("current_raw", 2000)
        .not_.is_("raw_pct_90d", "null")
        .order("current_raw", desc=True)
        .limit(50)
        .execute()
    )
    candidates = [normalise_card(c) for c in (res.data or []) if is_allowed_set(c.get("set_name", ""))]

    if not candidates:
        return {"card": None, "history": []}

    card = random.choice(candidates[:10])

    history_res = (
        supabase.table("daily_prices")
        .select("date, raw_usd")
        .eq("card_slug", card["card_slug"])
        .order("date", desc=False)
        .limit(365)
        .execute()
    )
    history = [
        {"date": row["date"], "price_usd": cents_to_dollars(row.get("raw_usd") or 0)}
        for row in (history_res.data or [])
    ]

    return {"card": card, "history": history}


# ── PROMPT BUILDER ────────────────────────────────────────────────────────────

def build_prompt(theme: str, theme_label: str, data: dict, today_str: str) -> str:
    data_json = json.dumps(data, indent=2, default=str)

    theme_instructions = {
        "movers": """Write about the biggest price movers in the Pokemon TCG this week.
Lead with the most dramatic riser. Explain possible reasons (tournament play, content creator attention, set rotation, low supply).
Cover 3-4 risers and 2-3 fallers in depth. Be analytical, not just descriptive.
Headline: specific — name the top card and its % move.
SEO: card name + "price" + "2026".""",

        "grading": """Write about grading economics — where the PSA 10 premium is most interesting right now.
Pick 2-3 cards where the data tells an interesting story (high multiplier, low pop, surprising premium).
Give honest grading advice — be specific about when it makes financial sense.
Headline: frame as a question or reveal. E.g. "Is X Worth Grading Right Now?"
SEO: "should I grade [card name]", "PSA 10 value 2026".""",

        "set_watch": """Write a deep dive on the set in the data — current market position, which cards are moving, what's driving it.
Cover the set's age, standout cards, and overall trend direction. Use actual card names and prices from the data.
Headline: name the set and hint at something surprising.
SEO: "[set name] card prices 2026", "best cards from [set name]".""",

        "sleepers": """Write about cards quietly building momentum — ones serious collectors notice before the crowd.
Be specific about why each card is interesting (low pop, rising trend, undervalued vs graded equivalent).
Write like a knowledgeable collector, not a hype piece.
Headline: create intrigue without clickbait. Use specific card names.
SEO: "undervalued pokemon cards 2026", "pokemon cards to watch".""",

        "pulse": """Write the week's market overview. What happened overall? Up or down? What drove it?
Connect macro trends to specific cards where possible. Include a short "what to watch next" section.
Tone: weekly market newsletter — informed, direct, no fluff.
Headline: state the market direction and hint at the cause.
SEO: "pokemon TCG market 2026", "pokemon card prices this week".""",

        "collector": """Write about whether a current set is worth buying sealed vs singles.
Use pull rates and top card values to estimate pack EV. Be honest — most packs lose money, say so.
Explain when sealed still makes sense (nostalgia, sealed grading, long hold).
Headline: practical question. "Should You Open [Set]? The Numbers Say..."
SEO: "pokemon booster box worth it 2026", "[set name] pack EV".""",

        "history": """Write a long-term price analysis of the card in the data using its full price history.
Identify key moments — peaks, crashes, recoveries — and what drove them.
Conclude where it sits now relative to its history.
Tone: analytical. Like a collector who has watched the market for years.
Headline: card name + timeframe + insight hint.
SEO: "[card name] price history", "[card name] value over time".""",
    }

    return f"""You are writing for PokePrices.io — a free Pokemon TCG price intelligence platform.
Tone: knowledgeable collector talking to other collectors. Direct, data-led, no hype, no fluff.
Never use marketing language. Never say "delve", "dive in", "it's worth noting", "in conclusion".
Write like a person, not an AI. Vary sentence length. Use the actual numbers from the data.

CRITICAL: All prices in the data are already in USD dollars (e.g. 10.03 means $10.03, 249.50 means $249.50).
Do NOT multiply or divide prices. Use them exactly as given.

Today: {today_str}
Theme: {theme_label}

{theme_instructions.get(theme, "")}

DATA:
{data_json}

Return ONLY a valid JSON object. No markdown fences, no text before or after the JSON.

{{
  "headline": "Specific, SEO-optimised headline. Under 70 chars.",
  "meta_title": "SEO title tag — card/set name + year. Under 60 chars.",
  "meta_description": "140-160 chars. Target keyword used naturally.",
  "hero_image_query": "3-5 word image search query. E.g. 'Charizard holo card close up'",
  "intro": "2-3 sentences. Hook with the most interesting data point immediately.",
  "slug_suffix": "url-friendly-suffix e.g. 'umbreon-vmax-price-surge'",
  "sections": [
    {{
      "type": "text",
      "content": "Paragraph. 80-150 words. Reference specific cards and prices from the data."
    }},
    {{
      "type": "chart",
      "title": "Chart title",
      "description": "One sentence on what this chart shows and why it matters.",
      "card_slug": "pc-XXXXXXX",
      "chart_kind": "line"
    }},
    {{
      "type": "card_grid",
      "heading": "Heading for this group",
      "card_slugs": ["pc-XXXXXXX", "pc-XXXXXXX"]
    }}
  ],
  "card_refs": ["pc-XXXXXXX"],
  "set_refs": ["Set Name"],
  "tags": ["keyword1", "keyword2", "keyword3"]
}}

Rules:
- 5-8 sections total, mix of text / chart / card_grid
- Use real card_slugs from the data (they start with pc-)
- card_grid: 2-6 cards per grid
- chart: only include if there is a real card_slug from the data
- Every text section must cite specific prices and % changes from the data
- Total word count across all text sections: 400-600 words
"""


# ── ARTICLE GENERATOR ─────────────────────────────────────────────────────────

def generate_article(theme: str, theme_label: str, data: dict, today: datetime) -> Optional[dict]:
    today_str = today.strftime("%B %d, %Y")
    prompt = build_prompt(theme, theme_label, data, today_str)
    raw = ""

    print(f"  Calling Claude Haiku for theme: {theme_label}...")

    try:
        response = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response.content[0].text.strip()

        start = raw.find('{')
        end = raw.rfind('}')
        if start == -1 or end == -1:
            print(f"  No JSON object found in response")
            print(f"  Raw response: {raw[:500]}")
            return None

        article = json.loads(raw[start:end+1])
        return article

    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        print(f"  Raw (first 500): {raw[:500]}")
        return None
    except Exception as e:
        print(f"  Claude API error: {e}")
        return None


# ── SLUG BUILDER ──────────────────────────────────────────────────────────────

def build_slug(theme: str, suffix: str, today: datetime) -> str:
    date_str = today.strftime("%Y-%m-%d")
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
    weekday = today.weekday()
    theme, theme_label = THEMES[weekday]

    print(f"PokePrices Insights Generator")
    print(f"Date: {today.strftime('%A %B %d, %Y')}")
    print(f"Theme: {theme_label} ({theme})")
    print()

    if already_published_today(theme, today):
        print("Article already published for this theme today. Skipping.")
        sys.exit(0)

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

    article = generate_article(theme, theme_label, data, today)
    if not article:
        print("Article generation failed.")
        supabase.table("insights").insert({
            "slug": f"{today.strftime('%Y-%m-%d')}-{theme}-error",
            "theme": theme,
            "theme_label": theme_label,
            "published_at": today.isoformat(),
            "headline": f"[Generation failed -- {theme_label}]",
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

    slug = build_slug(theme, article.get("slug_suffix", theme), today)
    print(f"  Slug: {slug}")

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
        print(f"  Inserted: {article.get('headline')}")
    except Exception as e:
        print(f"  Supabase insert error: {e}")
        sys.exit(1)

    print()
    print("Done.")


if __name__ == "__main__":
    main()
