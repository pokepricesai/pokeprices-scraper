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
# Sets with deep, reliable sales history only.
# Modern sets prioritised but key vintage included.

ALLOWED_SETS = {
    # Vintage — iconic sets with strong collector demand and deep data
    "Base Set",
    "Jungle",
    "Fossil",
    "Team Rocket",
    "Neo Genesis",
    "Neo Destiny",
    # Sun & Moon era — transitional, good data
    "Hidden Fates",
    "Cosmic Eclipse",
    "Unified Minds",
    "Team Up",
    "Unbroken Bonds",
    # Sword & Shield era — deep modern data
    "Rebel Clash",
    "Darkness Ablaze",
    "Champion's Path",
    "Vivid Voltage",
    "Shining Fates",
    "Battle Styles",
    "Chilling Reign",
    "Evolving Skies",
    "Celebrations",
    "Fusion Strike",
    "Brilliant Stars",
    "Astral Radiance",
    "Lost Origin",
    "Silver Tempest",
    "Crown Zenith",
    # Scarlet & Violet era — current meta, best data
    "Scarlet & Violet",
    "Paldea Evolved",
    "Obsidian Flames",
    "151",
    "Paradox Rift",
    "Paldean Fates",
    "Temporal Forces",
    "Twilight Masquerade",
    "Stellar Crown",
    "Surging Sparks",
    "Prismatic Evolutions",
}

# ── DATA QUALITY CONSTANTS ────────────────────────────────────────────────────

MIN_RAW_CENTS    = 2000    # $20 minimum raw value — no one cares about cheap commons
MIN_PSA10_CENTS  = 5000    # $50 minimum PSA 10 value for grading content
MAX_PCT_7D       = 100.0   # Cap 7-day moves — above this is likely a data anomaly
MAX_PCT_30D      = 200.0   # Cap 30-day moves — anything higher is almost certainly bad data
MIN_PRICE_POINTS = 30      # Minimum daily_prices entries for a card to be considered reliable


# ── HELPERS ───────────────────────────────────────────────────────────────────

def is_allowed_set(set_name: str) -> bool:
    return set_name in ALLOWED_SETS


def cents_to_dollars(cents) -> float:
    if not cents:
        return 0.0
    return round(int(cents) / 100, 2)


def normalise_card(card: dict) -> dict:
    """Convert price fields from cents to dollars in-place."""
    for field in ("current_raw", "current_psa10"):
        if card.get(field) is not None:
            card[field] = cents_to_dollars(card[field])
    return card


def is_credible_mover(card: dict) -> bool:
    """
    Filter out cards with anomalous percentage moves.
    After normalise_card(), current_raw is already in dollars.
    pct fields are percentages (float), not modified by normalise_card.
    """
    pct_7d  = card.get("raw_pct_7d")
    pct_30d = card.get("raw_pct_30d")

    if pct_7d is not None and abs(pct_7d) > MAX_PCT_7D:
        return False
    if pct_30d is not None and abs(pct_30d) > MAX_PCT_30D:
        return False
    return True


def get_price_point_counts(slugs: list) -> dict:
    """
    Return a dict of {card_slug: count} for how many daily_prices entries each card has.
    Cards with fewer than MIN_PRICE_POINTS are considered unreliable.
    Batches the query to avoid URL length issues.
    """
    if not slugs:
        return {}

    counts = {}
    batch_size = 50
    for i in range(0, len(slugs), batch_size):
        batch = slugs[i:i + batch_size]
        # Use pc- prefixed slugs as stored in daily_prices
        pc_slugs = [s if s.startswith("pc-") else f"pc-{s}" for s in batch]
        res = (
            supabase.table("daily_prices")
            .select("card_slug")
            .in_("card_slug", pc_slugs)
            .execute()
        )
        for row in (res.data or []):
            slug = row["card_slug"]
            counts[slug] = counts.get(slug, 0) + 1

    return counts


def filter_by_data_quality(cards: list, slug_field: str = "card_slug") -> list:
    """
    Remove cards that don't have enough price history to be reliable.
    slug_field is the key in the card dict that holds the card slug.
    """
    if not cards:
        return []

    slugs = [c.get(slug_field, "") for c in cards]
    counts = get_price_point_counts(slugs)

    reliable = []
    for card in cards:
        slug = card.get(slug_field, "")
        pc_slug = slug if slug.startswith("pc-") else f"pc-{slug}"
        if counts.get(pc_slug, 0) >= MIN_PRICE_POINTS:
            reliable.append(card)

    return reliable


# ── DATA FETCHERS ─────────────────────────────────────────────────────────────

def fetch_movers_data() -> dict:
    """Top credible risers and fallers — filtered for data quality and value."""
    risers_res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, raw_pct_7d, raw_pct_30d")
        .gt("raw_pct_7d", 0)
        .gt("current_raw", MIN_RAW_CENTS)
        .lte("raw_pct_7d", MAX_PCT_7D)        # cap anomalies at source
        .lte("raw_pct_30d", MAX_PCT_30D)
        .order("raw_pct_7d", desc=True)
        .limit(60)
        .execute()
    )
    fallers_res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, raw_pct_7d, raw_pct_30d")
        .lt("raw_pct_7d", 0)
        .gt("current_raw", MIN_RAW_CENTS)
        .gte("raw_pct_7d", -MAX_PCT_7D)
        .order("raw_pct_7d", desc=False)
        .limit(60)
        .execute()
    )

    risers_raw = [
        normalise_card(c) for c in (risers_res.data or [])
        if is_allowed_set(c.get("set_name", "")) and is_credible_mover(c)
    ]
    fallers_raw = [
        normalise_card(c) for c in (fallers_res.data or [])
        if is_allowed_set(c.get("set_name", "")) and is_credible_mover(c)
    ]

    risers = filter_by_data_quality(risers_raw)[:8]
    fallers = filter_by_data_quality(fallers_raw)[:6]

    return {"risers": risers, "fallers": fallers}


def fetch_grading_data() -> dict:
    """Cards where the PSA 10 premium tells an interesting story."""
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, current_psa10, raw_pct_30d")
        .gt("current_raw", MIN_RAW_CENTS)
        .gt("current_psa10", MIN_PSA10_CENTS)
        .order("current_psa10", desc=True)
        .limit(80)
        .execute()
    )

    cards = []
    for c in (res.data or []):
        if not is_allowed_set(c.get("set_name", "")):
            continue
        raw_usd   = cents_to_dollars(c.get("current_raw") or 0)
        psa10_usd = cents_to_dollars(c.get("current_psa10") or 0)
        c["current_raw"]       = raw_usd
        c["current_psa10"]     = psa10_usd
        c["grade_multiplier"]  = round(psa10_usd / raw_usd, 2) if raw_usd > 0 else None
        cards.append(c)

    cards.sort(key=lambda x: x.get("grade_multiplier") or 0, reverse=True)
    cards = filter_by_data_quality(cards)[:12]

    # Low-pop PSA data for context
    pop_res = (
        supabase.table("psa_population")
        .select("card_name, set_name, psa10_count, psa9_count, total_graded")
        .gt("psa10_count", 0)
        .lt("psa10_count", 500)
        .order("psa10_count", desc=False)
        .limit(10)
        .execute()
    )

    return {"premium_cards": cards[:10], "low_pop": pop_res.data or []}


def fetch_set_watch_data() -> dict:
    """Pick the most active allowed set this week with credible moves."""
    res = (
        supabase.table("card_trends")
        .select("set_name, card_slug, card_name, current_raw, raw_pct_7d, raw_pct_30d, raw_pct_90d")
        .not_.is_("raw_pct_7d", "null")
        .gt("current_raw", MIN_RAW_CENTS)
        .lte("raw_pct_7d", MAX_PCT_7D)
        .gte("raw_pct_7d", -MAX_PCT_7D)
        .execute()
    )

    all_cards = [
        c for c in (res.data or [])
        if is_allowed_set(c.get("set_name", "")) and is_credible_mover(c)
    ]

    if not all_cards:
        return {"set_name": "Unknown", "cards": [], "set_stats": {}}

    from collections import defaultdict
    set_activity = defaultdict(list)
    for card in all_cards:
        if card.get("raw_pct_7d") is not None:
            set_activity[card["set_name"]].append(abs(card["raw_pct_7d"]))

    # Require at least 8 cards moving to be worth writing about
    scored = {k: sum(v) / len(v) for k, v in set_activity.items() if len(v) >= 8}
    if not scored:
        return {"set_name": "Unknown", "cards": [], "set_stats": {}}

    best_set = max(scored.items(), key=lambda x: x[1])[0]
    set_cards = [normalise_card(c) for c in all_cards if c["set_name"] == best_set]
    set_cards = filter_by_data_quality(set_cards)
    set_cards.sort(key=lambda x: abs(x.get("raw_pct_7d") or 0), reverse=True)

    prices  = [c["current_raw"] for c in set_cards if c.get("current_raw")]
    risers  = [c for c in set_cards if (c.get("raw_pct_7d") or 0) > 0]
    fallers = [c for c in set_cards if (c.get("raw_pct_7d") or 0) < 0]

    return {
        "set_name": best_set,
        "cards": set_cards[:15],
        "set_stats": {
            "total_cards_tracked": len(set_cards),
            "avg_price_usd": round(sum(prices) / len(prices), 2) if prices else 0,
            "risers_count": len(risers),
            "fallers_count": len(fallers),
        }
    }


def fetch_sleepers_data() -> dict:
    """
    Cards with steady multi-month momentum — not spiky anomalies.
    Requires movement across both 30d AND 90d to filter one-off jumps.
    """
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, current_psa10, raw_pct_30d, raw_pct_90d")
        .gt("raw_pct_30d", 5)
        .gt("raw_pct_90d", 5)          # must be moving on both timeframes
        .lte("raw_pct_30d", MAX_PCT_30D)
        .lte("raw_pct_90d", 500)       # 90d cap looser but still guarded
        .gt("current_raw", MIN_RAW_CENTS)
        .lt("current_raw", 30000)      # under $300 raw — genuine sleepers, not established staples
        .order("raw_pct_30d", desc=True)
        .limit(80)
        .execute()
    )

    gems = [
        normalise_card(c) for c in (res.data or [])
        if is_allowed_set(c.get("set_name", ""))
    ]
    gems = filter_by_data_quality(gems)[:10]
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
    total  = latest.get("total_raw_usd") or 0
    median = latest.get("median_raw_usd") or 0
    # market_index stores in cents if values are large
    total_usd  = round(total / 100, 0)  if total  > 1_000_000 else round(float(total), 0)
    median_usd = round(median / 100, 2) if median > 10000     else round(float(median), 2)

    return {
        "market_rows": rows,
        "summary": {
            "current_total_usd": total_usd,
            "median_card_usd":   median_usd,
            "pct_7d":            latest.get("raw_pct_7d"),
            "pct_30d":           latest.get("raw_pct_30d"),
            "date":              latest.get("date"),
        }
    }


def fetch_collector_data() -> dict:
    """Top pull values by set — pack EV context. Only $20+ cards worth discussing."""
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, current_psa10, raw_pct_30d")
        .gt("current_raw", MIN_RAW_CENTS)
        .order("current_raw", desc=True)
        .limit(100)
        .execute()
    )

    cards = [
        normalise_card(c) for c in (res.data or [])
        if is_allowed_set(c.get("set_name", ""))
    ]
    cards = filter_by_data_quality(cards)

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
                "top_pull_usd":      round(max(c["current_raw"] for c in v), 2),
                "cards_over_20_usd": len([c for c in v if c["current_raw"] >= 20.0]),
            }
            for k, v in list(by_set.items())[:8]
        ]
    }


def fetch_history_data() -> dict:
    """Pick a $20+ card from an allowed set with rich price history."""
    res = (
        supabase.table("card_trends")
        .select("card_slug, card_name, set_name, current_raw, current_psa10, raw_pct_90d")
        .gt("current_raw", MIN_RAW_CENTS)
        .not_.is_("raw_pct_90d", "null")
        .order("current_raw", desc=True)
        .limit(80)
        .execute()
    )

    candidates = [
        normalise_card(c) for c in (res.data or [])
        if is_allowed_set(c.get("set_name", ""))
    ]
    candidates = filter_by_data_quality(candidates)

    if not candidates:
        return {"card": None, "history": []}

    card = random.choice(candidates[:15])

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
        "movers": """
Write about the most significant price moves in the Pokemon TCG this week.
Focus on cards with meaningful value — these are $20+ cards that real collectors are watching.
Lead with the strongest riser and explain what might be driving it: tournament results, content attention, supply constraints, rotation.
Cover 3-4 risers and 2-3 fallers. For each, give context — is this a correction after a previous move, a trend reversal, or new momentum?
Be honest about uncertainty. "This could be..." is fine. "This definitely means..." is not.
The article should read like a knowledgeable friend analysing the week, not a price alert digest.
Headline: name the top card and its move. Specific and factual.""",

        "grading": """
Write about where the grading premium is genuinely interesting right now.
Pick 2-3 cards where the PSA 10 multiplier or the pop count tells a real story.
A high multiplier on a card with thin pop is very different from a high multiplier on a widely graded common.
Give honest, practical grading advice. Talk about what a collector actually needs to consider: submission cost, turnaround time, the card's condition sensitivity, whether the raw market is liquid enough to compare against.
Don't recommend grading everything. Be specific about when it makes financial sense and when it doesn't.
The article should feel like advice from someone who has graded a lot of cards and learned what works.
Headline: frame it as a practical question or a data-driven reveal.""",

        "set_watch": """
Write a focused market analysis of the set in the data.
Start by establishing where this set sits in the collecting landscape — its age, print run context if relevant, and how it's been performing over the last few months.
Then drill into the specific cards moving this week. What's driving them? Are the expensive cards leading the set higher or is movement concentrated in mid-range cards?
Give a view on the set's overall trajectory. Is this a set that's found its floor and is building, or one that's been in a longer rally that might be tiring?
Use actual card names and prices throughout. Don't describe data vaguely.
Headline: name the set and hint at the story — something surprising or specific.""",

        "sleepers": """
Write about cards that have been building momentum steadily over weeks, not overnight spikes.
The cards in this data show consistent movement across both 30-day and 90-day windows — that's the signal worth writing about.
For each card, explain what makes it interesting. Is it undervalued relative to its graded equivalent? Is there a scarcity angle? Is it a card from a set that's getting collector attention?
Be measured. Not every sleeper turns into a big move — acknowledge that. But explain why these particular cards are worth watching.
Write like a collector who has done the homework, not like someone hyping picks.
Headline: name specific cards, create intrigue without manufactured excitement.""",

        "pulse": """
Write the week's market overview. Give the big picture first — is the overall market up, down, flat?
Then explain what's behind the movement. Are expensive cards driving the headline number while the mid-market is flat? Or is there broad movement across segments?
Connect macro trends to 2-3 specific cards or sets where the data supports it.
End with a short section on what's worth watching over the next week or two.
Keep the tone of a clear-eyed weekly newsletter. Informed, concise, no padding.""",

        "collector": """
Write about whether the featured set is genuinely worth buying sealed right now versus picking singles.
Do the actual maths with the top cards in the data. Estimate what percentage of packs would need to hit for a box to break even at current prices.
Be direct about what the numbers say. Most sealed product is not a good financial bet — say so when that's the case, but also explain the legitimate reasons collectors still buy it (sealed grading upside, the experience, long-term holds on low-print sets).
Finish with a practical recommendation: singles, sealed, or neither.
Headline: frame it as the practical question every collector faces before buying.""",

        "history": """
Write a long-form price analysis of the card in the data, using its full price history.
Take the reader through the card's price journey chronologically. Identify the key inflection points — peaks, crashes, sustained rallies, extended flat periods — and offer your best explanation for each.
Draw on what you know about the broader hobby context: set releases, grading surges, tournament metas, content creator effects.
Conclude with where the card sits now relative to its own history. Is it at a historically elevated level, a historical low, or somewhere in the middle? What does that mean for collectors considering it?
Tone: analytical and considered. Like someone who has watched this card's price for two years.
Headline: name the card, hint at the insight the history reveals.""",
    }

    return f"""You are writing a market intelligence article for PokePrices.io — a free Pokemon TCG price platform used by serious collectors.

TONE AND STYLE:
- Write like a knowledgeable collector talking to other knowledgeable collectors
- Direct and data-led. Reference actual prices and percentages from the data
- No hype, no marketing language, no manufactured excitement
- Never use: "delve", "dive in", "it's worth noting", "in conclusion", "fascinating", "exciting"
- Vary sentence length. Mix short punchy sentences with longer analytical ones
- Write in flowing paragraphs. The analysis lives in the prose, not in bullet points
- Card grids in the JSON are reference material — the writing should stand on its own

CRITICAL — PRICES:
All prices in the data are already in USD dollars (e.g. 24.50 means $24.50). Use them exactly.
Do NOT multiply or divide any price values.

Today: {today_str}
Theme: {theme_label}

ARTICLE BRIEF:
{theme_instructions.get(theme, "")}

DATA:
{data_json}

Return ONLY a valid JSON object. No markdown fences, no text outside the JSON.

{{
  "headline": "Specific, direct headline. Under 70 chars. Name cards or sets where possible.",
  "meta_title": "SEO title — card or set name + year. Under 60 chars.",
  "meta_description": "140-160 chars. Natural language, target keyword included.",
  "hero_image_query": "3-6 word image search query for the article thumbnail.",
  "intro": "2-3 sentences. Lead with the most compelling data point. Make the reader want to continue.",
  "slug_suffix": "url-friendly-suffix e.g. 'evolving-skies-charizard-vmax-rally'",
  "sections": [
    {{
      "type": "text",
      "content": "Well-written paragraph of analysis. 100-180 words. Specific prices and percentages from the data. Flows naturally from the previous section."
    }},
    {{
      "type": "card_grid",
      "heading": "Short label for this group of cards",
      "card_slugs": ["pc-XXXXXXX", "pc-XXXXXXX"]
    }},
    {{
      "type": "chart",
      "title": "Descriptive chart title",
      "description": "One sentence on what the chart shows and why it matters to the article.",
      "card_slug": "pc-XXXXXXX",
      "chart_kind": "line"
    }}
  ],
  "card_refs": ["pc-XXXXXXX"],
  "set_refs": ["Set Name"],
  "tags": ["keyword1", "keyword2", "keyword3"]
}}

STRUCTURE RULES:
- 5-8 sections total. Mix types but let text sections carry the weight of the analysis
- Open with a text section. Close with a text section
- card_grid sections should follow a text section that introduces those cards
- chart sections: only use real card_slugs from the data
- card_grid: 2-5 cards per grid
- Total word count across all text sections: 450-650 words
- Do not use more than 2 card_grid sections in one article
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
            max_tokens=2500,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response.content[0].text.strip()

        start = raw.find('{')
        end   = raw.rfind('}')
        if start == -1 or end == -1:
            print(f"  No JSON object found in response")
            print(f"  Raw: {raw[:500]}")
            return None

        article = json.loads(raw[start:end + 1])
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
    suffix   = re.sub(r"[^a-z0-9-]", "", suffix.lower().replace(" ", "-"))
    suffix   = re.sub(r"-+", "-", suffix).strip("-")
    return f"{date_str}-{theme}-{suffix}"


# ── DUPLICATE CHECK ───────────────────────────────────────────────────────────

def already_published_today(theme: str, today: datetime) -> bool:
    start = today.replace(hour=0, minute=0, second=0, microsecond=0)
    end   = start + timedelta(days=1)
    res   = (
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
    today   = datetime.now(timezone.utc)
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

    # Bail out early if we got no usable data (e.g. all cards filtered out)
    data_values = list(data.values())
    if all(
        (isinstance(v, list) and len(v) == 0) or
        (isinstance(v, dict) and len(v) == 0) or
        v is None
        for v in data_values
    ):
        print("  No usable data after quality filtering. Skipping article generation.")
        sys.exit(0)

    article = generate_article(theme, theme_label, data, today)
    if not article:
        print("Article generation failed.")
        supabase.table("insights").insert({
            "slug":             f"{today.strftime('%Y-%m-%d')}-{theme}-error",
            "theme":            theme,
            "theme_label":      theme_label,
            "published_at":     today.isoformat(),
            "headline":         f"[Generation failed -- {theme_label}]",
            "intro":            "",
            "meta_title":       "",
            "meta_description": "",
            "hero_image_query": "",
            "body_json":        [],
            "card_refs":        [],
            "set_refs":         [],
            "status":           "error",
            "generation_log":   "Claude returned invalid JSON or API error",
        }).execute()
        sys.exit(1)

    slug = build_slug(theme, article.get("slug_suffix", theme), today)
    print(f"  Slug: {slug}")

    print("Inserting into Supabase...")
    try:
        supabase.table("insights").insert({
            "slug":             slug,
            "theme":            theme,
            "theme_label":      theme_label,
            "published_at":     today.isoformat(),
            "headline":         article.get("headline", ""),
            "intro":            article.get("intro", ""),
            "meta_title":       article.get("meta_title", ""),
            "meta_description": article.get("meta_description", ""),
            "hero_image_query": article.get("hero_image_query", ""),
            "body_json":        article.get("sections", []),
            "card_refs":        article.get("card_refs", []),
            "set_refs":         article.get("set_refs", []),
            "status":           "published",
            "generation_log":   f"Generated {today.isoformat()}. Tags: {article.get('tags', [])}",
        }).execute()
        print(f"  Inserted: {article.get('headline')}")
    except Exception as e:
        print(f"  Supabase insert error: {e}")
        sys.exit(1)

    print()
    print("Done.")


if __name__ == "__main__":
    main()
