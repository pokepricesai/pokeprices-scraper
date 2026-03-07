"""
nightly_analytics.py
Runs after the main price scraper.
Adds to: market_index, card_scores, set_scores

Add to GitHub Actions workflow AFTER the main scraper step:
  - name: Run analytics
    run: python nightly_analytics.py
"""

import os
import math
from datetime import date, timedelta
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
TODAY = date.today().isoformat()


# ── 1. MARKET INDEX ──────────────────────────────────────────

def update_market_index():
    print("Updating market_index...")

    # Fetch today's prices
    result = supabase.table("daily_prices").select(
        "card_slug, raw_usd, psa9_usd, psa10_usd"
    ).eq("date", TODAY).execute()

    rows = result.data or []
    if not rows:
        print("  No price data for today — skipping market_index")
        return

    raw_prices  = [r["raw_usd"]   for r in rows if r.get("raw_usd")   and r["raw_usd"]   > 0]
    psa10_prices = [r["psa10_usd"] for r in rows if r.get("psa10_usd") and r["psa10_usd"] > 0]

    def median(lst):
        if not lst: return None
        s = sorted(lst)
        n = len(s)
        return (s[n // 2] + s[(n - 1) // 2]) // 2

    combined_total = sum(
        (r.get("raw_usd") or 0) +
        (r.get("psa9_usd") or 0) +
        (r.get("psa10_usd") or 0)
        for r in rows
    )

    record = {
        "date":                      TODAY,
        "total_raw_usd":             sum(raw_prices),
        "cards_with_raw_price":      len(raw_prices),
        "median_raw_usd":            median(raw_prices),
        "total_psa10_usd":           sum(psa10_prices),
        "cards_with_psa10_price":    len(psa10_prices),
        "median_psa10_usd":          median(psa10_prices),
        "total_combined_usd":        combined_total,
        "total_cards_tracked":       len(rows),
    }

    # Compute pct changes
    for days, key in [(1, "raw_pct_1d"), (7, "raw_pct_7d"), (30, "raw_pct_30d")]:
        past_date = (date.today() - timedelta(days=days)).isoformat()
        past = supabase.table("market_index").select("total_raw_usd").eq("date", past_date).execute()
        if past.data and past.data[0].get("total_raw_usd"):
            past_val = past.data[0]["total_raw_usd"]
            if past_val > 0:
                record[key] = round((record["total_raw_usd"] - past_val) / past_val * 100, 4)

    supabase.table("market_index").upsert(record, on_conflict="date").execute()
    print(f"  market_index updated: {len(rows)} cards, total raw ${sum(raw_prices)/100:,.0f}")


# ── 2. CARD SCORES ───────────────────────────────────────────

def compute_liquidity_score(sales_30d, days_since_last_sale):
    """0-100 liquidity score"""
    sales_score = min(60, (sales_30d or 0) * 10)
    dsls = days_since_last_sale or 999
    if dsls <= 7:   fresh = 40
    elif dsls <= 14: fresh = 30
    elif dsls <= 30: fresh = 20
    elif dsls <= 60: fresh = 10
    else:            fresh = 0
    return min(100, sales_score + fresh)

def compute_momentum_score(pct_7d, pct_30d, pct_90d):
    """0-100 momentum score centred at 50"""
    weighted = (
        (pct_7d  or 0) * 0.20 +
        (pct_30d or 0) * 0.50 +
        (pct_90d or 0) * 0.30
    )
    return max(0, min(100, round(weighted + 50)))

def compute_volatility_score(volatility_30d):
    """0-100 where 100 = most volatile"""
    if volatility_30d is None: return 50
    return max(0, min(100, round(volatility_30d * 200)))

def liquidity_label(score):
    if score >= 70: return "High"
    if score >= 35: return "Medium"
    return "Low"

def volatility_label(v30d):
    if v30d is None:  return "Unknown"
    if v30d > 0.3:   return "Extreme"
    if v30d > 0.15:  return "Volatile"
    if v30d > 0.06:  return "Moderate"
    return "Stable"

def update_card_scores():
    print("Updating card_scores...")

    # Pull today's metrics_daily (raw grade only for base scores)
    metrics_raw = supabase.table("metrics_daily").select(
        "card_slug, current_price, ath_price, ath_date, drawdown_pct, "
        "bottom_price, bottom_date, recovery_pct, "
        "pct_7d, pct_30d, pct_90d, volatility_30d, confidence, "
        "data_points_90d, freshness_days"
    ).eq("grade", "raw").eq("as_of", TODAY).execute()

    if not metrics_raw.data:
        print("  No metrics_daily data for today")
        return

    # Pull card_volume (Ungraded)
    volume = supabase.table("card_volume").select(
        "card_slug, sales_30d, days_since_last_sale, volume_label, confidence"
    ).eq("grade", "Ungraded").execute()
    vol_map = {r["card_slug"]: r for r in (volume.data or [])}

    # Pull spread_daily
    spread = supabase.table("spread_daily").select(
        "card_slug, ratio_10_to_raw, best_value_grade, premium_10_trend"
    ).eq("as_of", TODAY).execute()
    spread_map = {r["card_slug"]: r for r in (spread.data or [])}

    # Pull card names from card_trends
    trends = supabase.table("card_trends").select(
        "card_slug, card_name, set_name"
    ).execute()
    trend_map = {r["card_slug"]: r for r in (trends.data or [])}

    # Pull psa10 pop counts
    pop = supabase.table("card_population").select(
        "card_slug, population"
    ).eq("grade", "psa10").execute()
    pop_map = {r["card_slug"]: r["population"] for r in (pop.data or [])}

    scores = []
    for m in metrics_raw.data:
        slug = m["card_slug"]
        vol  = vol_map.get(slug, {})
        sp   = spread_map.get(slug, {})
        tr   = trend_map.get(slug, {})

        liq_score  = compute_liquidity_score(vol.get("sales_30d"), vol.get("days_since_last_sale"))
        mom_score  = compute_momentum_score(m.get("pct_7d"), m.get("pct_30d"), m.get("pct_90d"))
        volt_score = compute_volatility_score(m.get("volatility_30d"))

        # Confidence score
        conf = m.get("confidence", "low")
        conf_score = {"high": 90, "medium": 60}.get(conf, 25)

        # Hidden gem: rising + low pop + low listings (approximate listing count from card_volume)
        psa10_pop = pop_map.get(slug, 9999)
        sales_30d = vol.get("sales_30d") or 0
        pct_30d   = m.get("pct_30d") or 0
        gem_score = max(0, min(100, (
            min(40, max(0, int(pct_30d) + 20)) +
            min(30, max(0, 30 - psa10_pop // 10)) +
            min(20, sales_30d * 5) +
            (10 if conf == "high" else 0)
        )))

        scores.append({
            "card_slug":         slug,
            "card_name":         tr.get("card_name"),
            "set_name":          tr.get("set_name"),
            "liquidity_score":   liq_score,
            "volatility_score":  volt_score,
            "momentum_score":    mom_score,
            "confidence_score":  conf_score,
            "liquidity_label":   liquidity_label(liq_score),
            "volatility_label":  volatility_label(m.get("volatility_30d")),
            "ath_price":         m.get("ath_price"),
            "ath_date":          m.get("ath_date"),
            "current_price":     m.get("current_price"),
            "drawdown_pct":      float(m["drawdown_pct"]) if m.get("drawdown_pct") else None,
            "bottom_price":      m.get("bottom_price"),
            "recovery_pct":      float(m["recovery_pct"]) if m.get("recovery_pct") else None,
            "best_value_grade":  sp.get("best_value_grade"),
            "ratio_10_to_raw":   float(sp["ratio_10_to_raw"]) if sp.get("ratio_10_to_raw") else None,
            "premium_10_trend":  sp.get("premium_10_trend"),
            "hidden_gem_score":  gem_score,
            "as_of":             TODAY,
        })

    # Batch upsert in chunks of 500
    chunk_size = 500
    for i in range(0, len(scores), chunk_size):
        chunk = scores[i:i + chunk_size]
        supabase.table("card_scores").upsert(chunk, on_conflict="card_slug").execute()

    print(f"  card_scores updated: {len(scores)} cards")


# ── 3. SET SCORES ────────────────────────────────────────────

def update_set_scores():
    print("Updating set_scores...")

    result = supabase.table("set_metrics_daily").select("*").eq(
        "as_of", (date.today() - timedelta(days=1)).isoformat()  # use yesterday if today not ready
    ).execute()

    if not result.data:
        result = supabase.table("set_metrics_daily").select("*").order(
            "as_of", desc=True
        ).limit(500).execute()

    if not result.data:
        print("  No set_metrics_daily data")
        return

    # Normalise scores across all sets
    all_values = [r.get("set_total_value") or 0 for r in result.data if r.get("set_total_value")]
    max_value  = max(all_values) if all_values else 1

    scores = []
    for s in result.data:
        total_val  = s.get("set_total_value") or 0
        pct_90d    = float(s.get("set_pct_90d") or 0)
        top1_share = float(s.get("top1_share_pct") or 100)
        priced     = s.get("priced_cards") or 0
        total      = s.get("total_cards") or 1

        # Value score (0-40)
        val_score = min(40, max(0, round(math.log(max(1, total_val / 100000)) * 6)))
        # Momentum score (0-40)
        mom_score = min(40, max(0, round(pct_90d + 20)))
        # Diversification score (0-20)
        div_score = min(20, max(0, round((100 - top1_share) / 5)))
        strength  = val_score + mom_score + div_score

        # Era classification from set_metadata if available, else guess from name
        name = s.get("set_name", "")
        if any(x in name for x in ["Base", "Jungle", "Fossil", "Rocket", "Gym", "Neo", "Skyridge", "Aquapolis"]):
            era = "wotc"
        elif any(x in name for x in ["EX ", "FireRed", "Emerald", "Delta", "Legend"]):
            era = "ex"
        elif any(x in name for x in ["HeartGold", "Black", "White", "XY", "BW"]):
            era = "mid"
        elif any(x in name for x in ["Sun", "Moon", "Sword", "Shield"]):
            era = "modern"
        elif any(x in name for x in ["Scarlet", "Violet", "Paldea", "Paradox", "Obsidian", "Temporal", "Stellar"]):
            era = "recent"
        else:
            era = "other"

        scores.append({
            "set_name":            name,
            "total_cards":         s.get("total_cards"),
            "priced_cards":        priced,
            "set_total_value":     total_val,
            "set_median_value":    s.get("set_median_value"),
            "top1_share_pct":      top1_share,
            "top1_card_name":      s.get("top1_card_name"),
            "top1_card_price":     s.get("top1_card_price"),
            "set_pct_30d":         float(s.get("set_pct_30d") or 0),
            "set_pct_90d":         pct_90d,
            "set_pct_365d":        float(s.get("set_pct_365d") or 0),
            "divergence_90d":      float(s.get("divergence_90d") or 0),
            "value_score":         val_score,
            "momentum_score":      mom_score,
            "concentration_score": div_score,
            "strength_score":      strength,
            "era":                 era,
            "as_of":               s.get("as_of"),
        })

    chunk_size = 200
    for i in range(0, len(scores), chunk_size):
        supabase.table("set_scores").upsert(
            scores[i:i + chunk_size], on_conflict="set_name"
        ).execute()

    print(f"  set_scores updated: {len(scores)} sets")


# ── MAIN ─────────────────────────────────────────────────────

def refresh_robust_trends():
    """
    Calls the SQL function that computes median-window based trend metrics.
    Populates robust_pct_30d, robust_pct_7d, is_recovery, trend_quality
    on card_trends. Much more reliable than point-to-point pct changes.
    """
    print("Refreshing robust trend metrics...")
    try:
        supabase.rpc('refresh_robust_trends').execute()
        print("  robust_trends refresh complete")
    except Exception as e:
        print(f"  ERROR refreshing robust trends: {e}")


if __name__ == "__main__":
    update_market_index()
    update_card_scores()
    refresh_robust_trends()
    update_set_scores()
    print("Analytics update complete.")
