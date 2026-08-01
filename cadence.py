"""
cadence.py — Block 5A-W-49 cohort classifier.
========================================================================

Splits the PriceCharting scrape schedule into a daily and a weekly
cohort based on each card's latest successful raw price observation.

Cohorts (see block brief for exact definitions):

    DAILY_VALUE     : latest_raw_cents >= 200      ($2.00 or more)
    WEEKLY_LOW      : 0 < latest_raw_cents < 200   ($0.01 - $1.99)
    DAILY_DISCOVERY : no successful raw price yet, AND either brand-new
                      OR the last scrape category was a transient
                      transport failure (429 / 5xx / timeout / etc.)
    WEEKLY_UNPRICED : the PriceCharting product page was successfully
                      reached >= 3 times AND no usable raw price
                      appeared on any of those fetches
                      (page_reached_no_price counter).

Selection (--cadence flag):

    --cadence daily  → DAILY_VALUE + DAILY_DISCOVERY + retryable transients
    --cadence weekly → WEEKLY_LOW + WEEKLY_UNPRICED
    --cadence all    → every eligible card (default for --set / --sets-file
                       / --pc-ids invocations unless caller passes
                       --cadence explicitly)

Design goals:

  * Pure classification (no I/O). Every I/O helper is separately
    named, tested, and swappable in unit tests.
  * A single bulk fetch loads state for the entire catalogue. Never
    one HTTP request per card.
  * A card that crosses the $2 boundary in either direction moves
    cohorts automatically the next time the classifier runs — no
    batch-file edit required.
"""

from __future__ import annotations

from enum import Enum
from typing import Callable, Iterable

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

# Cadence boundary. $2.00 exactly is DAILY (>= 200 cents). $1.99 is WEEKLY.
DAILY_THRESHOLD_CENTS = 200

# Number of successful page-reached-no-price observations required
# before a card moves from DAILY_DISCOVERY into WEEKLY_UNPRICED.
NO_PRICE_STREAK_TO_WEEKLY = 3

# scrape_attempt_state.last_result_category values that classify as
# a retryable transient (stay in DAILY_DISCOVERY):
TRANSIENT_CATEGORIES = frozenset({
    "transient_http_failure",
    "timeout",
})


class Cohort(str, Enum):
    DAILY_VALUE = "DAILY_VALUE"
    WEEKLY_LOW = "WEEKLY_LOW"
    DAILY_DISCOVERY = "DAILY_DISCOVERY"
    WEEKLY_UNPRICED = "WEEKLY_UNPRICED"


# Cadence group memberships. Every eligible cohort belongs to exactly
# one group per cadence choice.
DAILY_COHORTS = frozenset({Cohort.DAILY_VALUE, Cohort.DAILY_DISCOVERY})
WEEKLY_COHORTS = frozenset({Cohort.WEEKLY_LOW, Cohort.WEEKLY_UNPRICED})


# ---------------------------------------------------------------------
# Pure classifier — no I/O
# ---------------------------------------------------------------------

def classify(
    latest_raw_cents: int | None,
    last_result_category: str | None = None,
    no_price_streak: int = 0,
) -> Cohort:
    """
    Return the cohort for a single card given its cached state.

    Parameters
    ----------
    latest_raw_cents
        Most recent successful raw/loose price observation, in cents.
        None (or 0) means no successful raw observation exists yet.
    last_result_category
        The `scrape_attempt_state.last_result_category` value if the
        card has one. Used only to determine whether a card lacking
        a raw price should stay in DAILY_DISCOVERY (retryable
        transient / never attempted) or move to WEEKLY_UNPRICED after
        enough no-price fetches.
    no_price_streak
        `scrape_attempt_state.consecutive_successful_no_price_count`.
        Reaching NO_PRICE_STREAK_TO_WEEKLY moves the card out of the
        daily cohort even if the last attempt was successful-with-no-
        price rather than transient.

    Cross-boundary examples:

      classify(200)  → DAILY_VALUE
      classify(199)  → WEEKLY_LOW
      classify(0)    → DAILY_DISCOVERY (never scraped or last-null)
      classify(None, "transient_http_failure", 0) → DAILY_DISCOVERY
      classify(None, "page_reached_no_price", 3)  → WEEKLY_UNPRICED
    """
    if latest_raw_cents is not None and latest_raw_cents >= DAILY_THRESHOLD_CENTS:
        return Cohort.DAILY_VALUE
    if latest_raw_cents is not None and latest_raw_cents > 0:
        return Cohort.WEEKLY_LOW
    # No latest raw. Decide DAILY_DISCOVERY vs WEEKLY_UNPRICED.
    # Per brief: "transient failures remain daily" — regardless of the
    # streak counter, if the most recent attempt was a transport error
    # (429 / 5xx / timeout) we retry soon rather than declaring the
    # card unpriced. WEEKLY_UNPRICED is reserved for evidence — three
    # successful page loads that returned no price.
    if last_result_category in TRANSIENT_CATEGORIES:
        return Cohort.DAILY_DISCOVERY
    if no_price_streak >= NO_PRICE_STREAK_TO_WEEKLY:
        return Cohort.WEEKLY_UNPRICED
    return Cohort.DAILY_DISCOVERY


def include_for_cadence(cohort: Cohort, cadence: str) -> bool:
    """
    Return True when a card in this cohort should be scraped in the
    given cadence run.

    cadence is one of {'daily', 'weekly', 'all'}.
    Any other value raises ValueError (fail fast).
    """
    if cadence == "all":
        return True
    if cadence == "daily":
        return cohort in DAILY_COHORTS
    if cadence == "weekly":
        return cohort in WEEKLY_COHORTS
    raise ValueError(f"unknown cadence: {cadence!r}")


# ---------------------------------------------------------------------
# Bulk state loader — pluggable HTTP for tests
# ---------------------------------------------------------------------

def load_state_from_supabase(
    supabase_url: str,
    supabase_key: str,
    http_get: Callable | None = None,
) -> dict[str, dict]:
    """
    Bulk-load classification state for the whole catalogue.

    Returns
    -------
    dict keyed by cards.card_slug (bare, without pc- prefix):
        {
          "latest_raw_cents": int | None,
          "last_result_category": str | None,
          "no_price_streak": int,
        }

    Uses at most two paginated queries — one for `card_trends`, one for
    `scrape_attempt_state`. Never one query per card.

    A missing `scrape_attempt_state` table (migration not yet applied)
    is treated as "no state" — every card without a card_trends row
    lands in DAILY_DISCOVERY. Applying the migration later just enables
    the WEEKLY_UNPRICED transition; existing behaviour does not change.
    """
    import requests
    _get = http_get or requests.get
    headers = {"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}"}
    state: dict[str, dict] = {}

    # 1. card_trends → latest raw price
    offset = 0
    while True:
        url = (f"{supabase_url}/rest/v1/card_trends"
               f"?select=card_slug,current_raw&order=card_slug"
               f"&offset={offset}&limit=1000")
        r = _get(url, headers=headers, timeout=60)
        if r.status_code != 200:
            break
        rows = r.json()
        if not rows:
            break
        for row in rows:
            state.setdefault(row["card_slug"], {})["latest_raw_cents"] = row.get("current_raw")
        if len(rows) < 1000:
            break
        offset += 1000

    # 2. scrape_attempt_state → optional (may not exist pre-migration)
    offset = 0
    while True:
        url = (f"{supabase_url}/rest/v1/scrape_attempt_state"
               f"?select=card_slug,last_result_category,consecutive_successful_no_price_count"
               f"&order=card_slug&offset={offset}&limit=1000")
        r = _get(url, headers=headers, timeout=60)
        if r.status_code != 200:
            # Migration not applied yet — silently continue.
            break
        rows = r.json()
        if not rows:
            break
        for row in rows:
            s = state.setdefault(row["card_slug"], {})
            s["last_result_category"] = row.get("last_result_category")
            s["no_price_streak"] = row.get("consecutive_successful_no_price_count") or 0
        if len(rows) < 1000:
            break
        offset += 1000

    return state


# ---------------------------------------------------------------------
# Filter helper — used by scraper.main()
# ---------------------------------------------------------------------

def _bare_slug(card_slug) -> str:
    """
    Block 5A-W-49-FIX2 — normalise a `card_slug` value to the BARE
    numeric string used by every non-daily_prices table.

    The scraper's `load_cards_from_pc_csvs` builds
    `card_slug = f"pc-{pc_id}"` because that same value ends up as the
    row key in daily_prices (which uses the `pc-` prefix). Every other
    table — cards, card_trends, scrape_attempt_state,
    provider_card_links — uses the BARE numeric slug. cadence state is
    keyed on the bare form, so we normalise here and here only. The
    price-writing path is untouched.
    """
    s = str(card_slug)
    return s[3:] if s.startswith("pc-") else s


def filter_cards_by_cadence(
    cards: Iterable[dict],
    cadence: str,
    state: dict[str, dict],
) -> tuple[list[dict], dict[Cohort, int]]:
    """
    Return the subset of the CSV-derived `cards` list that belongs in
    a `cadence` run, plus a cohort-histogram of every card considered
    (regardless of whether it was included).

    Accepts BOTH `card_slug` formats:
      * bare numeric (e.g. "8330138") — used by cards / card_trends
      * pc-prefixed (e.g. "pc-8330138") — used by daily_prices and by
        the scraper's in-memory card dicts

    Normalisation happens ONLY for the cadence-state lookup. The
    `card_slug` value on each dict is not mutated — downstream
    daily_prices writes still see whatever the caller passed in.
    """
    kept = []
    hist: dict[Cohort, int] = {c: 0 for c in Cohort}
    for c in cards:
        slug = _bare_slug(c["card_slug"])
        s = state.get(slug, {})
        coh = classify(
            latest_raw_cents=s.get("latest_raw_cents"),
            last_result_category=s.get("last_result_category"),
            no_price_streak=s.get("no_price_streak") or 0,
        )
        hist[coh] += 1
        if include_for_cadence(coh, cadence):
            kept.append(c)
    return kept, hist
