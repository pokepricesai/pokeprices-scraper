"""
scrape_state.py — Block 5A-W-49-FIX2
=====================================

Buffered writer for the `scrape_attempt_state` table introduced by
`migrations/2026-08-01-w49-scrape-state.sql`.

Rules encoded here (from the block brief):

    * `priced`                     → reset streak to 0, bump success ts
    * `page_reached_no_price`      → +1 streak,        bump success ts
    * `transient_http_failure`     → streak unchanged
    * `timeout`                    → streak unchanged
    * `not_found`                  → streak unchanged
    * `incorrect_product_rejected` → streak unchanged

Notes:
    * `card_slug` written to this table is ALWAYS the bare numeric
      form (matches `cards.card_slug`). Never `pc-<id>`.
    * A pre-loaded state map (from `cadence.load_state_from_supabase`)
      seeds each card's prior streak so a rerun keeps counting from
      the last real value instead of restarting at 0.
    * Buffered upserts of 100 rows via PostgREST
      `on_conflict=card_slug`; retries are idempotent.
    * Any batch failure is logged loudly and returns False; the caller
      is expected to surface that back to the scraper's error tally so
      partial writes don't silently corrupt cadence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import requests

# Categories that count as a "successful page reach". Only these
# increment or reset the streak counter.
CATEGORY_INCREMENTS_STREAK = frozenset({"page_reached_no_price"})
CATEGORY_RESETS_STREAK = frozenset({"priced"})

VALID_CATEGORIES = frozenset({
    "priced",
    "page_reached_no_price",
    "not_found",
    "transient_http_failure",
    "timeout",
    "incorrect_product_rejected",
})


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ScrapeStateBuffer:
    """Buffered writer. Owns a small dict of pending row upserts keyed
    on `card_slug` so multiple record() calls for the same card in the
    same run collapse into one write."""

    def __init__(self, state_map: dict | None = None):
        # Prior state loaded at run start — {bare_slug: {"no_price_streak": int, ...}}
        self._prior = state_map or {}
        self._pending: dict[str, dict] = {}
        self._errors: int = 0

    def record(
        self,
        bare_slug: str,
        category: str,
        http_status: int | None = None,
    ) -> None:
        """Compute the new streak from prior state and buffer the row."""
        if category not in VALID_CATEGORIES:
            raise ValueError(f"invalid scrape category: {category!r}")

        prev_streak = int(self._prior.get(bare_slug, {}).get("no_price_streak") or 0)
        # Also honour any pending write earlier in the same batch so a
        # card seen twice within one buffer flush stays coherent.
        if bare_slug in self._pending:
            prev_streak = int(self._pending[bare_slug].get(
                "consecutive_successful_no_price_count",
                prev_streak,
            ))

        if category in CATEGORY_RESETS_STREAK:
            new_streak = 0
        elif category in CATEGORY_INCREMENTS_STREAK:
            new_streak = prev_streak + 1
        else:
            new_streak = prev_streak

        now = _now_iso()
        row: dict = {
            "card_slug": bare_slug,
            "last_attempted_at": now,
            "last_result_category": category,
            "consecutive_successful_no_price_count": new_streak,
        }
        if http_status is not None:
            row["last_http_status"] = int(http_status)
        # `last_successful_fetch_at` only bumps on categories that reached
        # the actual product page — 200 responses. Not on transient or
        # not_found or timeout.
        if category in ("priced", "page_reached_no_price"):
            row["last_successful_fetch_at"] = now

        self._pending[bare_slug] = row

    def flush(self, supabase_url: str, supabase_key: str, batch_size: int = 100) -> bool:
        """Upsert the buffered rows in `batch_size` chunks. Returns True
        only when every row landed. On any HTTP error the buffer is
        preserved so the caller can retry."""
        if not self._pending:
            return True
        rows = list(self._pending.values())
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }
        url = f"{supabase_url}/rest/v1/scrape_attempt_state?on_conflict=card_slug"
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            try:
                resp = requests.post(url, json=batch, headers=headers, timeout=30)
            except Exception as e:
                self._errors += 1
                print(f"  scrape_state flush network error: {e}")
                return False
            if resp.status_code not in (200, 201, 204):
                self._errors += 1
                print(f"  scrape_state flush FAILED: {resp.status_code} — {resp.text[:200]}")
                return False
        # Success: clear the buffer.
        self._pending.clear()
        return True

    def pending_count(self) -> int:
        return len(self._pending)

    def error_count(self) -> int:
        return self._errors


# Convenience module-level singleton for scripts that want the simplest
# integration — the scraper's main() creates its own instance instead.
_default = ScrapeStateBuffer()


def record(bare_slug, category, http_status=None):
    _default.record(bare_slug, category, http_status)


def flush(supabase_url, supabase_key):
    return _default.flush(supabase_url, supabase_key)
