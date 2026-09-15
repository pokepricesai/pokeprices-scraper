"""Ensure the current + next monthly mtg_price_observations partition exists.

Uses the SECURITY-DEFINER function public.ensure_mtg_price_partition
installed by 2026-09-15a. Callable only via the service_role client.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

log = logging.getLogger("mtg_stage1d.partitions")

RPC = "/rest/v1/rpc/ensure_mtg_price_partition"


def _first_of_month(d: date) -> date:
    return d.replace(day=1)


def _next_month_first(d: date) -> date:
    m = _first_of_month(d)
    # December → January of next year.
    if m.month == 12:
        return date(m.year + 1, 1, 1)
    return date(m.year, m.month + 1, 1)


def ensure_current_and_next(supabase: Any, today: date | None = None) -> list[str]:
    """Idempotently ensure this month and next month exist.

    Returns the function's return values (["exists:mtg_price_...", "created:..."])
    for auditability.
    """
    today = today or date.today()
    curr = _first_of_month(today)
    nxt  = _next_month_first(today)
    return [_ensure(supabase, curr), _ensure(supabase, nxt)]


def _ensure(supabase: Any, target_month: date) -> str:
    code, body = supabase._req(
        RPC,
        method="POST",
        body={"target_month": target_month.isoformat()},
    )
    if code >= 400:
        raise RuntimeError(
            f"ensure_mtg_price_partition failed HTTP {code}: {body[:400]}"
        )
    try:
        parsed = json.loads(body) if body else body
    except json.JSONDecodeError:
        parsed = body
    result = parsed if isinstance(parsed, str) else str(parsed)
    log.info("ensure_mtg_price_partition(%s) → %s", target_month, result)
    return result
