"""Refresh mtg_current_prices by calling the server-side reducer.

Stage 1D constraint: never page millions of raw observations through
PostgREST just to deduplicate them. The reduction lives in
``public.refresh_mtg_current_prices(look_back_days)`` (installed by
migration 2026-09-15b) and only the summary row travels over HTTP.

Rows transferred over the wire per invocation:
    * request:  one small JSON body ({"look_back_days": N})
    * response: one row with four columns (bigint, bigint, date, jsonb)

Nothing else — no pagination, no per-observation transfer.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request

log = logging.getLogger("mtg_stage1d.current_prices")

# Public constants — used by tests and by the pipeline args.
INCREMENTAL_LOOK_BACK_DAYS       = 3
INITIAL_POPULATION_LOOK_BACK_DAYS = 30

RPC_PATH = "/rest/v1/rpc/refresh_mtg_current_prices"

# HTTP client timeout for the RPC. The server-side function has its own
# statement_timeout of 10 minutes; the client must be prepared to wait
# at least that long. The default SupabaseClient timeout is only 60s
# (fine for typical PostgREST calls, too short for this ETL RPC).
RPC_HTTP_TIMEOUT_SEC = 900.0

# Soft warning threshold for the recurring 3-day daily refresh.
# Above this, we should consider a touched-key refresh strategy.
INCREMENTAL_WARN_MS = 90_000


def refresh(
    supabase: Any,
    look_back_days: int = INCREMENTAL_LOOK_BACK_DAYS,
    dry_run: bool = False,
) -> dict:
    """Incremental server-side refresh (normal daily usage)."""
    return _invoke(supabase, look_back_days, dry_run, mode="incremental")


def initial_populate(
    supabase: Any,
    look_back_days: int = INITIAL_POPULATION_LOOK_BACK_DAYS,
    dry_run: bool = False,
) -> dict:
    """First-run population with a wider window.

    Server-side reducer handles the 21-million-row scan without any
    pagination. Empty-target detection is delegated to the DB (the
    upsert simply inserts everything on the first pass and returns
    the count).
    """
    return _invoke(supabase, look_back_days, dry_run, mode="initial")


def _invoke(supabase: Any, look_back_days: int, dry_run: bool, mode: str) -> dict:
    if dry_run or supabase is None:
        return {
            "mode":                mode,
            "look_back_days":      look_back_days,
            "keys_upserted":       0,
            "distinct_finishes":   0,
            "latest_observed_on":  None,
            "provider_breakdown":  {},
            "dry_run":             True,
            "raw_rows_transferred": 0,
            "wall_ms":             0,
        }
    # We bypass supabase._req here because that path has a 60-second
    # socket timeout AND exponential-backoff retries that combine to
    # abort long ETL RPCs after ~5-6 minutes. Direct urllib with a
    # 900-second timeout gives the server-side statement_timeout
    # (10 min) room to complete.
    url = f"{supabase.url}{RPC_PATH}"
    payload = json.dumps({"look_back_days": int(look_back_days)}).encode("utf-8")
    req = Request(url, method="POST", data=payload, headers={
        "apikey":         supabase.key,
        "Authorization":  f"Bearer {supabase.key}",
        "Accept":         "application/json",
        "Content-Type":   "application/json",
    })
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=RPC_HTTP_TIMEOUT_SEC) as r:
            code = r.getcode()
            body = r.read().decode("utf-8")
    except HTTPError as e:
        code = e.code
        body = e.read().decode("utf-8")
    wall_ms = int((time.perf_counter() - t0) * 1000)
    if code >= 400:
        raise RuntimeError(
            f"refresh_mtg_current_prices RPC failed HTTP {code} after {wall_ms}ms: {body[:400]}"
        )
    rows = json.loads(body) if body else []
    if not rows:
        return {
            "mode":                mode,
            "look_back_days":      look_back_days,
            "keys_upserted":       0,
            "distinct_finishes":   0,
            "latest_observed_on":  None,
            "provider_breakdown":  {},
            "dry_run":             False,
            "raw_rows_transferred": 0,
            "wall_ms":             wall_ms,
        }
    r = rows[0]
    log.info(
        "refresh_mtg_current_prices RPC completed: mode=%s look_back=%d keys=%s finishes=%s latest=%s wall=%dms",
        mode, look_back_days, r.get("keys_upserted"), r.get("distinct_finishes"),
        r.get("latest_observed_on"), wall_ms,
    )
    if mode == "incremental" and wall_ms > INCREMENTAL_WARN_MS:
        log.warning(
            "incremental current-price refresh took %dms — exceeds soft target of %dms. "
            "Consider a touched-key refresh strategy before enabling daily cron.",
            wall_ms, INCREMENTAL_WARN_MS,
        )
    return {
        "mode":                mode,
        "look_back_days":      look_back_days,
        "keys_upserted":       int(r.get("keys_upserted") or 0),
        "distinct_finishes":   int(r.get("distinct_finishes") or 0),
        "latest_observed_on":  r.get("latest_observed_on"),
        "provider_breakdown":  r.get("provider_breakdown") or {},
        "dry_run":             False,
        "raw_rows_transferred": 0,
        "wall_ms":             wall_ms,
    }
