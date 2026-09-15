"""DB-backed run lock for Stage 1D daily MTG ingestion.

INSERT … ON CONFLICT DO NOTHING on public.mtg_daily_ingest_locks is
atomic acquire; DELETE releases. Stale locks (heartbeat >30 min old +
expected_release_by past) are eligible for takeover once, logged.

The lock table has no anon/authenticated RLS policies; only
service_role (via SupabaseClient) can operate it.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

log = logging.getLogger("mtg_stage1d.lock")

LOCK_TABLE = "mtg_daily_ingest_locks"

DEFAULT_LEASE_HOURS = 4
STALE_HEARTBEAT_MIN = 30


class LockAcquireFailed(RuntimeError):
    """Raised when the lock is already held and cannot be taken over."""


@dataclass
class LockHandle:
    lock_key: str
    acquired_by: str
    acquired_at: str
    expected_release_by: str

    # Not persisted; convenience for the caller.
    stale_takeover: bool = False


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _who(process_name: str) -> str:
    gha = os.environ.get("GITHUB_ACTIONS")
    if gha == "true":
        run_id = os.environ.get("GITHUB_RUN_ID", "?")
        run_number = os.environ.get("GITHUB_RUN_NUMBER", "?")
        return f"github-actions:{process_name}:run-{run_id}:number-{run_number}"
    host = socket.gethostname()
    pid = os.getpid()
    return f"local:{process_name}:{host}:pid-{pid}"


def acquire(
    supabase: Any,
    lock_key: str,
    process_name: str,
    lease_hours: float = DEFAULT_LEASE_HOURS,
    metadata: dict | None = None,
) -> LockHandle:
    """Attempt to atomically acquire ``lock_key``.

    Raises :class:`LockAcquireFailed` if another live holder exists.
    """
    who = _who(process_name)
    now = datetime.now(tz=timezone.utc)
    payload = {
        "lock_key":            lock_key,
        "acquired_at":         _iso_utc(now),
        "acquired_by":         who,
        "heartbeat_at":        _iso_utc(now),
        "expected_release_by": _iso_utc(now + timedelta(hours=lease_hours)),
        "metadata":            metadata or {},
    }
    handle = _try_insert(supabase, payload)
    if handle is not None:
        return handle

    # A row already exists. Inspect it — if the holder is stale, take
    # over once.
    existing = _fetch_current(supabase, lock_key)
    if existing is None:
        # Racy: another writer released between INSERT and fetch. Retry
        # once.
        handle = _try_insert(supabase, payload)
        if handle is not None:
            return handle
        raise LockAcquireFailed(f"lock {lock_key!r} held; cannot acquire")

    if _is_stale(existing, now):
        log.warning(
            "lock %s appears stale (held by %s since %s, heartbeat %s); attempting one-shot takeover",
            lock_key, existing["acquired_by"], existing["acquired_at"], existing["heartbeat_at"],
        )
        _delete_row(supabase, lock_key, existing["acquired_by"])
        handle = _try_insert(supabase, payload)
        if handle is None:
            raise LockAcquireFailed(
                f"lock {lock_key!r}: takeover raced; giving up"
            )
        handle.stale_takeover = True
        return handle

    raise LockAcquireFailed(
        f"lock {lock_key!r} held by {existing['acquired_by']} "
        f"(acquired_at={existing['acquired_at']}, heartbeat_at={existing['heartbeat_at']})"
    )


def heartbeat(supabase: Any, handle: LockHandle) -> None:
    now = datetime.now(tz=timezone.utc)
    code, body = supabase._req(
        f"/rest/v1/{LOCK_TABLE}?lock_key=eq.{handle.lock_key}"
        f"&acquired_by=eq.{handle.acquired_by}",
        method="PATCH",
        body={"heartbeat_at": _iso_utc(now)},
        prefer="return=minimal",
    )
    if code >= 400:
        log.warning("heartbeat %s failed HTTP %s: %s", handle.lock_key, code, body[:200])


def release(supabase: Any, handle: LockHandle) -> None:
    code, body = supabase._req(
        f"/rest/v1/{LOCK_TABLE}?lock_key=eq.{handle.lock_key}"
        f"&acquired_by=eq.{handle.acquired_by}",
        method="DELETE",
        prefer="return=minimal",
    )
    if code >= 400:
        log.warning("release %s failed HTTP %s: %s", handle.lock_key, code, body[:200])


# ─── Helpers ────────────────────────────────────────────────────────────────

def _try_insert(supabase: Any, payload: dict) -> LockHandle | None:
    """Insert with ON CONFLICT DO NOTHING; return LockHandle if we won."""
    code, body = supabase._req(
        f"/rest/v1/{LOCK_TABLE}?on_conflict=lock_key",
        method="POST",
        body=[payload],
        prefer="resolution=ignore-duplicates,return=representation",
    )
    if code >= 400:
        raise RuntimeError(f"lock insert failed HTTP {code}: {body[:400]}")
    rows = json.loads(body) if body else []
    if not rows:
        return None
    r = rows[0]
    return LockHandle(
        lock_key=r["lock_key"],
        acquired_by=r["acquired_by"],
        acquired_at=r["acquired_at"],
        expected_release_by=r["expected_release_by"],
    )


def _fetch_current(supabase: Any, lock_key: str) -> dict | None:
    code, body = supabase._req(
        f"/rest/v1/{LOCK_TABLE}?lock_key=eq.{lock_key}"
        "&select=lock_key,acquired_at,acquired_by,heartbeat_at,expected_release_by"
    )
    if code >= 400:
        raise RuntimeError(f"lock fetch failed HTTP {code}: {body[:200]}")
    rows = json.loads(body) if body else []
    return rows[0] if rows else None


def _delete_row(supabase: Any, lock_key: str, acquired_by: str) -> None:
    code, body = supabase._req(
        f"/rest/v1/{LOCK_TABLE}?lock_key=eq.{lock_key}"
        f"&acquired_by=eq.{acquired_by}",
        method="DELETE",
        prefer="return=minimal",
    )
    if code >= 400:
        raise RuntimeError(f"lock delete (takeover) failed HTTP {code}: {body[:200]}")


def _is_stale(row: dict, now: datetime) -> bool:
    heartbeat_at = _parse_iso(row["heartbeat_at"])
    expected_release_by = _parse_iso(row["expected_release_by"])
    stale_hb = (now - heartbeat_at) >= timedelta(minutes=STALE_HEARTBEAT_MIN)
    past_lease = now >= expected_release_by
    return stale_hb and past_lease


def _parse_iso(s: str) -> datetime:
    # PostgREST emits "2026-09-15T15:00:00+00:00" style. datetime.fromisoformat
    # handles that in Python 3.11+.
    return datetime.fromisoformat(s.replace("Z", "+00:00"))
