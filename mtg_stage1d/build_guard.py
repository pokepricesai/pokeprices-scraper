"""Same-build MTGJSON duplicate-ingest guard.

Distinguishes INGESTION_COMPLETED from HEALTH_WARNING.

A run counts as "completed" if:
  * ``status == 'success'``, OR
  * ``status == 'partial' AND notes.errors == 0``  (warnings only)

A ``status == 'partial' AND notes.errors > 0`` run is NOT considered
completed — some observations failed to land; a re-run should retry.
A ``status == 'failed'`` or ``status == 'running'`` run is not
considered completed either.

Rationale: the daily cron may fire twice (15:00 and 18:00 UTC). We
must not re-run the same build after a fully-inserted-with-warnings
15:00 run, and we must re-run after a truly incomplete 15:00 run.
"""
from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger("mtg_stage1d.build_guard")


def is_run_completed(run: dict, notes: dict | None = None) -> bool:
    """Pure-logic completion classifier — unit-testable.

    :param run: the market_import_runs row (with ``status`` at minimum).
    :param notes: parsed notes dict; if None, will attempt to parse
                  ``run['notes']``.
    """
    status = run.get("status")
    if status == "success":
        return True
    if status != "partial":
        return False
    if notes is None:
        raw = run.get("notes")
        if isinstance(raw, str):
            try:
                notes = json.loads(raw)
            except json.JSONDecodeError:
                notes = None
        elif isinstance(raw, dict):
            notes = raw
    if not isinstance(notes, dict):
        return False
    return (notes.get("errors") or 0) == 0


def most_recent_ingested_build_date(supabase: Any) -> str | None:
    """Return the max ``notes.mtgjson_meta.data.date`` across recent
    MTGJSON runs that count as INGESTION_COMPLETED (per :func:`is_run_completed`).
    """
    code, body = supabase._req(
        "/rest/v1/market_import_runs"
        "?provider=eq.mtgjson"
        "&status=in.(success,partial)"
        "&order=started_at.desc"
        "&limit=25"
        "&select=id,status,started_at,notes"
    )
    if code >= 400:
        raise RuntimeError(f"build_guard fetch failed HTTP {code}: {body[:200]}")
    rows = json.loads(body) if body else []
    best: str | None = None
    for r in rows:
        notes = r.get("notes")
        if isinstance(notes, str):
            try:
                notes = json.loads(notes)
            except json.JSONDecodeError:
                continue
        if not isinstance(notes, dict):
            continue
        if not is_run_completed(r, notes):
            continue
        meta = notes.get("mtgjson_meta") or {}
        data = meta.get("data") or {}
        build_date = data.get("date")
        if not build_date:
            continue
        if best is None or build_date > best:
            best = build_date
    return best


def should_skip(current_build_date: str, most_recent: str | None) -> bool:
    """A rerun of the same or older MTGJSON build is a no-op."""
    if most_recent is None:
        return False
    return current_build_date <= most_recent
