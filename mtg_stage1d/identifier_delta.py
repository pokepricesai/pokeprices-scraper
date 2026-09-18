"""MTGJSON AllIdentifiers delta — inserts new (provider=mtgjson, uuid) mappings.

Stage 1D requirement: new Scryfall printings can appear alongside or
before new MTGJSON price records. If their UUID mappings are missing,
otherwise-valid AllPricesToday observations will be quarantined as
``obs_unmapped_uuid``. So the daily pipeline runs an identifier delta
BEFORE the price ingest.

Design
------
1. Download ``AllIdentifiers.json.gz`` from MTGJSON (small; ~50 MB).
2. SHA-256 verify against ``AllIdentifiers.json.gz.sha256``.
3. Load existing mtg_external_identifiers rows for
   ``provider='mtgjson' AND identifier_type='uuid'`` into a Python set.
4. Load current ``mtg_printings.scryfall_id → id`` mapping.
5. Stream ``AllIdentifiers.json.gz``. For each entry:
   - MTGJSON UUID = the top-level key
   - Scryfall UUID = ``entry.identifiers.scryfallId`` (also seen as
     ``entry.scryfall_id`` in some builds)
6. Skip when the MTGJSON UUID is already known.
7. Skip and count when the corresponding Scryfall printing is not known
   (that will be filled the next time Scryfall's Default Cards bulk
   moves).
8. Otherwise batch-insert into ``mtg_external_identifiers`` with
   ``ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING``.

Uniqueness note (Stage 1C invariant — do not change)
----------------------------------------------------
The Stage 1C migration 2026-09-14d deliberately dropped the composite
UNIQUE on ``(printing_id, provider, identifier_type)``. MTGJSON
legitimately maps some Scryfall printings to multiple UUIDs (5,205
cases at the time of Stage 1B, e.g. multi-face split cards in Unglued
each with their own MTGJSON UUID). The retained UNIQUE is:

    UNIQUE (provider, identifier_type, identifier_value)

which prevents a single identifier value from mapping to more than one
printing. Any code that inserts into mtg_external_identifiers MUST use
that constraint as its ON CONFLICT target.

Idempotency
-----------
Re-running yields zero new rows.

Not exercised (deliberately)
----------------------------
Marketplace identifiers (tcgplayer/cardmarket/etc.) are ALSO present in
AllIdentifiers.identifiers.*, but Stage 1D scope is UUID-only. The
broader marketplace-id load remains deferred until the frontend needs
it.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator
from urllib.request import Request, urlopen

log = logging.getLogger("mtg_stage1d.identifier_delta")

ALLIDS_URL      = "https://mtgjson.com/api/v5/AllIdentifiers.json.gz"
ALLIDS_SHA_URL  = "https://mtgjson.com/api/v5/AllIdentifiers.json.gz.sha256"

# MTGJSON's CDN rejects the default urllib UA with 403. Identify
# ourselves properly, matching the pattern used by scryfall_ingestion.
USER_AGENT = (
    "MTGPricesStage1D/1.0 (+https://www.pokeprices.io; contact@pokeprices.io)"
)


def _http_get(url: str, timeout: float):
    return urlopen(Request(url, headers={"User-Agent": USER_AGENT}), timeout=timeout)


BATCH = 500


@dataclass
class IdentifierDeltaStats:
    identifiers_scanned:        int = 0
    already_known:              int = 0
    newly_inserted:             int = 0
    unknown_scryfall_printing:  int = 0
    malformed_rows:             int = 0
    errors:                     int = 0
    sha256_expected:            str = ""
    sha256_actual:              str = ""
    mtgjson_meta:               dict = field(default_factory=dict)


# ─── Download + verify ─────────────────────────────────────────────────────

def download_and_verify(workspace: Path) -> tuple[Path, str, str]:
    """Return (local_path, expected_sha, actual_sha). Raises on mismatch."""
    workspace.mkdir(parents=True, exist_ok=True)
    with _http_get(ALLIDS_SHA_URL, timeout=60) as r:
        expected = r.read().decode("utf-8").strip()
    with _http_get(ALLIDS_URL, timeout=1800) as r:
        payload = r.read()
    local = workspace / "AllIdentifiers.json.gz"
    local.write_bytes(payload)
    (local.parent / "AllIdentifiers.json.gz.sha256").write_text(expected, encoding="utf-8")
    actual = hashlib.sha256(payload).hexdigest()
    if actual != expected:
        raise RuntimeError(
            f"AllIdentifiers SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return local, expected, actual


# ─── Streaming reader ──────────────────────────────────────────────────────

def _iter_entries(allids_gz: Path) -> Iterator[tuple[str, dict]]:
    """Yield (mtgjson_uuid, entry_dict) from AllIdentifiers.json.gz.

    Uses ijson.kvitems to walk ``data.*`` without holding the whole
    file in memory.
    """
    import ijson
    with gzip.open(allids_gz, "rb") as raw:
        yield from ijson.kvitems(raw, "data")


def _read_meta(allids_gz: Path) -> dict:
    """Read the ``meta`` object at the top of the file (small).

    MTGJSON v5 emits ``{"meta": {"date": "...", "version": "..."}, ...}``.
    Legacy builds nested as ``{"meta": {"data": {"date": "...", ...}}}``.
    Return the flat v5 shape when found, otherwise fall back to the
    legacy nested "data" child, otherwise return {}.
    """
    import ijson
    flat: dict = {}
    with gzip.open(allids_gz, "rb") as raw:
        for k, v in ijson.kvitems(raw, "meta"):
            if k == "data" and isinstance(v, dict):
                return v
            if isinstance(v, (str, int, float, bool)):
                flat[k] = v
    return flat


def extract_scryfall_id(entry: dict) -> str | None:
    """Extract the Scryfall UUID from an AllIdentifiers entry.

    MTGJSON exposes it under ``identifiers.scryfallId`` in v5. Historic
    builds sometimes have the flat ``scryfallId`` at the top; support
    both defensively.
    """
    ids_block = entry.get("identifiers")
    if isinstance(ids_block, dict):
        v = ids_block.get("scryfallId") or ids_block.get("scryfall_id")
        if isinstance(v, str) and v:
            return v
    v = entry.get("scryfallId") or entry.get("scryfall_id")
    if isinstance(v, str) and v:
        return v
    return None


# ─── DB helpers ────────────────────────────────────────────────────────────

def load_known_uuids(supabase: Any) -> set[str]:
    """Return the set of MTGJSON UUIDs already stored for provider=mtgjson."""
    out: set[str] = set()
    offset = 0
    page = 1000
    while True:
        code, body = supabase._req(
            "/rest/v1/mtg_external_identifiers"
            "?provider=eq.mtgjson&identifier_type=eq.uuid"
            f"&select=identifier_value&limit={page}&offset={offset}"
        )
        if code >= 400:
            raise RuntimeError(f"identifier fetch failed HTTP {code}: {body[:200]}")
        rows = json.loads(body) if body else []
        if not rows:
            break
        for r in rows:
            v = r.get("identifier_value")
            if isinstance(v, str) and v:
                out.add(v)
        if len(rows) < page:
            break
        offset += page
    return out


def load_scryfall_to_printing(supabase: Any) -> dict[str, str]:
    """Return {mtg_printings.scryfall_id → id} for existing printings."""
    out: dict[str, str] = {}
    offset = 0
    page = 1000
    while True:
        code, body = supabase._req(
            f"/rest/v1/mtg_printings?select=id,scryfall_id&limit={page}&offset={offset}"
        )
        if code >= 400:
            raise RuntimeError(f"printings fetch failed HTTP {code}: {body[:200]}")
        rows = json.loads(body) if body else []
        if not rows:
            break
        for r in rows:
            sid = r.get("scryfall_id")
            pid = r.get("id")
            if isinstance(sid, str) and isinstance(pid, str):
                out[sid] = pid
        if len(rows) < page:
            break
        offset += page
    return out


def _flush_inserts(supabase: Any, rows: list[dict]) -> int:
    if not rows:
        return 0
    # Stage 1C invariant: only UNIQUE (provider, identifier_type,
    # identifier_value) is enforced. See module docstring.
    on_conflict = "provider,identifier_type,identifier_value"
    code, body = supabase._req(
        f"/rest/v1/mtg_external_identifiers?on_conflict={on_conflict}",
        method="POST", body=rows,
        prefer="resolution=ignore-duplicates,return=representation",
    )
    if code >= 400:
        log.error("identifier insert failed HTTP %s: %s", code, body[:400])
        return 0
    try:
        inserted = json.loads(body) if body else []
        return len(inserted)
    except json.JSONDecodeError:
        return len(rows)


# Publicly exported for regression tests that verify the correct
# ON CONFLICT target is used everywhere it should be.
IDENTIFIER_CONFLICT_TARGET = "provider,identifier_type,identifier_value"


# ─── Orchestration ─────────────────────────────────────────────────────────

def run(
    supabase: Any,
    workspace: Path,
    dry_run: bool = False,
) -> IdentifierDeltaStats:
    """Full delta: download → verify → stream → insert new rows."""
    stats = IdentifierDeltaStats()
    allids_gz, expected, actual = download_and_verify(workspace)
    stats.sha256_expected = expected
    stats.sha256_actual   = actual
    stats.mtgjson_meta    = _read_meta(allids_gz)

    known = load_known_uuids(supabase) if supabase else set()
    scryfall_to_printing = load_scryfall_to_printing(supabase) if supabase else {}
    log.info("identifier_delta: known_uuids=%d scryfall_map=%d",
             len(known), len(scryfall_to_printing))

    batch: list[dict] = []
    return apply_stream(
        _iter_entries(allids_gz),
        known_uuids=known,
        scryfall_to_printing=scryfall_to_printing,
        supabase=supabase,
        dry_run=dry_run,
        stats=stats,
    )


def apply_stream(
    entries: Iterator[tuple[str, dict]],
    *,
    known_uuids: set[str],
    scryfall_to_printing: dict[str, str],
    supabase: Any,
    dry_run: bool,
    stats: IdentifierDeltaStats,
) -> IdentifierDeltaStats:
    """Pure-logic core (unit-testable). ``entries`` yields
    (mtgjson_uuid, entry_dict). See :func:`run` for the wrapper.
    """
    batch: list[dict] = []
    for mtgjson_uuid, entry in entries:
        stats.identifiers_scanned += 1
        if not isinstance(mtgjson_uuid, str) or not mtgjson_uuid:
            stats.malformed_rows += 1
            continue
        if mtgjson_uuid in known_uuids:
            stats.already_known += 1
            continue
        if not isinstance(entry, dict):
            stats.malformed_rows += 1
            continue
        scryfall_id = extract_scryfall_id(entry)
        if not scryfall_id:
            stats.malformed_rows += 1
            continue
        printing_id = scryfall_to_printing.get(scryfall_id)
        if not printing_id:
            stats.unknown_scryfall_printing += 1
            continue
        batch.append({
            "printing_id":      printing_id,
            "provider":         "mtgjson",
            "identifier_type":  "uuid",
            "identifier_value": mtgjson_uuid,
        })
        if len(batch) >= BATCH:
            if dry_run or supabase is None:
                stats.newly_inserted += len(batch)
            else:
                stats.newly_inserted += _flush_inserts(supabase, batch)
            batch = []
    if batch:
        if dry_run or supabase is None:
            stats.newly_inserted += len(batch)
        else:
            stats.newly_inserted += _flush_inserts(supabase, batch)

    log.info(
        "identifier_delta done: scanned=%d already_known=%d new=%d unknown_scryfall=%d malformed=%d errors=%d",
        stats.identifiers_scanned, stats.already_known, stats.newly_inserted,
        stats.unknown_scryfall_printing, stats.malformed_rows, stats.errors,
    )
    return stats
