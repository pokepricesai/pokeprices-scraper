"""One-shot Scryfall enrichment backfill — Stage 1D.

Reads a fresh Scryfall Default Cards + Oracle Cards bulk feed and
populates the columns added by migrations/2026-09-15c:

    mtg_printings:
      frame, frame_effects, promo_types, security_stamp, border_color,
      watermark, oversized, story_spotlight, booster, variation_of,
      content_warning, flavor_text, finishes_source
      (also refreshes) borderless

    mtg_oracle_cards:
      edhrec_rank, penny_rank, all_parts

Also writes external provider identifiers into
mtg_external_identifiers where present:
    tcgplayer_id, cardmarket_id, mtgo_id, mtgo_foil_id, arena_id,
    cardhoarder_id

Safety
------
Refuses to write unless:
  * ``--dry-run`` is passed (no writes, just parses + reports), OR
  * ``MTG_ENRICHMENT_ENABLED=true`` env var is set.

Idempotency
-----------
Every write goes through ON CONFLICT / merge-duplicates on the same
stable keys used by the Stage 1B ingester. Re-runs are safe.

Return codes:
    0   success
    1   crashed
    2   env-flag denied
    3   Supabase credentials missing
    4   MarketImportRunAbort
    5   Scryfall fetch failure
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import scryfall_ingestion as si  # noqa: E402
from mtgjson_ingestion import (   # noqa: E402
    SupabaseClient, build_default_supabase_client,
)


log = logging.getLogger("backfill_scryfall_enrichment")

ENV_ENABLED = "MTG_ENRICHMENT_ENABLED"

BATCH_SIZE = 500


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--only-scryfall-id", default=None,
                   help="Restrict to a single Scryfall printing UUID (for spot debug)")
    p.add_argument("--limit", type=int, default=None,
                   help="Stop after processing N printings (for spot debug)")
    return p.parse_args(argv)


def _configure_logging(v: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if v else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


# ─── Printing transform ────────────────────────────────────────────────────

_PROVIDER_ID_FIELDS = [
    ("tcgplayer_id",       "tcgplayer",  "product_id"),
    ("tcgplayer_etched_id","tcgplayer",  "etched_product_id"),
    ("cardmarket_id",      "cardmarket", "product_id"),
    ("mtgo_id",            "mtgo",       "id"),
    ("mtgo_foil_id",       "mtgo",       "foil_id"),
    ("arena_id",           "arena",      "id"),
    ("cardhoarder_id",     "cardhoarder","id"),
]


def _transform_printing_enrichment(card: dict) -> dict | None:
    sid = card.get("id")
    if not sid:
        return None
    finishes = card.get("finishes")
    return {
        "scryfall_id":      sid,
        "frame":            card.get("frame"),
        "frame_effects":    card.get("frame_effects"),
        "promo_types":      card.get("promo_types"),
        "security_stamp":   card.get("security_stamp"),
        "border_color":     card.get("border_color"),
        "watermark":        card.get("watermark"),
        "oversized":        card.get("oversized"),
        "story_spotlight":  card.get("story_spotlight"),
        "booster":          card.get("booster"),
        "variation_of":     card.get("variation_of"),
        "content_warning":  card.get("content_warning"),
        "flavor_text":      card.get("flavor_text"),
        "finishes_source":  finishes if isinstance(finishes, list) else None,
        # Refresh derived borderless bool
        "borderless":       card.get("border_color") == "borderless",
    }


def _transform_oracle_enrichment(card: dict) -> dict | None:
    oid = card.get("oracle_id")
    if not oid:
        return None
    return {
        "oracle_id":   oid,
        "edhrec_rank": card.get("edhrec_rank"),
        "penny_rank":  card.get("penny_rank"),
        "all_parts":   card.get("all_parts"),
    }


def _extract_external_ids(card: dict, printing_id: str) -> list[dict]:
    """Return a list of mtg_external_identifiers rows for this printing.

    ``printing_id`` is our internal UUID (resolved from scryfall_id).
    """
    rows: list[dict] = []
    for scryfall_key, provider, id_type in _PROVIDER_ID_FIELDS:
        val = card.get(scryfall_key)
        if val is None:
            continue
        rows.append({
            "printing_id":       printing_id,
            "provider":          provider,
            "identifier_type":   id_type,
            "identifier_value":  str(val),
        })
    return rows


# ─── Orchestrator ──────────────────────────────────────────────────────────

def main(argv=None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)
    log.info("scryfall enrichment backfill start dry_run=%s", args.dry_run)

    if not args.dry_run and os.environ.get(ENV_ENABLED, "").strip().lower() != "true":
        log.error("Refusing to write: %s != 'true'. Set it or pass --dry-run.", ENV_ENABLED)
        return 2

    supabase = build_default_supabase_client()
    if not args.dry_run and supabase is None:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing")
        return 3

    # Load scryfall_id -> internal printing_id map for external-id writes.
    log.info("loading scryfall_id -> printing_id map …")
    sid_to_pid = _load_printing_id_map(supabase)
    log.info("scryfall_id map size: %d", len(sid_to_pid))

    # ─── Oracle Cards enrichment ────────────────────────────────────────
    scryfall_client = si.ScryfallClient()
    bulk = scryfall_client.get_bulk_metadata()

    oracle_meta = bulk.get(si.BULK_TYPE_ORACLE)
    if oracle_meta:
        log.info("streaming Scryfall oracle_cards …")
        oracle_batch: list[dict] = []
        oracle_written = 0
        for raw in scryfall_client.iter_bulk_records(
            oracle_meta["download_uri"], oracle_meta.get("content_encoding")
        ):
            row = _transform_oracle_enrichment(raw)
            if row is None:
                continue
            oracle_batch.append(row)
            if len(oracle_batch) >= BATCH_SIZE:
                oracle_written += _flush_oracle(supabase, oracle_batch, args.dry_run)
                oracle_batch = []
        if oracle_batch:
            oracle_written += _flush_oracle(supabase, oracle_batch, args.dry_run)
        log.info("oracle enrichment done: %d rows upserted", oracle_written)

    # ─── Default Cards enrichment + external identifiers ───────────────
    default_meta = bulk.get(si.BULK_TYPE_DEFAULT)
    if not default_meta:
        log.error("scryfall default_cards bulk metadata missing")
        return 5

    log.info("streaming Scryfall default_cards …")
    printing_batch: list[dict] = []
    extid_batch: list[dict] = []
    printing_written = 0
    extid_written = 0
    seen = 0
    for raw in scryfall_client.iter_bulk_records(
        default_meta["download_uri"], default_meta.get("content_encoding")
    ):
        if args.only_scryfall_id and raw.get("id") != args.only_scryfall_id:
            continue
        seen += 1
        prow = _transform_printing_enrichment(raw)
        if prow:
            printing_batch.append(prow)
        pid = sid_to_pid.get(raw.get("id"))
        if pid:
            extid_batch.extend(_extract_external_ids(raw, pid))

        if len(printing_batch) >= BATCH_SIZE:
            printing_written += _flush_printings(supabase, printing_batch, args.dry_run)
            printing_batch = []
        if len(extid_batch) >= BATCH_SIZE:
            extid_written += _flush_extids(supabase, extid_batch, args.dry_run)
            extid_batch = []
        if args.limit and seen >= args.limit:
            break

    if printing_batch:
        printing_written += _flush_printings(supabase, printing_batch, args.dry_run)
    if extid_batch:
        extid_written += _flush_extids(supabase, extid_batch, args.dry_run)

    log.info(
        "backfill complete: printings=%d external_ids=%d (dry_run=%s)",
        printing_written, extid_written, args.dry_run,
    )
    return 0


def _load_printing_id_map(supabase) -> dict[str, str]:
    if supabase is None:
        return {}
    out: dict[str, str] = {}
    offset = 0
    while True:
        code, body = supabase._req(
            f"/rest/v1/mtg_printings?select=id,scryfall_id&limit=1000&offset={offset}"
        )
        if code >= 400:
            raise RuntimeError(f"printing map fetch failed: HTTP {code} {body[:200]}")
        rows = json.loads(body) if body else []
        for r in rows:
            if r.get("scryfall_id") and r.get("id"):
                out[r["scryfall_id"]] = r["id"]
        if len(rows) < 1000:
            break
        offset += 1000
    return out


def _flush_oracle(supabase, rows: list[dict], dry_run: bool) -> int:
    if dry_run or supabase is None:
        return len(rows)
    code, body = supabase._req(
        "/rest/v1/mtg_oracle_cards?on_conflict=oracle_id",
        method="POST", body=rows,
        prefer="resolution=merge-duplicates,return=minimal",
    )
    if code >= 400:
        log.error("oracle flush failed: HTTP %s %s", code, body[:400])
        return 0
    return len(rows)


def _flush_printings(supabase, rows: list[dict], dry_run: bool) -> int:
    if dry_run or supabase is None:
        return len(rows)
    code, body = supabase._req(
        "/rest/v1/mtg_printings?on_conflict=scryfall_id",
        method="POST", body=rows,
        prefer="resolution=merge-duplicates,return=minimal",
    )
    if code >= 400:
        log.error("printings flush failed: HTTP %s %s", code, body[:400])
        return 0
    return len(rows)


def _flush_extids(supabase, rows: list[dict], dry_run: bool) -> int:
    if dry_run or supabase is None:
        return len(rows)
    # STAGE 1C invariant (migration 2026-09-14d): only
    # UNIQUE(provider, identifier_type, identifier_value) is enforced.
    # UNIQUE(printing_id, provider, identifier_type) was removed to
    # allow multi-UUID printings.
    code, body = supabase._req(
        "/rest/v1/mtg_external_identifiers?on_conflict=provider,identifier_type,identifier_value",
        method="POST", body=rows,
        prefer="resolution=merge-duplicates,return=minimal",
    )
    if code >= 400:
        log.error("external_id flush failed: HTTP %s %s", code, body[:400])
        return 0
    return len(rows)


if __name__ == "__main__":
    sys.exit(main())
