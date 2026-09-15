"""Automatic Scryfall bulk-data freshness check + invocation.

Uses the existing proven ``scryfall_ingestion.ScryfallCatalogueIngestion``
class. Does not re-implement Scryfall ingestion; only decides whether
to invoke it and gathers the updated_at timestamps for build-guard use
by future runs.

Design
------
1. Query Scryfall ``/bulk-data`` for oracle_cards / default_cards /
   rulings ``updated_at`` timestamps.
2. Query the last successful (or partial-with-zero-errors) Scryfall
   ``market_import_runs`` row and read its ``notes`` for the previously
   stored ``oracle_updated_at`` / ``default_updated_at`` /
   ``rulings_updated_at``.
3. If all three unchanged → return ``UNCHANGED``.
4. If any changed → invoke ``ScryfallCatalogueIngestion``. The
   Scryfall ingestion path is fail-closed on its own
   ``market_import_runs`` row.

Return value describes what happened so the pipeline can incorporate it
into the summary + validation.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("mtg_stage1d.scryfall_delta")


@dataclass
class ScryfallDeltaOutcome:
    action:      str                  # 'unchanged' | 'invoked' | 'failed' | 'skipped_by_flag' | 'changed_deferred'
    remote_updated_at:  dict = field(default_factory=dict)
    local_updated_at:   dict = field(default_factory=dict)
    scryfall_run_id:    str | None = None
    status:      str | None = None    # scryfall_ingestion.finish status when invoked
    error:       str | None = None
    reason:      str | None = None    # populated when action='changed_deferred'


@dataclass
class ScryfallFreshness:
    """Result of the cheap freshness probe against Scryfall.

    ``changed`` is True iff any of oracle_cards / default_cards /
    rulings has a newer updated_at than what we last successfully
    ingested. ONE HTTP GET to Scryfall + ONE PostgREST call; safe to
    run on every daily job including the 18:00 fallback.
    """
    changed:            bool
    remote_updated_at:  dict
    local_updated_at:   dict


USER_AGENT = (
    "MTGPricesStage1D/1.0 (+https://www.pokeprices.io; contact@pokeprices.io)"
)


def _remote_updated_at() -> dict[str, str]:
    from urllib.request import Request, urlopen
    with urlopen(
        Request("https://api.scryfall.com/bulk-data",
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"}),
        timeout=60,
    ) as r:
        body = json.loads(r.read().decode("utf-8"))
    out: dict[str, str] = {}
    for item in body.get("data") or []:
        t = item.get("type")
        u = item.get("updated_at")
        if t and u:
            out[t] = u
    return out


def _last_scryfall_updated_at(supabase: Any) -> dict[str, str]:
    """Return the most recently ingested Scryfall updated_at map.

    Considers runs where the Scryfall ingest itself completed
    (status='success' or status='partial' with notes.errors==0).
    """
    if supabase is None:
        return {}
    code, body = supabase._req(
        "/rest/v1/market_import_runs"
        "?provider=eq.scryfall"
        "&status=in.(success,partial)"
        "&order=started_at.desc&limit=5"
        "&select=id,status,notes"
    )
    if code >= 400:
        return {}
    rows = json.loads(body) if body else []
    for r in rows:
        notes = r.get("notes")
        if isinstance(notes, str):
            try:
                notes = json.loads(notes)
            except json.JSONDecodeError:
                continue
        if not isinstance(notes, dict):
            continue
        # Partial with errors is not "completed" for this purpose.
        if r.get("status") == "partial" and (notes.get("errors") or 0) > 0:
            continue
        return {
            "oracle_cards":  notes.get("oracle_updated_at"),
            "default_cards": notes.get("default_updated_at"),
            "rulings":       notes.get("rulings_updated_at"),
        }
    return {}


def check_freshness(supabase: Any) -> ScryfallFreshness:
    """Cheap freshness probe. ONE Scryfall /bulk-data GET + ONE
    PostgREST call. Safe on every daily run including the 18:00
    fallback. Returns the raw diff; does NOT invoke the ingester.
    """
    remote = _remote_updated_at()
    local  = _last_scryfall_updated_at(supabase)

    def _same(k: str) -> bool:
        return (remote.get(k) or "") == (local.get(k) or "")

    changed = not (_same("oracle_cards") and _same("default_cards") and _same("rulings"))
    return ScryfallFreshness(changed=changed, remote_updated_at=remote, local_updated_at=local)


def invoke_ingester(supabase: Any, *, dry_run: bool,
                     freshness: ScryfallFreshness | None = None) -> ScryfallDeltaOutcome:
    """Invoke the existing Stage 1B ScryfallCatalogueIngestion.

    Callers should first check :func:`check_freshness` and only call
    this when ``freshness.changed`` is True (otherwise we do work for
    no reason). ``freshness`` is optional and only used to populate
    the outcome fields.
    """
    outcome = ScryfallDeltaOutcome(
        action="invoked",
        remote_updated_at=freshness.remote_updated_at if freshness else {},
        local_updated_at=freshness.local_updated_at if freshness else {},
    )
    try:
        import scryfall_ingestion as si
        # NOTE: scryfall_ingestion has its OWN SupabaseClient
        # implementation (requests-based, exposes lookup_id_map / etc.)
        # Do NOT pass the mtgjson_ingestion SupabaseClient — the two
        # classes are not compatible. Build the Scryfall-flavoured
        # client from env.
        scry_supabase = si.build_default_supabase_client() if not dry_run else None
        client = si.ScryfallClient()
        ing = si.ScryfallCatalogueIngestion(
            supabase=scry_supabase, scryfall=client, dry_run=dry_run,
            import_type="scraper_nightly",
        )
        ing.start()
        ing.run()
        status = "success" if ing.stats.errors == 0 else "partial"
        ing.finish(status=status)
        outcome.status = status
        outcome.scryfall_run_id = ing.run_id
    except Exception as e:
        log.exception("scryfall_delta: ingest failed")
        outcome.action = "failed"
        outcome.error = str(e)
    return outcome


def check_and_maybe_invoke(
    supabase: Any,
    *,
    dry_run: bool,
    skip: bool = False,
    defer_invocation: bool = False,
) -> ScryfallDeltaOutcome:
    """Legacy convenience: freshness check + optional invocation.

    * ``skip=True`` → returns ``skipped_by_flag`` without contacting
      Scryfall (used by ``--skip-scryfall``).
    * ``defer_invocation=True`` → runs the cheap freshness check but
      never invokes the ingester. When the source moved, returns
      ``action='changed_deferred'`` so the caller can log that the
      catalogue refresh is deferred to the next primary run.
    * Otherwise: invokes when freshness changed, mirrors the historical
      behaviour of this function.
    """
    if skip:
        return ScryfallDeltaOutcome(action="skipped_by_flag")

    freshness = check_freshness(supabase)
    outcome = ScryfallDeltaOutcome(
        action="unchanged",
        remote_updated_at=freshness.remote_updated_at,
        local_updated_at=freshness.local_updated_at,
    )
    if not freshness.changed:
        log.info("scryfall_delta: bulk metadata unchanged; skipping")
        return outcome

    log.info(
        "scryfall_delta: bulk moved (oracle: %s -> %s / default: %s -> %s / rulings: %s -> %s)",
        freshness.local_updated_at.get("oracle_cards"),  freshness.remote_updated_at.get("oracle_cards"),
        freshness.local_updated_at.get("default_cards"), freshness.remote_updated_at.get("default_cards"),
        freshness.local_updated_at.get("rulings"),       freshness.remote_updated_at.get("rulings"),
    )
    if defer_invocation:
        outcome.action = "changed_deferred"
        outcome.reason = "scryfall bulk moved but caller deferred invocation (typical on duplicate-build fallback)"
        return outcome

    invoked = invoke_ingester(supabase, dry_run=dry_run, freshness=freshness)
    return invoked
