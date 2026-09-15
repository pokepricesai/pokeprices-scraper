"""Stage 1D — minimal daily MTG ingest support modules.

Submodules:
    lock          — DB-backed run gate.
    partitions    — ensure current + next monthly partition exists.
    build_guard   — same-build MTGJSON dedup guard.
    current_prices — refresh mtg_current_prices from touched keys.
    gap_repair    — 14-day gap detection + weekly AllPrices repair.
    validation    — warn vs hard-fail routing after a run.
"""
