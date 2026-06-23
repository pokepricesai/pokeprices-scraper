# Recent-sales pilot — nightly allow-list job

**Block 4B-S-4A.** Scales the nightly allow-list pilot (originally Block
4B-S-3A, 58 cards) to 3,000 cards per run. This is still the *only*
automated recent-sales path in the scraper repo; full-catalogue ingestion
does not exist and is explicitly out of scope.

## What runs

`.github/workflows/recent-sales-pilot.yml` schedules
`scripts/run_recent_sales_pilot.py` once per night.

Command:

```
python scripts/run_recent_sales_pilot.py \
  --limit 3000 \
  --delay-seconds 1.0 \
  --max-retries 3 \
  --retry-backoff-seconds 10
```

Allow-list source: `public.recent_sales_card_allow_list` filtered to
`provider='pricecharting' AND enabled=TRUE`. The allow-list now contains
**17,949 enabled rows**, covering every PriceCharting card with
`raw_usd >= $5` (seeded as part of Block 4B-W-2A's expansion). The
`--limit 3000` flag caps a single nightly job at the first 3,000
allow-listed cards; the remaining ~15k allow-listed cards are **not**
processed in one job yet. Even without `--limit`, the runner only
processes cards present in the allow-list.

### Coverage status

| | count |
| --- | --- |
| Allow-list rows (provider=pricecharting, enabled) | 17,949 |
| Cards processed per nightly run (this workflow) | up to 3,000 |
| Cards processed per nightly run outside allow-list | 0 |

The full 17,949-card allow-list is **not** processed in a single nightly
job today. The next step (separate block) is to introduce rotating weekly
batches — each scheduled run picks a different slice of the allow-list so
that every allow-listed card refreshes on a fixed cadence. Until that
lands, only the first 3,000 cards (by allow-list ordering) refresh each
night.

## Schedule

`cron: '0 13 * * *'` (13:00 UTC, every day).

The existing `nightly-scrape` workflow starts at 08:00 UTC and typically
finishes (including refresh-and-analytics, detect-deals, and
nightly-analytics) by ~12:00 UTC. 13:00 UTC leaves ~1 hour of headroom
before the pilot kicks off — no risk of contention with the main scrape.

At `--delay-seconds 1.0` the happy path for 3,000 cards is ~50 minutes
of fetch pacing alone. The job has a 180-minute `timeout-minutes` budget
to absorb per-card parse/upsert cost plus a moderate rate of retries; a
mass-throttle event still surfaces as a job timeout rather than silently
absorbing latency.

## Required GitHub secrets

Both already configured (used by other workflows — no new secrets needed):

- `SUPABASE_URL` — Supabase project URL.
- `SUPABASE_SERVICE_KEY` — Supabase service-role key (write-permissioned;
  used elsewhere in `nightly-scrape.yml`'s `nightly-analytics` job).

The workflow also sets `RECENT_SALES_INGESTION_ENABLED=true` at the
workflow `env` level. The ingestion module enforces a strict-equals check
on that string, so flipping the value is the single-line disable switch
(see below).

## How to disable

In **increasing** order of friction:

1. **Flip the env value in the workflow file** to anything other than
   `"true"` (e.g. `"false"`) and commit. The ingestion module's
   `is_ingestion_enabled()` then returns `False`; `init_for_scraper_run()`
   returns `None` before any Supabase HTTP call; the runner exits
   successfully with no writes. **Reversible by changing the value back.**
2. **Pause the workflow** in the GitHub Actions UI (Actions →
   "Recent-sales Pilot (Allow-list Only)" → `⋯` → Disable workflow). No
   commit needed; the next scheduled run is skipped.
3. **Delete the workflow file** and commit. Permanent; only do this if
   the pilot is being retired entirely.

The first two options are reversible without touching `recent_sales`
or `market_import_runs`. The DB rows from any prior successful run stay.

## How to trigger manually

GitHub Actions UI → "Recent-sales Pilot (Allow-list Only)" → **Run
workflow** → pick `main`. Triggers the same command via
`workflow_dispatch`. The concurrency group blocks the manual run from
overlapping with a scheduled run (queued, not cancelled).

To do a dry-run dispatch without touching the YAML, run the script
locally with `--dry-run`; the workflow itself only runs the WRITE path.

## Expected output

Successful nightly run (GitHub Actions log):

```
allow-list loaded: 17949 card_ids (provider=pricecharting, enabled=true)
loading PriceCharting card catalogue from /home/runner/work/.../pc_csvs
allow-list matches in CSVs: 17949/17949
--limit 3000 applied; processing 3000 cards
[1/3000] fetching pcid=… …
[2/3000] fetching pcid=… …
…
pilot done. mode=WRITE allow_listed=17949 fetched=3000 parsed=3000 ok=N quarantined=K
   rejected=0 upserted=N errors=0 skipped_no_html=0 skipped_429=0 skipped_http_error=0
```

Database side (one row per run):

- `public.market_import_runs`
  - `source='pilot'`, `status='success'`
  - `pages_processed=3000` (or fewer if some cards were skipped)
  - `duration_ms` populated
  - `notes` JSON carrying `import_type='recent_sales_pilot'`,
    `cards_allowlisted`, `cards_parsed`, `rows_upserted`,
    `errors_count`, `fetched`, `skipped_no_html`, `skipped_429`,
    `skipped_http_error`
- `public.recent_sales` — rows for each parsed OK sale, linked by
  `import_run_id` to the run above.

## How to inspect results

Two paths, neither requires logging into the scraper:

1. **Admin view in the web repo** (gated by `RECENT_SALES_ADMIN_VIEW_ENABLED`
   on the web side) — recommended for routine spot-checks.
2. **SQL Editor in Supabase** — use the verification queries in the
   Block 4B-S-2A patch report (the `(a)`–`(f)` queries). The most useful
   one-liner is:
   ```sql
   select id, source, status, pages_processed, rows_ok, rows_quarantined,
          rows_rejected, duration_ms, notes::jsonb
   from market_import_runs
   where source = 'pilot'
   order by started_at desc
   limit 5;
   ```

## What this workflow does NOT do

- **Does NOT enable full-catalogue ingestion.** The runner is hard-coded
  to consult `recent_sales_card_allow_list` first; cards outside it are
  no-ops at the per-card hook. No `RECENT_SALES_FULL_CATALOGUE` flag is
  introduced.
- **Does NOT touch the nightly price scraper.** `nightly-scrape.yml` and
  its jobs (`batch1`…`batch6`, `refresh-and-analytics`, `detect-deals`,
  `nightly-analytics`) are unchanged.
- **Does NOT change `recent_sales_parser.py`.** Block 4A-S1 parser is
  frozen.
- **Does NOT change the web repo.**
- **Does NOT add public UI.** Surface remains admin-only.

## Block boundaries

- This scraper repo owns: the parser, the ingestion module, the manual
  runner, this workflow.
- The web repo (Block 4B-W-1 / 4B-W-2A) owns: the migration, the
  allow-list seed, the admin inspection view.
- Future blocks own: per-card review queue actions, public surfacing,
  rotating weekly batches so every allow-listed card refreshes on a fixed
  cadence (the remaining ~15k cards beyond tonight's 3,000-card slice),
  and any future full-catalogue rollout.
