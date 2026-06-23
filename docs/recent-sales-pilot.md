# Recent-sales pilot — nightly allow-list job

**Block 4B-S-5A.** Rotates the 3,000-card allow-list pilot (Block 4B-S-4A)
across the week so the full 17,949-row allow-list is refreshed every
seven days. This is still the *only* automated recent-sales path in the
scraper repo; full-catalogue ingestion does not exist and is explicitly
out of scope.

## What runs

`.github/workflows/recent-sales-pilot.yml` schedules
`scripts/run_recent_sales_pilot.py` once per night. The workflow first
computes a day-of-week `OFFSET` in a shell step (see "Weekly rotation"
below) and passes it to the runner.

Effective command (Wednesday shown):

```
python scripts/run_recent_sales_pilot.py \
  --offset 6000 \
  --limit 3000 \
  --delay-seconds 1.0 \
  --max-retries 3 \
  --retry-backoff-seconds 10
```

Allow-list source: `public.recent_sales_card_allow_list` filtered to
`provider='pricecharting' AND enabled=TRUE`. The allow-list contains
**17,949 enabled rows**, covering every PriceCharting card with
`raw_usd >= $5` (seeded as part of Block 4B-W-2A's expansion). Even with
no `--limit`, the runner only processes cards present in the allow-list
— the `--offset` flag selects a *slice within the allow-list*, never
outside it.

## Weekly rotation

The runner sorts the allow-list deterministically (numeric ascending on
`provider_card_id` when all entries are pure digits — they are today —
otherwise lexicographic), then takes `--limit` rows starting at
`--offset`. The workflow derives `OFFSET` from the UTC weekday:

| UTC day | `date -u +%u` | `--offset` | Rows covered |
| --- | --- | ---: | --- |
| Monday    | 1 |      0 | 1 – 3000 |
| Tuesday   | 2 |   3000 | 3001 – 6000 |
| Wednesday | 3 |   6000 | 6001 – 9000 |
| Thursday  | 4 |   9000 | 9001 – 12000 |
| Friday    | 5 |  12000 | 12001 – 15000 |
| Saturday  | 6 |  15000 | 15001 – 17949 (partial tail, 2949 cards) |
| Sunday    | 7 |      0 | 1 – 3000 (high-value head re-refresh) |

Mon–Sat together cover all 17,949 rows exactly once. Sunday is pinned
to offset 0 so the high-value head of the allow-list (lowest
`provider_card_id`s by sort order) gets a second refresh inside the
seven-day window.

### Batch size

3,000 cards per nightly run. This number is set in two places that must
stay in sync:

- The workflow command (`--limit 3000`).
- The day-of-week → offset arithmetic in the workflow's "Compute weekly
  rotation offset" step (`OFFSET=$(( (DOW - 1) * 3000 ))`).

If you change the batch size, update both lines. The runner accepts
`--batch-size` as an alias for `--limit` so local runs can use the
clearer name (`--batch-size 3000`).

### Offset logic in the runner

- `--offset N` is 0-based into the sorted allow-list.
- Values `>=` allow-list size wrap via modulo (`effective_offset = N % total`).
  Negative offsets are also modulo-normalised. This keeps the runner
  usable even if the workflow's date math overshoots the list size.
- When `effective_offset + limit > total`, the slice is the partial tail
  (no wrap *within* the batch — that keeps the contract simple: one
  contiguous slice per run). This is the documented Saturday behaviour.
- An empty allow-list returns an empty slice and exits successfully.

The rotation context (`allow_list_total`, `offset`, `effective_offset`,
`batch_size`, `selected_start`, `selected_end`) is recorded inside
`market_import_runs.notes` for every run — no schema change, just extra
JSON keys.

### How to manually run a specific batch

Three options, in increasing order of friction:

1. **`workflow_dispatch` from GitHub Actions UI** — Actions →
   "Recent-sales Pilot (Allow-list Only)" → **Run workflow**. The
   dispatched run still calls the same shell step, so it picks the
   *today's* offset; useful for re-running the current day's slice but
   not for picking a specific past day.
2. **Local invocation with explicit flags** — bypasses the workflow's
   day-of-week step entirely:
   ```
   $env:SUPABASE_URL="..."; $env:SUPABASE_SERVICE_KEY="..."
   $env:RECENT_SALES_INGESTION_ENABLED="true"
   python scripts/run_recent_sales_pilot.py `
     --offset 9000 --batch-size 3000 `
     --delay-seconds 1.0 --max-retries 3 --retry-backoff-seconds 10
   ```
   Pick any `--offset` (e.g. `9000` for Thursday's slice). Use
   `--dry-run` to parse without writing.
3. **Temporary workflow edit + dispatch** — change the `OFFSET=...` line
   in the workflow, commit, dispatch. Revert afterwards.

### Coverage status

| | count |
| --- | --- |
| Allow-list rows (provider=pricecharting, enabled) | 17,949 |
| Cards processed per nightly run | up to 3,000 |
| Cards processed per nightly run outside allow-list | 0 |
| Days to cover full allow-list once | 6 (Mon–Sat) |
| Days in the rotation cycle | 7 (Sun re-refreshes the head) |

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
`workflow_dispatch`, *including* the day-of-week offset shell step — so
a manual dispatch processes today's slice, not a forced first batch. The
concurrency group blocks the manual run from overlapping with a
scheduled run (queued, not cancelled).

To force a specific batch other than today's, run the script locally
with `--offset N --batch-size 3000` (see "How to manually run a specific
batch" under Weekly rotation above). To do a dry-run dispatch without
touching the YAML, run the script locally with `--dry-run`; the
workflow itself only runs the WRITE path.

## Expected output

Successful nightly run, Wednesday (GitHub Actions log):

```
UTC weekday=3 → offset=6000
allow-list loaded: 17949 card_ids (provider=pricecharting, enabled=true)
loading PriceCharting card catalogue from /home/runner/work/.../pc_csvs
allow-list matches in CSVs: 17949/17949
rotation: allow_list_total=17949 offset=6000 effective_offset=6000 batch_size=3000 selected=3000 (rows 6001-9000)
[1/3000] fetching pcid=… …
[2/3000] fetching pcid=… …
…
pilot done. mode=WRITE allow_listed=17949 fetched=3000 parsed=3000 ok=N quarantined=K
   rejected=0 upserted=N errors=0 skipped_no_html=0 skipped_429=0 skipped_http_error=0
```

Database side (one row per run):

- `public.market_import_runs`
  - `source='pilot'`, `status='success'`
  - `pages_processed=3000` (or fewer if some cards were skipped;
    Saturday's tail batch is 2949 rather than 3000)
  - `duration_ms` populated
  - `notes` JSON carrying `import_type='recent_sales_pilot'`,
    `cards_allowlisted`, `cards_parsed`, `rows_upserted`,
    `errors_count`, `fetched`, `skipped_no_html`, `skipped_429`,
    `skipped_http_error`, **and the rotation keys**
    `allow_list_total`, `offset`, `effective_offset`, `batch_size`,
    `selected_start`, `selected_end`
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
  no-ops at the per-card hook. `--offset` slices *within* the
  allow-list — it cannot reach a non-allow-listed card. No
  `RECENT_SALES_FULL_CATALOGUE` flag is introduced.
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
  any cadence tuning beyond today's weekly rotation, and any future
  full-catalogue rollout (currently out of scope).
