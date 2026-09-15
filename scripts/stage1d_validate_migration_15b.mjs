// scripts/stage1d_validate_migration_15b.mjs
//
// Live validation for migration 2026-09-15b (mtg_current_prices +
// refresh_mtg_current_prices function).
//
// Read-only-ish: it makes a shape-only RPC call
// (look_back_days=0 → no rows in window → zero writes) to prove the
// function is executable by service_role and returns the expected
// summary row shape. It also probes anon rejection of both table and
// function.
//
// It does NOT run the initial-population look_back_days=30 call —
// that is Gate 4 in the pipeline gates and belongs to the manual
// walkthrough, not the migration validation.

import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
function loadEnv() {
  const p = join(__dirname, '..', '..', 'pokeprices-web', '.env.local');
  const raw = readFileSync(p, 'utf8');
  const e = {};
  for (const line of raw.split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Za-z0-9_]+)\s*=\s*(.*)\s*$/);
    if (!m) continue;
    let v = m[2];
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
    e[m[1]] = v;
  }
  return e;
}
const env = { ...loadEnv(), ...process.env };
const URL = env.SUPABASE_URL || env.NEXT_PUBLIC_SUPABASE_URL;
const SERVICE_KEY = env.SUPABASE_SERVICE_KEY || env.SUPABASE_SERVICE_ROLE_KEY;
const ANON_KEY = env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
if (!URL || !SERVICE_KEY || !ANON_KEY) {
  console.error('missing SUPABASE_URL / SERVICE_KEY / ANON_KEY');
  process.exit(2);
}

const HS = { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` };
const HA = { apikey: ANON_KEY,    Authorization: `Bearer ${ANON_KEY}`   };

let pass = 0, fail = 0;
function check(cond, label, detail = '') {
  if (cond) { pass += 1; console.log(`  ✓ ${label}`); }
  else      { fail += 1; console.log(`  ✗ ${label}${detail ? '  — ' + detail : ''}`); }
}

async function fetchJson(url, headers, opts = {}) {
  const r = await fetch(url, { headers: { ...headers, Accept: 'application/json' }, ...opts });
  const text = await r.text();
  let body = text;
  try { body = text ? JSON.parse(text) : null; } catch {}
  return { status: r.status, body, contentRange: r.headers.get('content-range') };
}

console.log('=== Stage 1D — validate migration 2026-09-15b (LIVE) ===\n');

// ─── 1. Table exists + shape + starts empty ────────────────────────────────
console.log('## mtg_current_prices — table + shape');
{
  const r = await fetchJson(
    `${URL}/rest/v1/mtg_current_prices?select=printing_finish_id,provider,market,currency,price_type,condition,price,observed_on,ingestion_source,updated_at&limit=0`,
    { ...HS, Prefer: 'count=exact', Range: '0-0' },
  );
  check(r.status === 200, 'GET succeeds with expected columns (service_role)', `HTTP ${r.status}`);
  const total = parseInt((r.contentRange || '').split('/')[1] || '-1', 10);
  check(total === 0, 'table starts empty (Stage 1D initial population is Gate 4)',
        `content-range total=${total}`);
}

// ─── 2. RLS: anon SELECT rejected/empty ────────────────────────────────────
console.log('\n## RLS — anon has no read access');
{
  const r = await fetchJson(`${URL}/rest/v1/mtg_current_prices?select=printing_finish_id&limit=1`, HA);
  const rowsIfAny = Array.isArray(r.body) ? r.body.length : -1;
  check(
    (r.status === 200 && rowsIfAny === 0) || r.status === 401 || r.status === 403,
    'anon cannot read rows',
    `HTTP ${r.status} rows=${rowsIfAny} body=${JSON.stringify(r.body).slice(0,180)}`,
  );
}

// ─── 3. RLS: anon INSERT rejected ──────────────────────────────────────────
console.log('\n## RLS — anon has no write access');
{
  const r = await fetch(`${URL}/rest/v1/mtg_current_prices`, {
    method: 'POST',
    headers: { ...HA, 'Content-Type': 'application/json', Prefer: 'return=minimal' },
    body: JSON.stringify([{
      printing_finish_id: '00000000-0000-0000-0000-000000000000',
      provider: 'anon-probe',
      currency: 'USD',
      price: 1.23,
      observed_on: '2026-09-15',
      ingestion_source: 'anon-probe',
    }]),
  });
  check(r.status >= 400, 'anon INSERT rejected', `HTTP ${r.status}`);
}

// ─── 4. Service_role INSERT + DELETE probe ────────────────────────────────
// Uses a fake printing_finish_id — the FK would normally reject that.
// So we skip the INSERT round-trip on this table (any real value would
// touch a real printing_finish; we want a non-mutating validation).
// The empty-then-empty state proves service_role has SELECT, and the
// RPC section below proves service_role can write via the reducer.

// ─── 5. RPC exists + service_role can execute + shape ─────────────────────
console.log('\n## refresh_mtg_current_prices — shape + service_role EXECUTE');
{
  // look_back_days=0: since = CURRENT_DATE - 0 = today. There are no
  // observations dated today yet (Stage 1C stops at 2026-09-13), so
  // the reducer scans zero rows and returns a summary with
  // keys_upserted=0. This proves executability + shape without
  // populating the table.
  const r = await fetch(`${URL}/rest/v1/rpc/refresh_mtg_current_prices`, {
    method: 'POST',
    headers: { ...HS, 'Content-Type': 'application/json' },
    body: JSON.stringify({ look_back_days: 0 }),
  });
  const text = await r.text();
  let body = text;
  try { body = text ? JSON.parse(text) : null; } catch {}
  check(r.status === 200, 'service_role RPC returns 200', `HTTP ${r.status} body=${JSON.stringify(body).slice(0,240)}`);
  check(Array.isArray(body) && body.length === 1, 'RPC returns exactly one summary row',
        `type=${Array.isArray(body) ? 'array len=' + body.length : typeof body}`);
  if (Array.isArray(body) && body.length === 1) {
    const row = body[0];
    const keys = Object.keys(row).sort();
    const expected = ['distinct_finishes','keys_upserted','latest_observed_on','provider_breakdown'].sort();
    check(JSON.stringify(keys) === JSON.stringify(expected),
          'summary columns match: ' + expected.join(','),
          'got: ' + keys.join(','));
    check(row.keys_upserted === 0, 'look_back_days=0 upserts zero rows',
          `keys_upserted=${row.keys_upserted}`);
    check(row.latest_observed_on === null, 'latest_observed_on is null on empty table',
          `latest_observed_on=${row.latest_observed_on}`);
  }
}

// ─── 6. RPC anon rejected ─────────────────────────────────────────────────
console.log('\n## refresh_mtg_current_prices — anon rejected');
{
  const r = await fetch(`${URL}/rest/v1/rpc/refresh_mtg_current_prices`, {
    method: 'POST',
    headers: { ...HA, 'Content-Type': 'application/json' },
    body: JSON.stringify({ look_back_days: 3 }),
  });
  const text = await r.text();
  check(r.status >= 400, 'anon RPC call rejected',
        `HTTP ${r.status} body=${text.slice(0,240)}`);
}

// ─── 7. Bad-argument guard: negative look_back_days ────────────────────────
console.log('\n## refresh_mtg_current_prices — bad-argument guard');
{
  const r = await fetch(`${URL}/rest/v1/rpc/refresh_mtg_current_prices`, {
    method: 'POST',
    headers: { ...HS, 'Content-Type': 'application/json' },
    body: JSON.stringify({ look_back_days: -1 }),
  });
  const text = await r.text();
  check(r.status >= 400, 'RPC rejects look_back_days < 0',
        `HTTP ${r.status} body=${text.slice(0,240)}`);
}

// ─── 8. mtg_current_prices remains empty after the shape-only RPC ─────────
console.log('\n## mtg_current_prices unchanged after shape probes');
{
  const r = await fetchJson(
    `${URL}/rest/v1/mtg_current_prices?select=printing_finish_id&limit=0`,
    { ...HS, Prefer: 'count=exact', Range: '0-0' },
  );
  const total = parseInt((r.contentRange || '').split('/')[1] || '-1', 10);
  check(total === 0, 'table still empty (Gate 4 will populate it)',
        `content-range total=${total}`);
}

// ─── 9. Regression: 15a lock table still healthy ──────────────────────────
console.log('\n## 15a regression — mtg_daily_ingest_locks still healthy');
{
  const r = await fetchJson(
    `${URL}/rest/v1/mtg_daily_ingest_locks?select=lock_key&limit=0`,
    { ...HS, Prefer: 'count=exact', Range: '0-0' },
  );
  const total = parseInt((r.contentRange || '').split('/')[1] || '-1', 10);
  check(r.status === 200, 'lock table reachable', `HTTP ${r.status}`);
  check(total === 0, 'lock table empty',
        `content-range total=${total}`);

  // ensure_mtg_price_partition still works
  const r2 = await fetch(`${URL}/rest/v1/rpc/ensure_mtg_price_partition`, {
    method: 'POST',
    headers: { ...HS, 'Content-Type': 'application/json' },
    body: JSON.stringify({ target_month: '2026-09-01' }),
  });
  const t2 = await r2.text();
  check(r2.status === 200 && t2.includes('exists:'),
        'ensure_mtg_price_partition still returns exists:',
        `HTTP ${r2.status} body=${t2.slice(0,180)}`);
}

console.log(`\nresults: pass=${pass} fail=${fail}`);
process.exit(fail === 0 ? 0 : 1);
