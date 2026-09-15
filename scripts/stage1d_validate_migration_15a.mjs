// scripts/stage1d_validate_migration_15a.mjs
//
// Live validation for migration 2026-09-15a
// (mtg_daily_ingest_locks + ensure_mtg_price_partition).
//
// Read-only-ish: it inserts + deletes ONE probe row from
// mtg_daily_ingest_locks to prove the lock flow works, and calls the
// partition helper with a target month that is expected to already
// exist. It does not create any new partitions in production; a
// missing month is created + kept (idempotent, harmless).
//
// Exit code 0 = all checks passed. Non-zero = at least one failure.

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

console.log('=== Stage 1D — validate migration 2026-09-15a (LIVE) ===\n');

// ─── 1. Table exists + shape (service_role GET) ────────────────────────────
console.log('## mtg_daily_ingest_locks — table + shape');
{
  const r = await fetchJson(
    `${URL}/rest/v1/mtg_daily_ingest_locks?select=lock_key,acquired_at,acquired_by,heartbeat_at,expected_release_by,metadata&limit=0`,
    { ...HS, Prefer: 'count=exact', Range: '0-0' },
  );
  check(r.status === 200, 'GET succeeds (service_role)', `HTTP ${r.status}`);
  const total = parseInt((r.contentRange || '').split('/')[1] || '-1', 10);
  check(total === 0, 'table starts empty', `content-range total=${total}`);
}

// ─── 2. RLS: anon SELECT returns zero rows (no policy grants access) ──────
console.log('\n## RLS — anon has no read access');
{
  const r = await fetchJson(
    `${URL}/rest/v1/mtg_daily_ingest_locks?select=lock_key&limit=1`,
    HA,
  );
  // Two acceptable shapes:
  //   * 200 with empty array (RLS enabled, no policy → invisible to anon)
  //   * 401/403 (revoke-based rejection)
  // A 200 with rows would indicate a leak.
  const rowsIfAny = Array.isArray(r.body) ? r.body.length : -1;
  check(
    (r.status === 200 && rowsIfAny === 0) || r.status === 401 || r.status === 403,
    'anon cannot read rows',
    `HTTP ${r.status} rows=${rowsIfAny} body=${JSON.stringify(r.body).slice(0, 200)}`,
  );
}

// ─── 3. RLS: anon INSERT rejected ──────────────────────────────────────────
console.log('\n## RLS — anon has no write access');
{
  const r = await fetch(`${URL}/rest/v1/mtg_daily_ingest_locks`, {
    method: 'POST',
    headers: { ...HA, 'Content-Type': 'application/json', Prefer: 'return=minimal' },
    body: JSON.stringify([{
      lock_key: '__anon_probe__',
      acquired_by: 'anon-probe',
      expected_release_by: new Date(Date.now() + 3_600_000).toISOString(),
    }]),
  });
  check(r.status >= 400, 'anon INSERT rejected', `HTTP ${r.status}`);
}

// ─── 4. Service_role INSERT + release round-trip ──────────────────────────
console.log('\n## Lock round-trip (service_role)');
const probeKey = `__stage1d_validate_${Date.now()}__`;
{
  const now = new Date();
  const release = new Date(now.getTime() + 3_600_000);
  const r = await fetch(`${URL}/rest/v1/mtg_daily_ingest_locks?on_conflict=lock_key`, {
    method: 'POST',
    headers: { ...HS, 'Content-Type': 'application/json',
               Prefer: 'resolution=ignore-duplicates,return=representation' },
    body: JSON.stringify([{
      lock_key: probeKey,
      acquired_by: 'stage1d-validate',
      heartbeat_at: now.toISOString(),
      expected_release_by: release.toISOString(),
      metadata: { probe: true },
    }]),
  });
  const text = await r.text();
  const rows = text ? JSON.parse(text) : [];
  check(r.status === 201 && rows.length === 1, 'service_role INSERT ON CONFLICT DO NOTHING succeeds', `HTTP ${r.status} rows=${rows.length}`);

  // Attempting a second insert should return 201 + empty array (conflict swallowed).
  const r2 = await fetch(`${URL}/rest/v1/mtg_daily_ingest_locks?on_conflict=lock_key`, {
    method: 'POST',
    headers: { ...HS, 'Content-Type': 'application/json',
               Prefer: 'resolution=ignore-duplicates,return=representation' },
    body: JSON.stringify([{
      lock_key: probeKey,
      acquired_by: 'stage1d-validate-2',
      expected_release_by: release.toISOString(),
    }]),
  });
  const text2 = await r2.text();
  const rows2 = text2 ? JSON.parse(text2) : [];
  check(r2.status === 201 && rows2.length === 0, 'second INSERT returns empty array (conflict swallowed)', `HTTP ${r2.status} rows=${rows2.length}`);

  // Release
  const r3 = await fetch(
    `${URL}/rest/v1/mtg_daily_ingest_locks?lock_key=eq.${probeKey}&acquired_by=eq.stage1d-validate`,
    { method: 'DELETE', headers: { ...HS, Prefer: 'return=minimal' } },
  );
  check(r3.status === 204, 'service_role DELETE releases the lock', `HTTP ${r3.status}`);

  // Confirm empty again
  const r4 = await fetchJson(
    `${URL}/rest/v1/mtg_daily_ingest_locks?select=lock_key&lock_key=eq.${probeKey}`,
    HS,
  );
  check(r4.status === 200 && Array.isArray(r4.body) && r4.body.length === 0,
        'probe row is gone after release');
}

// ─── 5. ensure_mtg_price_partition — service_role RPC ─────────────────────
console.log('\n## ensure_mtg_price_partition — service_role');
async function callEnsure(headers, target_month) {
  const r = await fetch(`${URL}/rest/v1/rpc/ensure_mtg_price_partition`, {
    method: 'POST',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({ target_month }),
  });
  const text = await r.text();
  let body = text;
  try { body = text ? JSON.parse(text) : null; } catch {}
  return { status: r.status, body };
}
{
  // Pick a month we know already exists from Stage 1C bootstrap.
  const r = await callEnsure(HS, '2026-09-01');
  check(r.status === 200 && typeof r.body === 'string' && r.body.startsWith('exists:'),
        'existing partition returns "exists:…"',
        `HTTP ${r.status} body=${JSON.stringify(r.body)}`);
}
{
  // Now the current + next month per rolling buffer. Either exists: or created:.
  const now = new Date();
  const iso1 = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-01`;
  const r = await callEnsure(HS, iso1);
  check(r.status === 200 && typeof r.body === 'string' && /^(exists|created):/.test(r.body),
        `current-month idempotent (${iso1})`,
        `HTTP ${r.status} body=${JSON.stringify(r.body)}`);

  const nextY = now.getUTCMonth() === 11 ? now.getUTCFullYear() + 1 : now.getUTCFullYear();
  const nextM = now.getUTCMonth() === 11 ? 1 : now.getUTCMonth() + 2;
  const iso2 = `${nextY}-${String(nextM).padStart(2, '0')}-01`;
  const r2 = await callEnsure(HS, iso2);
  check(r2.status === 200 && typeof r2.body === 'string' && /^(exists|created):/.test(r2.body),
        `next-month idempotent (${iso2})`,
        `HTTP ${r2.status} body=${JSON.stringify(r2.body)}`);

  // Re-call same month → must return "exists:"
  const r3 = await callEnsure(HS, iso2);
  check(r3.status === 200 && typeof r3.body === 'string' && r3.body.startsWith('exists:'),
        `next-month second call returns "exists:…"`,
        `HTTP ${r3.status} body=${JSON.stringify(r3.body)}`);
}

// ─── 6. ensure_mtg_price_partition — anon rejected ────────────────────────
console.log('\n## ensure_mtg_price_partition — anon rejected');
{
  const r = await callEnsure(HA, '2026-09-01');
  // PostgREST returns 401/403 or a 42501 permission_denied (returned as 400/500).
  check(r.status >= 400, 'anon RPC call rejected', `HTTP ${r.status} body=${JSON.stringify(r.body).slice(0,200)}`);
}

// ─── 7. Anon reads on the raw + printings tables still work (regression) ──
console.log('\n## Regression — anon read on existing public tables still works');
{
  const r = await fetchJson(`${URL}/rest/v1/mtg_sets?select=code&limit=1`, HA);
  check(r.status === 200 && Array.isArray(r.body) && r.body.length === 1,
        'anon can still read mtg_sets',
        `HTTP ${r.status}`);
}

console.log(`\nresults: pass=${pass} fail=${fail}`);
process.exit(fail === 0 ? 0 : 1);
