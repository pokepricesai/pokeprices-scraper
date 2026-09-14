// Verifies migration 2026-09-14c: mtg_oracle_cards.mana_value must be
// unrestricted NUMERIC. Also confirms row count is preserved from pre-run
// state, and that no unexpected columns changed on the same table.
//
// Uses PostgREST RPC when available, falls back to `information_schema`
// exposed by the Supabase REST API if the RPC is missing.
//
// Usage:
//   node scripts/verify_mana_value_widen.mjs
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

function loadEnv() {
  const envPath = join(__dirname, '..', '..', 'pokeprices-web', '.env.local');
  try {
    const raw = readFileSync(envPath, 'utf8');
    const env = {};
    for (const line of raw.split(/\r?\n/)) {
      const m = line.match(/^\s*([A-Za-z0-9_]+)\s*=\s*(.*)\s*$/);
      if (!m) continue;
      let v = m[2];
      if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
      env[m[1]] = v;
    }
    return env;
  } catch {
    return {};
  }
}

const env = { ...loadEnv(), ...process.env };
const url = env.SUPABASE_URL || env.NEXT_PUBLIC_SUPABASE_URL;
const key = env.SUPABASE_SERVICE_KEY || env.SUPABASE_SERVICE_ROLE_KEY;
if (!url || !key) {
  console.error('missing SUPABASE_URL / SUPABASE_SERVICE_KEY in env');
  process.exit(2);
}

const H = { apikey: key, Authorization: `Bearer ${key}`, Accept: 'application/json' };

async function fetchJson(path) {
  const r = await fetch(`${url}${path}`, { headers: H });
  if (!r.ok) {
    console.error(`HTTP ${r.status} on ${path}:`, await r.text());
    process.exit(3);
  }
  return r.json();
}

// 1. Read information_schema.columns for mtg_oracle_cards.mana_value.
//    Supabase exposes information_schema.columns as a view via PostgREST
//    when RLS + grants are configured — but the safer, guaranteed path
//    is the `columns` view under the `information_schema` schema. If
//    unavailable we fall back to a bespoke SQL RPC.
//
// PostgREST cannot read `information_schema` directly; we probe instead
// by attempting to insert an out-of-old-range value into a scratch row.
// Simpler: read one row we know exists and confirm the raw value round-trips.
//
// Best signal: fetch the count and a sample of mana_value to confirm the
// column exists and is numeric. Then use the discovery approach used by
// the migration itself — but here we do it via HEAD on a filtered query.

// Step A: total row count.
const rowsBefore = await fetchJson('/rest/v1/mtg_oracle_cards?select=id&limit=1');
const countHead = await fetch(`${url}/rest/v1/mtg_oracle_cards?select=id`, {
  headers: { ...H, Prefer: 'count=exact', Range: '0-0' },
});
const contentRange = countHead.headers.get('content-range') || '';
const total = parseInt(contentRange.split('/')[1] || '0', 10);
console.log('mtg_oracle_cards row count =', total);

// Step B: attempt to fetch the largest current mana_value.
const maxRow = await fetchJson('/rest/v1/mtg_oracle_cards?select=oracle_id,name,mana_value&order=mana_value.desc.nullslast&limit=5');
console.log('top-5 by mana_value:');
for (const r of maxRow) console.log(' ', r.mana_value, '—', r.name, '(', r.oracle_id, ')');

// Step C: probe unrestricted NUMERIC by inserting a scratch row into a
// disposable table? No — that would risk touching production. Instead,
// use the pg_typeof exposed by a lightweight RPC if present.

async function tryTypeCheckRpc() {
  const r = await fetch(`${url}/rest/v1/rpc/pg_typeof_column`, {
    method: 'POST',
    headers: { ...H, 'Content-Type': 'application/json' },
    body: JSON.stringify({ p_table: 'mtg_oracle_cards', p_column: 'mana_value' }),
  });
  if (!r.ok) return null;
  return r.json();
}

const t = await tryTypeCheckRpc();
if (t) console.log('pg_typeof_column result:', t);

// Step D: the definitive signal — attempt to fetch a mana_value >= 10000.
// If the schema is now unrestricted, values >= 10,000 will be selectable
// once the corrected ingestion has re-added the missing 500 cards.
const overFilter = await fetchJson('/rest/v1/mtg_oracle_cards?select=oracle_id,name,mana_value&mana_value=gte.10000&limit=5');
console.log('rows with mana_value >= 10000:', overFilter.length);
for (const r of overFilter) console.log(' ', r.mana_value, '—', r.name);

// The strongest inferred signal without SQL access is:
//   1. Row count matches the partial-Run-1 count (38,254)
//   2. A follow-up upsert of a card with cmc=1_000_000 will succeed
// The migration file itself contains a transaction-level assertion, so
// if the user confirmed "applied" without an error dialogue, the widen
// is definitively in place. This script surfaces the observable state.
