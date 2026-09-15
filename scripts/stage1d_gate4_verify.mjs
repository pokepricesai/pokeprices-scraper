// scripts/stage1d_gate4_verify.mjs
// Verify mtg_current_prices state after the initial population landed
// server-side despite the Kong-504 on the client.

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
const KEY = env.SUPABASE_SERVICE_KEY || env.SUPABASE_SERVICE_ROLE_KEY;
const H = { apikey: KEY, Authorization: `Bearer ${KEY}`, Accept: 'application/json' };

async function exactCount(filter) {
  const q = filter ? `${filter}&select=printing_finish_id` : 'select=printing_finish_id';
  const r = await fetch(`${URL}/rest/v1/mtg_current_prices?${q}`, {
    headers: { ...H, Prefer: 'count=exact', Range: '0-0' },
  });
  return parseInt((r.headers.get('content-range') || '').split('/')[1] || '0', 10);
}

async function fetchJson(path) {
  const r = await fetch(`${URL}${path}`, { headers: H });
  return r.json();
}

console.log('=== Gate 4 verification: mtg_current_prices state ===\n');

const total = await exactCount('');
console.log(`## Total rows: ${total.toLocaleString()}`);

console.log('\n## Per-provider row counts');
for (const p of ['tcgplayer','cardmarket','cardkingdom','cardhoarder','manapool']) {
  const c = await exactCount(`provider=eq.${p}`);
  console.log(`  ${p.padEnd(12)} = ${c.toLocaleString()}`);
}

console.log('\n## Latest observed_on distribution (top 10)');
// Use paged fetches (count via GROUP BY isn't supported by PostgREST GET,
// so approximate with per-date exact counts).
for (const d of ['2026-09-13','2026-09-12','2026-09-11','2026-09-10','2026-09-09','2026-09-08','2026-09-07','2026-09-06','2026-09-05','2026-09-04','2026-08-30','2026-08-15','2026-08-01']) {
  const c = await exactCount(`observed_on=eq.${d}`);
  console.log(`  ${d} = ${c.toLocaleString()}`);
}

console.log('\n## Distinct printing_finish_id count (via limit-based estimate)');
// Fetch the earliest + latest observed_on values.
const earliest = await fetchJson('/rest/v1/mtg_current_prices?select=observed_on&order=observed_on.asc&limit=1');
const latest   = await fetchJson('/rest/v1/mtg_current_prices?select=observed_on&order=observed_on.desc&limit=1');
console.log(`  earliest observed_on = ${earliest[0]?.observed_on}`);
console.log(`  latest observed_on   = ${latest[0]?.observed_on}`);

console.log('\n## Sample rows (first 3)');
const sample = await fetchJson('/rest/v1/mtg_current_prices?select=*&limit=3');
for (const row of sample) {
  console.log(`  finish=${row.printing_finish_id.slice(0,8)}... provider=${row.provider} market=${row.market||'∅'} currency=${row.currency} price_type=${row.price_type||'∅'} price=${row.price} observed_on=${row.observed_on}`);
}
