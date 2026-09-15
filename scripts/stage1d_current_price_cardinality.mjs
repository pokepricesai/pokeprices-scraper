// Stage 1D sizing probe — READ-ONLY.
// How many distinct current-price keys exist in the observations table
// on the latest day, and how many across a broader window? This lets
// us pin mtg_current_prices row-count expectations.
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
const url = env.SUPABASE_URL || env.NEXT_PUBLIC_SUPABASE_URL;
const key = env.SUPABASE_SERVICE_KEY || env.SUPABASE_SERVICE_ROLE_KEY;
const H = { apikey: key, Authorization: `Bearer ${key}`, Accept: 'application/json' };

async function exactCount(filter, selectCol='observed_on') {
  const r = await fetch(`${url}/rest/v1/mtg_price_observations?${filter}&select=${selectCol}`,
    { headers: { ...H, Prefer: 'count=exact', Range: '0-0' } });
  return parseInt((r.headers.get('content-range') || '').split('/')[1] || '0', 10);
}

// A. Per-day counts across the last 7 days.
console.log('## Per-day row counts (last 10 days of bootstrap window)');
const daysDesc = ['2026-09-13','2026-09-12','2026-09-11','2026-09-10','2026-09-09','2026-09-08','2026-09-07','2026-09-06','2026-09-05','2026-09-04'];
for (const d of daysDesc) {
  const c = await exactCount(`observed_on=eq.${d}`);
  console.log(`  ${d} = ${c.toLocaleString()}`);
}

// B. Provider × price_type breakdown on the latest day — approximates the
//    number of DISTINCT current-price identities today.
console.log('\n## Latest-day provider × price_type');
for (const p of ['tcgplayer','cardmarket','cardkingdom','cardhoarder','manapool']) {
  for (const pt of ['retail','buylist']) {
    const c = await exactCount(`observed_on=eq.2026-09-13&provider=eq.${p}&price_type=eq.${pt}`);
    console.log(`  ${p.padEnd(12)} ${pt.padEnd(8)} = ${c.toLocaleString()}`);
  }
}

// C. Sum of daily counts across 7 buylist-active days — upper bound for
//    the union of current-price keys if every day sees the same keys.
console.log('\n## Aggregate over 2026-09-07..2026-09-13 (7 days) — provider-scoped');
for (const p of ['tcgplayer','cardmarket','cardkingdom','cardhoarder','manapool']) {
  const c = await exactCount(`observed_on=gte.2026-09-07&observed_on=lte.2026-09-13&provider=eq.${p}`);
  console.log(`  ${p.padEnd(12)} 7d rows = ${c.toLocaleString()}`);
}
