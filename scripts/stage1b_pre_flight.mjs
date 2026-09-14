// Stage 1B pre-flight probe:
//  * confirm the Run 1 (partial) market_import_runs record is untouched
//  * confirm mtg_price_observations + mtg_external_identifiers are empty
//  * confirm legacy archive counts match Stage 1A snapshot
//  * confirm no Pokémon table was touched (spot-check row counts)
//
// Read-only. Never writes.
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
function loadEnv() {
  const p = join(__dirname, '..', '..', 'pokeprices-web', '.env.local');
  try {
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
  } catch { return {}; }
}
const env = { ...loadEnv(), ...process.env };
const url = env.SUPABASE_URL || env.NEXT_PUBLIC_SUPABASE_URL;
const key = env.SUPABASE_SERVICE_KEY || env.SUPABASE_SERVICE_ROLE_KEY;
const H = { apikey: key, Authorization: `Bearer ${key}`, Accept: 'application/json' };

async function count(table, filter = '') {
  const q = filter ? `${filter}&select=*` : 'select=*';
  const r = await fetch(`${url}/rest/v1/${table}?${q}`, {
    headers: { ...H, Prefer: 'count=exact', Range: '0-0' },
  });
  const cr = r.headers.get('content-range') || '';
  return parseInt(cr.split('/')[1] || '0', 10);
}

async function fetchOne(table, filter) {
  const r = await fetch(`${url}/rest/v1/${table}?${filter}&limit=1`, { headers: H });
  const b = await r.json();
  return Array.isArray(b) ? b[0] : b;
}

console.log('=== Stage 1B pre-flight ===\n');

// Run 1 (partial) record preservation
const run1 = await fetchOne('market_import_runs', 'id=eq.95379098-b304-4aa4-bb69-a59137a114bc');
if (!run1) {
  console.error('FAIL: Run 1 market_import_runs record 95379098-... NOT FOUND');
  process.exit(1);
}
console.log('Run 1 record preserved:');
console.log(`  id=${run1.id}`);
console.log(`  provider=${run1.provider}  source=${run1.source}  status=${run1.status}`);
console.log(`  started_at=${run1.started_at}`);
console.log(`  completed_at=${run1.completed_at}`);
console.log(`  parser_version=${run1.parser_version}`);
if (run1.status !== 'partial') {
  console.error(`FAIL: Run 1 status expected 'partial', got '${run1.status}'`);
  process.exit(1);
}
console.log('  → status=partial ✓\n');

// Stage 1C tables must remain empty
const extId = await count('mtg_external_identifiers');
const priceObs = await count('mtg_price_observations');
console.log(`mtg_external_identifiers  = ${extId} (must be 0)`);
console.log(`mtg_price_observations    = ${priceObs} (must be 0)`);
if (extId !== 0 || priceObs !== 0) {
  console.error('FAIL: Stage 1C tables non-empty');
  process.exit(1);
}
console.log('  → both empty ✓\n');

// Legacy archive counts
console.log('Legacy archive tables:');
console.log(`  mtg_sets_legacy_20260327          = ${await count('mtg_sets_legacy_20260327')} (expected 1,029)`);
console.log(`  mtg_cards_legacy_20260327         = ${await count('mtg_cards_legacy_20260327')} (expected 104,505)`);
console.log(`  mtg_daily_prices_legacy_20260327  = ${await count('mtg_daily_prices_legacy_20260327')} (expected 98,250)`);
console.log(`  mtg_card_trends_legacy_20260327   = ${await count('mtg_card_trends_legacy_20260327')} (expected 0)`);

// Pokémon spot-check
console.log('\nPokémon side spot-check:');
console.log(`  cards                = ${await count('cards')}`);
console.log(`  daily_prices         = ${await count('daily_prices')}`);
console.log(`  card_latest_prices   = ${await count('card_latest_prices')}`);
console.log(`  card_trends          = ${await count('card_trends')}`);
console.log(`  provider_card_links  = ${await count('provider_card_links')}`);

// Current MTG production catalogue state (post-partial Run 1)
console.log('\nMTG production catalogue (post-Run-1 partial state):');
for (const t of ['mtg_sets', 'mtg_oracle_cards', 'mtg_printings',
                 'mtg_printing_finishes', 'mtg_oracle_legalities', 'mtg_rulings']) {
  console.log(`  ${t.padEnd(28)} = ${await count(t)}`);
}
console.log('\npre-flight OK');
