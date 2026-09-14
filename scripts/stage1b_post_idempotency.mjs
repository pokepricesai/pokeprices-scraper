// Stage 1B — post-idempotency verification.
// Expected: catalogue counts unchanged between Run 2 and Run 3; Run 3
// record recorded as success; legacy archives + Pokémon side untouched.
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
async function count(table, filter = '') {
  const q = filter ? `${filter}&select=*` : 'select=*';
  const r = await fetch(`${url}/rest/v1/${table}?${q}`, { headers: { ...H, Prefer: 'count=exact', Range: '0-0' } });
  return parseInt((r.headers.get('content-range') || '').split('/')[1] || '0', 10);
}
async function fetchOne(table, filter) {
  const r = await fetch(`${url}/rest/v1/${table}?${filter}&limit=1`, { headers: H });
  const b = await r.json();
  return Array.isArray(b) ? b[0] : b;
}

const POST_RUN2 = {
  mtg_sets: 1049, mtg_oracle_cards: 38754, mtg_printings: 117860,
  mtg_printing_finishes: 173321, mtg_oracle_legalities: 891342,
  mtg_rulings: 78912, mtg_external_identifiers: 0, mtg_price_observations: 0,
};

console.log('=== POST-IDEMPOTENCY VERIFICATION ===\n');
console.log('## Catalogue counts — Run 2 (expected) vs Run 3 (actual)\n');
let diverged = false;
for (const [t, expected] of Object.entries(POST_RUN2)) {
  const actual = await count(t);
  const mark = actual === expected ? '✓' : '✗';
  if (actual !== expected) diverged = true;
  console.log(`  ${t.padEnd(28)} expected=${String(expected).padStart(8)}  actual=${String(actual).padStart(8)}  ${mark}`);
}
console.log(`\n  Overall: ${diverged ? 'DIVERGED' : 'IDENTICAL (stable across runs)'}\n`);

console.log('## Legacy archives — unchanged since Stage 1A\n');
for (const [t, exp] of [
  ['mtg_sets_legacy_20260327', 1029],
  ['mtg_cards_legacy_20260327', 104505],
  ['mtg_daily_prices_legacy_20260327', 98250],
  ['mtg_card_trends_legacy_20260327', 0],
]) {
  const c = await count(t);
  console.log(`  ${t.padEnd(36)} expected=${String(exp).padStart(7)}  actual=${String(c).padStart(7)}  ${c === exp ? '✓' : '✗'}`);
}

console.log('\n## Pokémon side — untouched\n');
for (const [t, exp] of [
  ['cards', 64813],
  ['daily_prices', 11109692],
  ['card_latest_prices', 63070],
  ['card_trends', 59267],
  ['provider_card_links', 41272],
]) {
  const c = await count(t);
  console.log(`  ${t.padEnd(22)} expected=${String(exp).padStart(11)}  actual=${String(c).padStart(11)}  ${c === exp ? '✓' : '✗'}`);
}

console.log('\n## Three market_import_runs records\n');
for (const id of [
  '95379098-b304-4aa4-bb69-a59137a114bc',   // Run 1 partial
  'd7ca4592-12d2-4c5a-ade9-da433b6f8da9',   // Run 2 success
  '102690f6-850b-499d-880c-2e7c9aeea337',   // Run 3 idempotency
]) {
  const r = await fetchOne('market_import_runs', `id=eq.${id}`);
  if (!r) { console.log(`  ${id}  MISSING`); continue; }
  const notes = typeof r.notes === 'string' ? JSON.parse(r.notes) : r.notes;
  console.log(`  ${r.id}  ${r.status.padEnd(8)}  errors=${notes?.errors ?? '?'}  ` +
              `dur=${r.duration_ms ?? '?'}ms  started=${r.started_at}`);
}

console.log('\n## Ruling / legality dedup stability across runs\n');
for (const id of [
  'd7ca4592-12d2-4c5a-ade9-da433b6f8da9',
  '102690f6-850b-499d-880c-2e7c9aeea337',
]) {
  const r = await fetchOne('market_import_runs', `id=eq.${id}`);
  const n = typeof r.notes === 'string' ? JSON.parse(r.notes) : r.notes;
  console.log(`  ${id}  rulings_duplicate=${n.rulings_duplicate}  legalities_duplicate=${n.legalities_duplicate}`);
}

// FK integrity after idempotency
console.log('\n## Post-idempotency FK integrity\n');
console.log(`  printings NULL oracle_card_id   = ${await count('mtg_printings','oracle_card_id=is.null')}`);
console.log(`  printings NULL set_id           = ${await count('mtg_printings','set_id=is.null')}`);
console.log(`  finishes NULL printing_id       = ${await count('mtg_printing_finishes','printing_id=is.null')}`);
console.log(`  legalities NULL oracle_card_id  = ${await count('mtg_oracle_legalities','oracle_card_id=is.null')}`);
console.log(`  rulings NULL oracle_card_id     = ${await count('mtg_rulings','oracle_card_id=is.null')}`);
