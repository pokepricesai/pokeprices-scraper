// scripts/stage1d_patch_stale_mtgjson_meta.mjs
//
// One-shot repair: the two Stage 1D scraper_nightly runs written
// during Gate 5 and Gate 6 recorded notes.mtgjson_meta.data.date =
// '2026-09-13' because the pipeline was passing the stale bootstrap
// reconciliation.mtgjson_meta rather than the fresh AllPricesToday
// meta. The observed_on values that landed are '2026-09-14', so we
// patch the notes to match the reality of the data ingested.
//
// The pipeline code has been fixed so future runs will not need this.

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
const H = { apikey: KEY, Authorization: `Bearer ${KEY}`, 'Content-Type': 'application/json' };

const CORRECT_META = {
  data: { date: '2026-09-14', version: '5.3.0+20260914' },
  meta: { date: '2026-09-14', version: '5.3.0+20260914' },
};

const rows = await (await fetch(
  `${URL}/rest/v1/market_import_runs?provider=eq.mtgjson&source=eq.scraper_nightly&order=started_at.desc&limit=5&select=id,notes`,
  { headers: { ...H, Accept: 'application/json' } },
)).json();

console.log(`Found ${rows.length} scraper_nightly mtgjson row(s):`);
for (const r of rows) {
  const notes = JSON.parse(r.notes);
  const oldDate = notes.mtgjson_meta?.data?.date;
  console.log(`  ${r.id}: notes.mtgjson_meta.data.date=${oldDate}`);
  if (oldDate === '2026-09-13') {
    notes.mtgjson_meta = CORRECT_META;
    const patch = await fetch(
      `${URL}/rest/v1/market_import_runs?id=eq.${r.id}`,
      { method: 'PATCH',
        headers: { ...H, Prefer: 'return=minimal' },
        body: JSON.stringify({ notes: JSON.stringify(notes) }) },
    );
    console.log(`    → patched (HTTP ${patch.status})`);
  } else {
    console.log(`    → skip (not stale)`);
  }
}
