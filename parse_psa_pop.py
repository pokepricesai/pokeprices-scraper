"""
PSA Population Report Parser
=============================
Usage:
1. Go to PSA pop report page for a set
2. Select all (Ctrl+A), copy (Ctrl+C), paste into a .txt file
3. Run: python parse_psa_pop.py fossil.txt "Fossil" base_set.txt "Base Set" ...
4. Output: psa_pop_data.csv + psa_pop_insert.sql
"""

import re, csv, sys
from datetime import date

def parse_val(s):
    s = s.strip().replace(',', '')
    if s in ('\u2013', '-', '\u2014', '\u002d'):
        return 0
    if re.match(r'^\d+$', s):
        return int(s)
    return None

def parse_psa_pop(text, set_name):
    lines = text.strip().split('\n')
    cards = []
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip TOTAL POPULATION block entirely
        if line.startswith('TOTAL POPULATION'):
            i += 1
            while i < len(lines):
                if re.match(r'^\d+\t', lines[i].strip()):
                    break
                i += 1
            continue
        
        if not line or line.startswith('Card No.'):
            i += 1
            continue
        
        match = re.match(r'^(\d+)\t(.+?)(?:Shop with Affiliates)?$', line)
        if not match:
            i += 1
            continue
        
        card_number = match.group(1)
        raw_name = match.group(2).strip()
        i += 1
        variant = ""
        
        while i < len(lines):
            peek = lines[i].strip()
            if peek in ('Grade', '+', 'Q') or re.match(r'^\d+\t', peek):
                break
            cleaned = peek.replace('Shop with Affiliates', '').strip()
            if cleaned:
                variant = (variant + ' ' + cleaned).strip() if variant else cleaned
            i += 1
        
        # Skip Grade, +, Q labels
        for label in ['Grade', '+', 'Q']:
            if i < len(lines) and lines[i].strip() == label:
                i += 1
        
        # Read 39 interleaved values (13 columns x 3 rows)
        vals = []
        while len(vals) < 39 and i < len(lines):
            v = parse_val(lines[i].strip())
            if v is not None:
                vals.append(v)
                i += 1
            else:
                break
        
        if len(vals) >= 39:
            g = [vals[j * 3] for j in range(13)]  # Grade row
            total = g[12]
            psa_10 = g[11]
            
            cards.append({
                'set_name': set_name, 'card_number': card_number,
                'card_name': raw_name, 'variant': variant,
                'full_name': f"{raw_name} ({variant})" if variant else raw_name,
                'auth': g[0], 'psa_1': g[1], 'psa_1_5': g[2], 'psa_2': g[3],
                'psa_3': g[4], 'psa_4': g[5], 'psa_5': g[6], 'psa_6': g[7],
                'psa_7': g[8], 'psa_8': g[9], 'psa_9': g[10], 'psa_10': psa_10,
                'total': total,
                'gem_rate': round(psa_10 / total * 100, 2) if total > 0 else 0,
            })
    
    return cards

def generate_sql(all_cards):
    today = date.today().isoformat()
    sql = f"""-- PSA Population Data Import — Generated {today}

CREATE TABLE IF NOT EXISTS psa_population (
    id SERIAL PRIMARY KEY,
    set_name TEXT NOT NULL,
    card_number TEXT NOT NULL,
    card_name TEXT NOT NULL,
    variant TEXT DEFAULT '',
    full_name TEXT NOT NULL,
    auth INTEGER DEFAULT 0,
    psa_1 INTEGER DEFAULT 0, psa_1_5 INTEGER DEFAULT 0,
    psa_2 INTEGER DEFAULT 0, psa_3 INTEGER DEFAULT 0,
    psa_4 INTEGER DEFAULT 0, psa_5 INTEGER DEFAULT 0,
    psa_6 INTEGER DEFAULT 0, psa_7 INTEGER DEFAULT 0,
    psa_8 INTEGER DEFAULT 0, psa_9 INTEGER DEFAULT 0,
    psa_10 INTEGER DEFAULT 0,
    total_graded INTEGER DEFAULT 0,
    gem_rate NUMERIC(5,2) DEFAULT 0,
    scraped_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_psa_pop_set_card ON psa_population(set_name, card_number);
CREATE INDEX IF NOT EXISTS idx_psa_pop_name ON psa_population(card_name);
CREATE INDEX IF NOT EXISTS idx_psa_pop_full_name ON psa_population(full_name);

"""
    sets_seen = set()
    for c in all_cards:
        if c['set_name'] not in sets_seen:
            sql += f"DELETE FROM psa_population WHERE set_name = '{c['set_name']}';\n"
            sets_seen.add(c['set_name'])
    
    for start in range(0, len(all_cards), 100):
        chunk = all_cards[start:start+100]
        sql += "\nINSERT INTO psa_population (set_name, card_number, card_name, variant, full_name, auth, psa_1, psa_1_5, psa_2, psa_3, psa_4, psa_5, psa_6, psa_7, psa_8, psa_9, psa_10, total_graded, gem_rate, scraped_date) VALUES\n"
        vals = []
        for c in chunk:
            ne = c['card_name'].replace("'", "''")
            ve = c['variant'].replace("'", "''")
            fe = c['full_name'].replace("'", "''")
            vals.append(f"('{c['set_name']}','{c['card_number']}','{ne}','{ve}','{fe}',{c['auth']},{c['psa_1']},{c['psa_1_5']},{c['psa_2']},{c['psa_3']},{c['psa_4']},{c['psa_5']},{c['psa_6']},{c['psa_7']},{c['psa_8']},{c['psa_9']},{c['psa_10']},{c['total']},{c['gem_rate']},'{today}')")
        sql += ',\n'.join(vals) + ';\n'
    return sql

def main():
    if len(sys.argv) < 3 or len(sys.argv) % 2 == 0:
        print("Usage: python parse_psa_pop.py <file.txt> 'Set Name' [file2.txt 'Set Name 2' ...]")
        sys.exit(1)
    
    all_cards = []
    for idx in range(1, len(sys.argv), 2):
        with open(sys.argv[idx], 'r', encoding='utf-8') as f:
            cards = parse_psa_pop(f.read(), sys.argv[idx+1])
        all_cards.extend(cards)
        total = sum(c['total'] for c in cards)
        print(f"\n{'='*60}")
        print(f"SET: {sys.argv[idx+1]} — {len(cards)} entries, {total:,} total graded")
        for c in sorted(cards, key=lambda x: x['total'], reverse=True)[:5]:
            print(f"  #{c['card_number']} {c['full_name']}: {c['total']:,} graded, PSA10={c['psa_10']:,} ({c['gem_rate']}%)")
    
    fields = ['set_name','card_number','card_name','variant','full_name','auth',
              'psa_1','psa_1_5','psa_2','psa_3','psa_4','psa_5','psa_6','psa_7',
              'psa_8','psa_9','psa_10','total','gem_rate']
    with open('psa_pop_data.csv', 'w', newline='') as f:
        csv.DictWriter(f, fieldnames=fields).writeheader()
        csv.DictWriter(f, fieldnames=fields).writerows(all_cards)
    
    with open('psa_pop_insert.sql', 'w') as f:
        f.write(generate_sql(all_cards))
    
    print(f"\n{'='*60}")
    print(f"TOTAL: {len(all_cards)} entries — psa_pop_data.csv + psa_pop_insert.sql")

if __name__ == '__main__':
    main()
