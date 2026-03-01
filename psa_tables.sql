-- ============================================================
-- PSA Population Tables for PokePrices
-- Run this in Supabase SQL Editor
-- ============================================================

-- Drop existing table if you want a clean start
-- (Comment this out if you already have data you want to keep)
-- DROP TABLE IF EXISTS psa_population CASCADE;
-- DROP TABLE IF EXISTS psa_pop_history CASCADE;

-- Main population table (latest data, upserted weekly)
CREATE TABLE IF NOT EXISTS psa_population (
    id SERIAL PRIMARY KEY,
    set_name TEXT NOT NULL,
    release_year TEXT DEFAULT '',
    card_number TEXT NOT NULL,
    card_name TEXT NOT NULL,
    variant TEXT DEFAULT '',
    full_name TEXT NOT NULL,
    psa_spec_id TEXT DEFAULT '',
    auth INTEGER DEFAULT 0,
    psa_1 INTEGER DEFAULT 0,
    psa_1_5 INTEGER DEFAULT 0,
    psa_2 INTEGER DEFAULT 0,
    psa_3 INTEGER DEFAULT 0,
    psa_4 INTEGER DEFAULT 0,
    psa_5 INTEGER DEFAULT 0,
    psa_6 INTEGER DEFAULT 0,
    psa_7 INTEGER DEFAULT 0,
    psa_8 INTEGER DEFAULT 0,
    psa_9 INTEGER DEFAULT 0,
    psa_10 INTEGER DEFAULT 0,
    total_graded INTEGER DEFAULT 0,
    gem_rate NUMERIC(5,2) DEFAULT 0,
    scraped_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Unique constraint for upsert (set + number + variant = unique card)
-- Drop old constraint if it exists with different columns
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'psa_population_unique_card'
    ) THEN
        ALTER TABLE psa_population 
        ADD CONSTRAINT psa_population_unique_card 
        UNIQUE (set_name, card_number, card_name, variant);
    END IF;
END $$;

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_psa_pop_set ON psa_population(set_name);
CREATE INDEX IF NOT EXISTS idx_psa_pop_card_name ON psa_population(card_name);
CREATE INDEX IF NOT EXISTS idx_psa_pop_full_name ON psa_population(full_name);
CREATE INDEX IF NOT EXISTS idx_psa_pop_spec_id ON psa_population(psa_spec_id);
CREATE INDEX IF NOT EXISTS idx_psa_pop_year ON psa_population(release_year);

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_psa_pop_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS psa_pop_updated ON psa_population;
CREATE TRIGGER psa_pop_updated
    BEFORE UPDATE ON psa_population
    FOR EACH ROW
    EXECUTE FUNCTION update_psa_pop_timestamp();

-- ============================================================
-- History table (weekly snapshots for trend tracking)
-- ============================================================

CREATE TABLE IF NOT EXISTS psa_pop_history (
    id SERIAL PRIMARY KEY,
    set_name TEXT NOT NULL,
    card_number TEXT NOT NULL,
    card_name TEXT NOT NULL,
    variant TEXT DEFAULT '',
    psa_spec_id TEXT DEFAULT '',
    psa_8 INTEGER DEFAULT 0,
    psa_9 INTEGER DEFAULT 0,
    psa_10 INTEGER DEFAULT 0,
    total_graded INTEGER DEFAULT 0,
    gem_rate NUMERIC(5,2) DEFAULT 0,
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Unique constraint: one snapshot per card per date
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'psa_pop_history_unique_snapshot'
    ) THEN
        ALTER TABLE psa_pop_history 
        ADD CONSTRAINT psa_pop_history_unique_snapshot 
        UNIQUE (set_name, card_number, card_name, variant, snapshot_date);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_psa_history_date ON psa_pop_history(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_psa_history_card ON psa_pop_history(set_name, card_name);

-- ============================================================
-- Verify setup
-- ============================================================

SELECT 'psa_population' as table_name, COUNT(*) as rows FROM psa_population
UNION ALL
SELECT 'psa_pop_history', COUNT(*) FROM psa_pop_history;
