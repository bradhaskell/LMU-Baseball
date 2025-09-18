CREATE TABLE IF NOT EXISTS raw_cauldron_scoreboard (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    name TEXT,
    year INT,
    season TEXT,
    team TEXT,
    captain TEXT,
    week INT,
    "Strike +50%" TEXT,
    "OS InZone +50%" TEXT,
    "IZT Strike +25%" TEXT,
    "S&C PR" TEXT,
    "ATC Room" TEXT,
    "Strike < 50%" TEXT,
    "OS" TEXT,
    -- ... add more columns as they appear in the sheet; leave as TEXT in raw
    _row_hash TEXT            -- for idempotency (optional)
);
