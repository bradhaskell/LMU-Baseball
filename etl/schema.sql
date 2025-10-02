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
  "OS" TEXT
  -- add other columns you need; keep TEXT in raw
);
