PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS subnets (
  netuid INTEGER PRIMARY KEY,
  registration_counter TEXT,
  name TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('active','deregistered')),
  first_seen_block INTEGER NOT NULL,
  last_seen_block INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_subnets_status ON subnets(status);

CREATE TABLE IF NOT EXISTS hourly_summary (
  period_start_ms INTEGER PRIMARY KEY,
  payload TEXT NOT NULL,
  block_count INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
