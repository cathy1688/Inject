import type { Env } from './types';

const DATA_VERSION = '4';

const META_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS subnets (
    netuid INTEGER PRIMARY KEY,
    registration_counter TEXT,
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active','deregistered')),
    first_seen_block INTEGER NOT NULL,
    last_seen_block INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_subnets_status ON subnets(status)`,
  `CREATE TABLE IF NOT EXISTS hourly_summary (
    period_start_ms INTEGER PRIMARY KEY,
    payload TEXT NOT NULL,
    block_count INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
  )`
];

const BLOCK_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS blocks (
    block_number INTEGER PRIMARY KEY,
    block_hash TEXT NOT NULL UNIQUE,
    timestamp_ms INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp_ms)`
];

let ready: Promise<void> | null = null;

async function run(db: D1Database, statements: string[]): Promise<void> {
  await db.batch(statements.map(sql => db.prepare(sql)));
}

async function ensureDataVersion(env: Env): Promise<void> {
  const row = await env.META_DB.prepare("SELECT value FROM sync_state WHERE key='data_version'").first<{value:string}>();
  if (row?.value === DATA_VERSION) return;

  // v4 is the formula-only theory contract: real pool injection stays an observed
  // chain value, while theoretical injection is independently calculated from
  // RootProp × AlphaEmission × AlphaPrice. The project has only just launched,
  // so restart the bootstrap dataset once rather than mix incompatible rows.
  await Promise.all([
    env.BLOCKS_0.prepare('DELETE FROM blocks').run(),
    env.BLOCKS_1.prepare('DELETE FROM blocks').run(),
    env.BLOCKS_2.prepare('DELETE FROM blocks').run(),
    env.BLOCKS_3.prepare('DELETE FROM blocks').run()
  ]);
  await env.META_DB.batch([
    env.META_DB.prepare('DELETE FROM subnets'),
    env.META_DB.prepare('DELETE FROM hourly_summary'),
    env.META_DB.prepare('DELETE FROM sync_state')
  ]);
  await env.META_DB.prepare('INSERT INTO sync_state(key,value,updated_at_ms) VALUES(?,?,?)')
    .bind('data_version', DATA_VERSION, Date.now()).run();
}

export function ensureSchema(env: Env): Promise<void> {
  if (!ready) {
    ready = (async () => {
      await run(env.META_DB, META_SCHEMA);
      await Promise.all([
        run(env.BLOCKS_0, BLOCK_SCHEMA),
        run(env.BLOCKS_1, BLOCK_SCHEMA),
        run(env.BLOCKS_2, BLOCK_SCHEMA),
        run(env.BLOCKS_3, BLOCK_SCHEMA)
      ]);
      await ensureDataVersion(env);
    })().catch(error => {
      ready = null;
      throw error;
    });
  }
  return ready;
}
