import type { BlockPayload, Env, SubnetRecord, SubnetSummaryValue, SummaryPayload } from './types';

export const HOUR_MS = 3_600_000;
export const DAY_MS = 86_400_000;
const SHARD_WINDOW_MS = 8 * DAY_MS;

export function blockDatabases(env: Env): D1Database[] {
  return [env.BLOCKS_0, env.BLOCKS_1, env.BLOCKS_2, env.BLOCKS_3];
}

export function blockDbForTimestamp(env: Env, timestampMs: number): D1Database {
  const window = Math.floor(timestampMs / SHARD_WINDOW_MS);
  const slot = ((window % 4) + 4) % 4;
  return blockDatabases(env)[slot];
}

export async function getState(db: D1Database, key: string): Promise<string | null> {
  const row = await db.prepare('SELECT value FROM sync_state WHERE key = ?').bind(key).first<{ value: string }>();
  return row?.value ?? null;
}

export async function setState(db: D1Database, key: string, value: string): Promise<void> {
  await db.prepare(`
    INSERT INTO sync_state(key,value,updated_at_ms) VALUES(?,?,?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at_ms=excluded.updated_at_ms
  `).bind(key, value, Date.now()).run();
}

export async function listKnownNetuids(db: D1Database): Promise<number[]> {
  const result = await db.prepare('SELECT netuid FROM subnets ORDER BY netuid').all<{ netuid: number }>();
  return result.results.map(r => Number(r.netuid));
}

export async function upsertSubnets(db: D1Database, rows: SubnetRecord[]): Promise<void> {
  if (!rows.length) return;
  const statements = rows.map(row => db.prepare(`
    INSERT INTO subnets(netuid,registration_counter,name,status,first_seen_block,last_seen_block,updated_at_ms)
    VALUES(?,?,?,?,?,?,?)
    ON CONFLICT(netuid) DO UPDATE SET
      registration_counter=excluded.registration_counter,
      name=excluded.name,
      status=excluded.status,
      last_seen_block=excluded.last_seen_block,
      updated_at_ms=excluded.updated_at_ms
  `).bind(row.netuid, row.registration_counter, row.name, row.status, row.first_seen_block, row.last_seen_block, row.updated_at_ms));
  for (let i = 0; i < statements.length; i += 64) await db.batch(statements.slice(i, i + 64));
}

export async function storeBlock(env: Env, blockNumber: number, blockHash: string, timestampMs: number, payload: BlockPayload): Promise<boolean> {
  const db = blockDbForTimestamp(env, timestampMs);
  const result = await db.prepare(`
    INSERT OR IGNORE INTO blocks(block_number,block_hash,timestamp_ms,payload,created_at_ms)
    VALUES(?,?,?,?,?)
  `).bind(blockNumber, blockHash, timestampMs, JSON.stringify(payload), Date.now()).run();
  const changes = Number((result.meta as {changes?: number} | undefined)?.changes ?? 0);
  return changes > 0;
}

/** Replace only the compact payload after a best-effort theory calculation. */
export async function updateBlockPayload(env: Env, blockNumber: number, timestampMs: number, payload: BlockPayload): Promise<void> {
  const db = blockDbForTimestamp(env, timestampMs);
  await db.prepare('UPDATE blocks SET payload = ? WHERE block_number = ?')
    .bind(JSON.stringify(payload), blockNumber).run();
}

function emptySummary(value: [string,string,string|null]): SubnetSummaryValue {
  return ['0','0',value[2] == null ? null : '0',0];
}

export function addBlockToSummary(summary: SummaryPayload, block: BlockPayload): void {
  for (const [netuid, value] of Object.entries(block)) {
    const old = summary[netuid] ?? emptySummary(value);
    old[0] = (BigInt(old[0]) + BigInt(value[0])).toString();
    old[1] = (BigInt(old[1]) + BigInt(value[1])).toString();
    old[2] = old[2] == null || value[2] == null ? null : (BigInt(old[2]) + BigInt(value[2])).toString();
    old[3] += 1;
    summary[netuid] = old;
  }
}

export function mergeSummaries(target: SummaryPayload, source: SummaryPayload): void {
  for (const [netuid, value] of Object.entries(source)) {
    const old = target[netuid] ?? ['0','0',value[2] == null ? null : '0',0];
    old[0] = (BigInt(old[0]) + BigInt(value[0])).toString();
    old[1] = (BigInt(old[1]) + BigInt(value[1])).toString();
    old[2] = old[2] == null || value[2] == null ? null : (BigInt(old[2]) + BigInt(value[2])).toString();
    old[3] += value[3];
    target[netuid] = old;
  }
}

export async function rebuildHourlySummary(env: Env, periodStartMs: number): Promise<void> {
  const periodEndMs = periodStartMs + HOUR_MS;
  const summary: SummaryPayload = {};
  let blocks = 0;
  for (const db of blockDatabases(env)) {
    const result = await db.prepare('SELECT payload FROM blocks WHERE timestamp_ms >= ? AND timestamp_ms < ? ORDER BY timestamp_ms')
      .bind(periodStartMs, periodEndMs).all<{payload:string}>();
    blocks += result.results.length;
    for (const row of result.results) addBlockToSummary(summary, JSON.parse(row.payload) as BlockPayload);
  }
  await env.META_DB.prepare(`
    INSERT INTO hourly_summary(period_start_ms,payload,block_count,updated_at_ms) VALUES(?,?,?,?)
    ON CONFLICT(period_start_ms) DO UPDATE SET payload=excluded.payload, block_count=excluded.block_count, updated_at_ms=excluded.updated_at_ms
  `).bind(periodStartMs, JSON.stringify(summary), blocks, Date.now()).run();
}

/** Build every closed hour exactly once; retries are idempotent because rows are replaced. */
export async function ensureClosedHourlySummaries(env: Env, latestTimestampMs: number): Promise<void> {
  const currentHour = Math.floor(latestTimestampMs / HOUR_MS) * HOUR_MS;
  const state = await getState(env.META_DB, 'next_hourly_summary_ms');
  if (state == null) {
    await setState(env.META_DB, 'next_hourly_summary_ms', String(currentHour));
    return;
  }
  let next = Number(state);
  while (Number.isFinite(next) && next < currentHour) {
    await rebuildHourlySummary(env, next);
    next += HOUR_MS;
    await setState(env.META_DB, 'next_hourly_summary_ms', String(next));
  }
}

export async function cleanupOldData(env: Env, retentionDays: number): Promise<void> {
  const cutoff = Date.now() - retentionDays * DAY_MS;
  for (const db of blockDatabases(env)) await db.prepare('DELETE FROM blocks WHERE timestamp_ms < ?').bind(cutoff).run();
  await env.META_DB.prepare('DELETE FROM hourly_summary WHERE period_start_ms < ?').bind(Math.floor(cutoff/HOUR_MS)*HOUR_MS).run();
  await setState(env.META_DB, 'last_cleanup_ms', String(Date.now()));
}
