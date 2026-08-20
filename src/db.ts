import type { BlockPayload, Env, SubnetRecord, SubnetSummaryValue, SummaryPayload } from './types';

export const MINUTE_MS = 60_000;
export const HOUR_MS = 3_600_000;
export const DAY_MS = 86_400_000;
const SHARD_WINDOW_MS = 8 * DAY_MS; // 4 shards = 32-day rotation; retention is 30 days.

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
  const now = Date.now();
  await db.prepare(`
    INSERT INTO sync_state(key,value,updated_at_ms) VALUES(?,?,?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at_ms=excluded.updated_at_ms
  `).bind(key, value, now).run();
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
  const exists = await db.prepare('SELECT 1 AS ok FROM blocks WHERE block_number = ?').bind(blockNumber).first<{ ok: number }>();
  if (exists) return false;
  await db.prepare('INSERT INTO blocks(block_number,block_hash,timestamp_ms,payload,created_at_ms) VALUES(?,?,?,?,?)')
    .bind(blockNumber, blockHash, timestampMs, JSON.stringify(payload), Date.now()).run();
  return true;
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

async function mergeSummaryRow(db: D1Database, table: 'minute_summary'|'hourly_summary'|'daily_summary', startMs: number, addition: SummaryPayload, blockCount: number): Promise<void> {
  const row = await db.prepare(`SELECT payload FROM ${table} WHERE period_start_ms=?`).bind(startMs).first<{payload:string}>();
  const merged: SummaryPayload = row?.payload ? JSON.parse(row.payload) as SummaryPayload : {};
  mergeSummaries(merged, addition);
  await db.prepare(`
    INSERT INTO ${table}(period_start_ms,payload,block_count,updated_at_ms) VALUES(?,?,?,?)
    ON CONFLICT(period_start_ms) DO UPDATE SET payload=excluded.payload, block_count=${table}.block_count+excluded.block_count, updated_at_ms=excluded.updated_at_ms
  `).bind(startMs, JSON.stringify(merged), blockCount, Date.now()).run();
}

export async function updateSummariesForNewBlocks(metaDb: D1Database, newBlocks: Array<{timestampMs:number; payload:BlockPayload}>): Promise<void> {
  if (!newBlocks.length) return;
  const minute = new Map<number,{payload:SummaryPayload; blocks:number}>();
  const hour = new Map<number,{payload:SummaryPayload; blocks:number}>();
  const day = new Map<number,{payload:SummaryPayload; blocks:number}>();

  const add = (map: Map<number,{payload:SummaryPayload;blocks:number}>, period: number, block: BlockPayload) => {
    const entry = map.get(period) ?? {payload:{},blocks:0};
    addBlockToSummary(entry.payload, block);
    entry.blocks++;
    map.set(period,entry);
  };

  for (const block of newBlocks) {
    add(minute, Math.floor(block.timestampMs/MINUTE_MS)*MINUTE_MS, block.payload);
    add(hour, Math.floor(block.timestampMs/HOUR_MS)*HOUR_MS, block.payload);
    add(day, Math.floor(block.timestampMs/DAY_MS)*DAY_MS, block.payload);
  }
  for (const [p,v] of minute) await mergeSummaryRow(metaDb,'minute_summary',p,v.payload,v.blocks);
  for (const [p,v] of hour) await mergeSummaryRow(metaDb,'hourly_summary',p,v.payload,v.blocks);
  for (const [p,v] of day) await mergeSummaryRow(metaDb,'daily_summary',p,v.payload,v.blocks);
}

export async function cleanupOldData(env: Env, retentionDays: number): Promise<void> {
  const cutoff = Date.now() - retentionDays * DAY_MS;
  for (const db of blockDatabases(env)) await db.prepare('DELETE FROM blocks WHERE timestamp_ms < ?').bind(cutoff).run();
  const dayCutoff = Math.floor(cutoff/DAY_MS)*DAY_MS;
  await env.META_DB.batch([
    env.META_DB.prepare('DELETE FROM minute_summary WHERE period_start_ms < ?').bind(cutoff),
    env.META_DB.prepare('DELETE FROM hourly_summary WHERE period_start_ms < ?').bind(cutoff),
    env.META_DB.prepare('DELETE FROM daily_summary WHERE period_start_ms < ?').bind(dayCutoff)
  ]);
  await setState(env.META_DB, 'last_cleanup_ms', String(Date.now()));
}
