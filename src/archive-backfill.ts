import type { BlockPayload, Env } from './types';
import { SubtensorRpc } from './rpc';
import {
  decodeBool,
  decodeI96F32Raw,
  decodeSubnetPrices,
  decodeU16,
  decodeU64,
  decodeU64F64Raw,
  decodeU96F32Raw,
  globalStorageKeys,
  storageKeys
} from './storage';
import {
  calculateIndependentTheory,
  DEFAULT_GATE_EXPONENT_RAW,
  DEFAULT_GATE_QUANTILE_RAW,
  DEFAULT_GATE_RANK,
  THEORY_MODEL_VERSION,
  type TheorySnapshot,
  type TheorySubnetState
} from './theory';
import {
  getEarliestStoredBlockNumber,
  getState,
  getStoredBlocksAfter,
  HOUR_MS,
  rebuildHourlySummary,
  setState,
  updateBlockPayload
} from './db';

const ROOT_NETUID = 0;
const DEFAULT_BACKFILL_BATCH = 4;
const ARCHIVE_ENDPOINTS = [
  'wss://archive.chain.opentensor.ai:443',
  'wss://archive.sub.latent.to:443'
] as const;

function rpcBigInt(value: unknown): bigint | null {
  if (typeof value === 'bigint') return value >= 0n ? value : null;
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return BigInt(value);
  if (typeof value === 'string') {
    if (/^\d+$/.test(value)) return BigInt(value);
    if (/^0x[0-9a-f]+$/i.test(value)) return BigInt(value);
  }
  return null;
}

async function readAlphaPrices(rpc: SubtensorRpc, atHash: string): Promise<Map<number,bigint>> {
  try {
    const records = await rpc.currentAlphaPricesAll(atHash);
    const prices = new Map<number,bigint>();
    for (const record of records ?? []) {
      const netuid = Number(record?.netuid);
      const price = rpcBigInt(record?.price);
      if (Number.isInteger(netuid) && netuid > 0 && price != null && price > 0n) prices.set(netuid, price);
    }
    if (prices.size > 0) return prices;
  } catch {}
  try { return decodeSubnetPrices(await rpc.currentAlphaPricesAllScale(atHash)); }
  catch { return new Map(); }
}

function formulaStorageKeys(candidates: number[]): string[] {
  const keys = new Set<string>([
    globalStorageKeys.totalIssuance,
    globalStorageKeys.taoWeight,
    globalStorageKeys.emissionGateBar,
    globalStorageKeys.emissionGateExponent,
    globalStorageKeys.emissionBarQuantile,
    globalStorageKeys.emissionBarRank,
    storageKeys.subnetTao(ROOT_NETUID)
  ]);
  for (const netuid of candidates) {
    keys.add(storageKeys.networksAdded(netuid));
    keys.add(storageKeys.firstEmissionBlockNumber(netuid));
    keys.add(storageKeys.subtokenEnabled(netuid));
    keys.add(storageKeys.networkRegistrationAllowed(netuid));
    keys.add(storageKeys.subnetEmissionEnabled(netuid));
    keys.add(storageKeys.subnetMechanism(netuid));
    keys.add(storageKeys.subnetMovingPrice(netuid));
    keys.add(storageKeys.minerBurned(netuid));
    keys.add(storageKeys.subnetAlphaIn(netuid));
    keys.add(storageKeys.subnetAlphaOut(netuid));
  }
  return [...keys];
}

function boolWithDefault(raw: string | null | undefined, fallback: boolean): boolean {
  return raw == null ? fallback : decodeBool(raw);
}

function decodeFormulaSnapshot(
  state: Map<string, string | null>,
  candidates: number[],
  prices: Map<number,bigint>
): TheorySnapshot {
  const totalIssuanceHex = state.get(globalStorageKeys.totalIssuance) ?? null;
  const taoWeightHex = state.get(globalStorageKeys.taoWeight) ?? null;
  if (totalIssuanceHex == null) throw new Error('Missing Subtensor.TotalIssuance for archive theory');
  if (taoWeightHex == null) throw new Error('Missing Subtensor.TaoWeight for archive theory');

  const subnets: TheorySubnetState[] = candidates.map(netuid => ({
    netuid,
    networkAdded: boolWithDefault(state.get(storageKeys.networksAdded(netuid)), false),
    firstEmissionStarted: state.get(storageKeys.firstEmissionBlockNumber(netuid)) != null,
    subtokenEnabled: boolWithDefault(state.get(storageKeys.subtokenEnabled(netuid)), false),
    registrationAllowed: boolWithDefault(state.get(storageKeys.networkRegistrationAllowed(netuid)), true),
    emissionEnabled: boolWithDefault(state.get(storageKeys.subnetEmissionEnabled(netuid)), true),
    mechanism: decodeU16(state.get(storageKeys.subnetMechanism(netuid)) ?? null),
    movingPriceRaw: decodeI96F32Raw(state.get(storageKeys.subnetMovingPrice(netuid)) ?? null),
    minerBurnedRaw: decodeU96F32Raw(state.get(storageKeys.minerBurned(netuid)) ?? null),
    alphaInRao: decodeU64(state.get(storageKeys.subnetAlphaIn(netuid)) ?? null),
    alphaOutRao: decodeU64(state.get(storageKeys.subnetAlphaOut(netuid)) ?? null),
    priceRaoPerAlpha: prices.get(netuid) ?? null
  }));

  const gateExponentHex = state.get(globalStorageKeys.emissionGateExponent) ?? null;
  const gateQuantileHex = state.get(globalStorageKeys.emissionBarQuantile) ?? null;
  const gateRankHex = state.get(globalStorageKeys.emissionBarRank) ?? null;
  return {
    totalIssuanceRao: decodeU64(totalIssuanceHex),
    rootTaoRao: decodeU64(state.get(storageKeys.subnetTao(ROOT_NETUID)) ?? null),
    taoWeightRaw: decodeU64(taoWeightHex),
    emissionGateBarRaw: decodeU64F64Raw(state.get(globalStorageKeys.emissionGateBar) ?? null),
    emissionGateExponentRaw: gateExponentHex == null ? DEFAULT_GATE_EXPONENT_RAW : decodeU64F64Raw(gateExponentHex),
    emissionBarQuantileRaw: gateQuantileHex == null ? DEFAULT_GATE_QUANTILE_RAW : decodeU64F64Raw(gateQuantileHex),
    emissionBarRank: gateRankHex == null ? DEFAULT_GATE_RANK : decodeU16(gateRankHex),
    subnets
  };
}

async function readFormulaSnapshot(rpc: SubtensorRpc, hash: string, candidates: number[]): Promise<TheorySnapshot> {
  const [state, prices] = await Promise.all([
    rpc.queryStorage(formulaStorageKeys(candidates), hash),
    readAlphaPrices(rpc, hash)
  ]);
  return decodeFormulaSnapshot(state, candidates, prices);
}

function applyTheory(payload: BlockPayload, candidates: number[], snapshot: TheorySnapshot, blockNumber: number): number {
  const results = calculateIndependentTheory(snapshot, blockNumber);
  let enriched = 0;
  for (const netuid of candidates) {
    const value = payload[String(netuid)];
    if (!value) continue;
    const theory = results.get(netuid);
    if (!theory || theory.theoreticalInjectedRao == null) continue;
    value[2] = theory.theoreticalInjectedRao.toString();
    enriched++;
  }
  return enriched;
}

async function backfillBatch(env: Env, rpc: SubtensorRpc, batchSize: number): Promise<void> {
  const formulaVersion = await getState(env.META_DB, 'theory_formula_version');
  let cursorState = await getState(env.META_DB, 'theory_backfill_cursor');

  if (formulaVersion !== THEORY_MODEL_VERSION) {
    const earliest = await getEarliestStoredBlockNumber(env);
    await setState(env.META_DB, 'theory_formula_version', THEORY_MODEL_VERSION);
    cursorState = String((earliest ?? 1) - 1);
    await setState(env.META_DB, 'theory_backfill_cursor', cursorState);
  } else if (cursorState == null) {
    const earliest = await getEarliestStoredBlockNumber(env);
    cursorState = String((earliest ?? 1) - 1);
    await setState(env.META_DB, 'theory_backfill_cursor', cursorState);
  }

  let cursor = Number(cursorState);
  const rows = await getStoredBlocksAfter(env, cursor, batchSize);
  if (!rows.length) {
    await setState(env.META_DB, 'theory_backfill_status', 'complete');
    await setState(env.META_DB, 'theory_backfill_last_processed', '0');
    return;
  }

  await setState(env.META_DB, 'theory_backfill_status', 'running');
  let processed = 0;
  const touchedHours = new Set<number>();

  for (const row of rows) {
    const payload = JSON.parse(row.payload) as BlockPayload;
    const candidates = Object.keys(payload).map(Number).filter(n => Number.isInteger(n) && n > 0);
    if (!candidates.length) {
      cursor = row.block_number;
      processed++;
      await setState(env.META_DB, 'theory_backfill_cursor', String(cursor));
      continue;
    }

    const header = await rpc.header(row.block_hash);
    const snapshot = await readFormulaSnapshot(rpc, header.parentHash, candidates);
    const enriched = applyTheory(payload, candidates, snapshot, row.block_number);
    if (enriched !== candidates.length) {
      await setState(env.META_DB, 'theory_backfill_status', 'partial');
      throw new Error(`Archive theory incomplete at block ${row.block_number}: ${enriched}/${candidates.length}`);
    }

    await updateBlockPayload(env, row.block_number, row.timestamp_ms, payload);
    touchedHours.add(Math.floor(row.timestamp_ms / HOUR_MS) * HOUR_MS);
    cursor = row.block_number;
    processed++;
    await setState(env.META_DB, 'theory_backfill_cursor', String(cursor));
  }

  for (const hour of touchedHours) await rebuildHourlySummary(env, hour);
  await setState(env.META_DB, 'theory_backfill_last_processed', String(processed));
  const more = await getStoredBlocksAfter(env, cursor, 1);
  await setState(env.META_DB, 'theory_backfill_status', more.length ? 'running' : 'complete');
}

/** Historical reconstruction only. Live collection must never use an archive node. */
export async function runArchiveTheoryBackfill(env: Env, batchSize = DEFAULT_BACKFILL_BATCH): Promise<void> {
  const now = Date.now();
  const lockUntil = Number(await getState(env.META_DB, 'theory_backfill_lock_until_ms') ?? '0');
  if (lockUntil > now) return;
  await setState(env.META_DB, 'theory_backfill_lock_until_ms', String(now + 45_000));

  const errors: string[] = [];
  try {
    for (const endpoint of ARCHIVE_ENDPOINTS) {
      const rpc = new SubtensorRpc(endpoint);
      try {
        await rpc.connect();
        await backfillBatch(env, rpc, Math.max(1, Math.min(12, Math.trunc(batchSize))));
        await setState(env.META_DB, 'theory_backfill_rpc', endpoint);
        await env.META_DB.prepare("DELETE FROM sync_state WHERE key='theory_backfill_error'").run();
        return;
      } catch (error) {
        errors.push(`${endpoint}: ${error instanceof Error ? error.message : String(error)}`);
      } finally {
        rpc.close();
      }
    }

    await setState(env.META_DB, 'theory_backfill_status', 'error');
    await setState(env.META_DB, 'theory_backfill_error', errors.join(' | ').slice(0, 1800));
  } finally {
    await setState(env.META_DB, 'theory_backfill_lock_until_ms', '0');
  }
}
