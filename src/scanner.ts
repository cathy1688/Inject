import type { BlockPayload, Env, SubnetRecord } from './types';
import { SubtensorRpc, parseBlockNumber } from './rpc';
import {
  PREFIX,
  TIMESTAMP_NOW_KEY,
  decodeBool,
  decodeFirstVecUtf8,
  decodeI96F32Raw,
  decodeSubnetPrices,
  decodeU16,
  decodeU64,
  decodeU64F64Raw,
  decodeU96F32Raw,
  globalStorageKeys,
  netuidFromBlake2ConcatStorageKey,
  netuidFromIdentityStorageKey,
  storageKeys
} from './storage';
import {
  calculateIndependentTheory,
  DEFAULT_GATE_EXPONENT_RAW,
  DEFAULT_GATE_QUANTILE_RAW,
  DEFAULT_GATE_RANK,
  type TheorySnapshot,
  type TheorySubnetState
} from './theory';
import {
  ensureClosedHourlySummaries,
  getEarliestStoredBlockNumber,
  getState,
  getStoredBlocksAfter,
  HOUR_MS,
  listKnownNetuids,
  rebuildHourlySummary,
  setState,
  storeBlock,
  updateBlockPayload,
  upsertSubnets
} from './db';

const DEFAULT_WS = 'wss://entrypoint-finney.opentensor.ai:443';
const ROOT_NETUID = 0;
const THEORY_FORMULA_VERSION = '6-independent';
const THEORY_BACKFILL_BATCH = 4;

type BlockState = {
  timestampMs: number;
  payload: BlockPayload;
  active: number[];
  nextFormula: TheorySnapshot;
};

type TheoryStats = { enriched: number; missingRoot: number; missingAlpha: number; missingPrice: number };

async function discoverCandidateNetuids(rpc: SubtensorRpc, hash: string, known: number[]): Promise<number[]> {
  const keys = await rpc.keysPaged(PREFIX.networksAdded, hash);
  const found = keys.map(netuidFromIdentityStorageKey).filter((n): n is number => n != null && n !== ROOT_NETUID);
  return [...new Set([...known.filter(n => n !== ROOT_NETUID), ...found])].sort((a,b) => a-b);
}

async function readSubnetNames(rpc: SubtensorRpc, hash: string): Promise<Map<number,string>> {
  const keys = await rpc.keysPaged(PREFIX.subnetIdentitiesV3, hash);
  if (!keys.length) return new Map();
  const values = await rpc.queryStorage(keys, hash);
  const names = new Map<number,string>();
  for (const key of keys) {
    const netuid = netuidFromBlake2ConcatStorageKey(key);
    if (netuid == null || netuid === ROOT_NETUID) continue;
    const name = decodeFirstVecUtf8(values.get(key) ?? null);
    if (name) names.set(netuid, name);
  }
  return names;
}

async function readActiveNetuids(rpc: SubtensorRpc, hash: string, candidates: number[]): Promise<number[]> {
  const keys = candidates.map(storageKeys.networksAdded);
  const state = await rpc.queryStorage(keys, hash);
  return candidates.filter(netuid => decodeBool(state.get(storageKeys.networksAdded(netuid)) ?? null));
}

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
  if (totalIssuanceHex == null) throw new Error('Missing Subtensor.TotalIssuance for theory');
  if (taoWeightHex == null) throw new Error('Missing Subtensor.TaoWeight for theory');

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

/** End-state of block N is the complete pre-coinbase state for block N+1. */
async function readFormulaSnapshot(rpc: SubtensorRpc, hash: string, candidates: number[]): Promise<TheorySnapshot> {
  const [state, prices] = await Promise.all([
    rpc.queryStorage(formulaStorageKeys(candidates), hash),
    readAlphaPrices(rpc, hash)
  ]);
  return decodeFormulaSnapshot(state, candidates, prices);
}

/**
 * Read observed outputs and, in the same storage pass, all independent formula
 * state needed by the next block. Actual injection/Chain Buy are never fed back
 * into the theory engine.
 */
async function readBlockState(rpc: SubtensorRpc, hash: string, blockNumber: number, candidates: number[]): Promise<BlockState> {
  const keys = new Set<string>([TIMESTAMP_NOW_KEY, ...formulaStorageKeys(candidates)]);
  for (const netuid of candidates) {
    keys.add(storageKeys.taoInEmission(netuid));
    keys.add(storageKeys.excessTao(netuid));
  }
  const [state, prices] = await Promise.all([
    rpc.queryStorage([...keys], hash),
    readAlphaPrices(rpc, hash)
  ]);
  const timestampMs = Number(decodeU64(state.get(TIMESTAMP_NOW_KEY) ?? null));
  if (!Number.isSafeInteger(timestampMs) || timestampMs <= 0) throw new Error(`Invalid Timestamp.Now at block ${blockNumber}`);

  const nextFormula = decodeFormulaSnapshot(state, candidates, prices);
  const payload: BlockPayload = {};
  const active: number[] = [];
  for (const subnet of nextFormula.subnets) {
    if (!subnet.networkAdded) continue;
    active.push(subnet.netuid);
    const actual = decodeU64(state.get(storageKeys.taoInEmission(subnet.netuid)) ?? null);
    const chainBuy = decodeU64(state.get(storageKeys.excessTao(subnet.netuid)) ?? null);
    payload[String(subnet.netuid)] = [actual.toString(), chainBuy.toString(), null];
  }
  return { timestampMs, payload, active, nextFormula };
}

function applyTheory(payload: BlockPayload, active: number[], previous: TheorySnapshot, blockNumber: number): TheoryStats {
  let enriched = 0, missingPrice = 0;
  const results = calculateIndependentTheory(previous, blockNumber);
  for (const netuid of active) {
    const value = payload[String(netuid)];
    if (!value) continue;
    const theory = results.get(netuid);
    if (!theory) continue;
    if (theory.theoreticalInjectedRao == null) {
      if (theory.missingPrice) missingPrice++;
      continue;
    }
    value[2] = theory.theoreticalInjectedRao.toString();
    enriched++;
  }
  return { enriched, missingRoot: 0, missingAlpha: 0, missingPrice };
}

async function syncRegistry(env: Env, rpc: SubtensorRpc, hash: string, blockNumber: number, candidates: number[], active: number[]) {
  const names = await readSubnetNames(rpc, hash);
  const safeActive = active.filter(n => n !== ROOT_NETUID);
  const counterKeys = safeActive.map(storageKeys.registeredSubnetCounter);
  const counters = await rpc.queryStorage(counterKeys, hash);
  const now = Date.now();
  const previous = await env.META_DB.prepare('SELECT netuid,status,first_seen_block,name FROM subnets WHERE netuid > 0').all<{netuid:number;status:string;first_seen_block:number;name:string}>();
  const prevMap = new Map(previous.results.map(r => [Number(r.netuid), r]));
  const activeSet = new Set(safeActive);
  let added = 0, removed = 0;
  const rows: SubnetRecord[] = [];
  for (const netuid of candidates) {
    if (netuid === ROOT_NETUID) continue;
    const prev = prevMap.get(netuid);
    const isActive = activeSet.has(netuid);
    if (isActive && (!prev || prev.status !== 'active')) added++;
    if (!isActive && prev?.status === 'active') removed++;
    if (!prev && !isActive) continue;
    const counter = isActive ? decodeU64(counters.get(storageKeys.registeredSubnetCounter(netuid)) ?? null).toString() : null;
    rows.push({
      netuid, registration_counter: counter, name: names.get(netuid) ?? prev?.name ?? `Subnet ${netuid}`,
      status: isActive ? 'active' : 'deregistered', first_seen_block: prev?.first_seen_block ?? blockNumber,
      last_seen_block: blockNumber, updated_at_ms: now
    });
  }
  await upsertSubnets(env.META_DB, rows);
  await env.META_DB.prepare('DELETE FROM subnets WHERE netuid = 0').run();
  await setState(env.META_DB, 'registry_added_last', String(added));
  await setState(env.META_DB, 'registry_removed_last', String(removed));
  await setState(env.META_DB, 'last_registry_sync_block', String(blockNumber));
  return { added, removed };
}

/**
 * Recalculate retained V4/V5 rows with the independent V6 model. Historical
 * actual injection and Chain Buy are preserved; only payload[2] is replaced.
 */
async function backfillTheory(env: Env, rpc: SubtensorRpc, batchSize = THEORY_BACKFILL_BATCH): Promise<void> {
  const formulaVersion = await getState(env.META_DB, 'theory_formula_version');
  let cursorState = await getState(env.META_DB, 'theory_backfill_cursor');
  if (formulaVersion !== THEORY_FORMULA_VERSION) {
    const earliest = await getEarliestStoredBlockNumber(env);
    await setState(env.META_DB, 'theory_formula_version', THEORY_FORMULA_VERSION);
    await env.META_DB.prepare("DELETE FROM sync_state WHERE key IN ('theory_backfill_error','theory_backfill_missing_root','theory_backfill_missing_alpha','theory_backfill_missing_price')").run();
    if (earliest == null) {
      await setState(env.META_DB, 'theory_backfill_status', 'complete');
      return;
    }
    cursorState = String(earliest - 1);
    await setState(env.META_DB, 'theory_backfill_cursor', cursorState);
    await setState(env.META_DB, 'theory_backfill_status', 'running');
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
    const previousFormula = await readFormulaSnapshot(rpc, header.parentHash, candidates);
    const stats = applyTheory(payload, candidates, previousFormula, row.block_number);
    await setState(env.META_DB, 'theory_backfill_missing_root', String(stats.missingRoot));
    await setState(env.META_DB, 'theory_backfill_missing_alpha', String(stats.missingAlpha));
    await setState(env.META_DB, 'theory_backfill_missing_price', String(stats.missingPrice));
    if (stats.enriched !== candidates.length) {
      await setState(env.META_DB, 'theory_backfill_status', 'partial');
      break;
    }

    await updateBlockPayload(env, row.block_number, row.timestamp_ms, payload);
    touchedHours.add(Math.floor(row.timestamp_ms / HOUR_MS) * HOUR_MS);
    cursor = row.block_number;
    processed++;
    await setState(env.META_DB, 'theory_backfill_cursor', String(cursor));
  }

  for (const hour of touchedHours) await rebuildHourlySummary(env, hour);
  await setState(env.META_DB, 'theory_backfill_last_processed', String(processed));
  if (processed === rows.length) {
    const more = await getStoredBlocksAfter(env, cursor, 1);
    await setState(env.META_DB, 'theory_backfill_status', more.length ? 'running' : 'complete');
  }
}

export interface ScanResult { finalizedBlock:number; scanned:number; stored:number; activeSubnets:number; addedSubnets:number; removedSubnets:number; }

export async function scanToFinalized(env: Env, maxBlocks = 24): Promise<ScanResult> {
  const rpc = new SubtensorRpc(env.SUBTENSOR_WS_URL || DEFAULT_WS);
  try {
    await rpc.connect();
    const finalizedHash = await rpc.finalizedHead();
    const finalizedHeader = await rpc.header(finalizedHash);
    const finalizedBlock = parseBlockNumber(finalizedHeader.number);
    const saved = Number(await getState(env.META_DB, 'last_finalized_block') ?? '0');
    const start = saved > 0 ? saved + 1 : finalizedBlock;
    const end = Math.min(finalizedBlock, start + Math.max(1, maxBlocks) - 1);

    const known = await listKnownNetuids(env.META_DB);
    const candidates = await discoverCandidateNetuids(rpc, finalizedHash, known);
    const latestActive = await readActiveNetuids(rpc, finalizedHash, candidates);
    const registry = await syncRegistry(env, rpc, finalizedHash, finalizedBlock, candidates, latestActive);

    let stored = 0;
    let lastTheory: TheoryStats = { enriched:0, missingRoot:0, missingAlpha:0, missingPrice:0 };
    if (start <= end) {
      const parentHash = start === finalizedBlock ? finalizedHeader.parentHash : await rpc.blockHash(start - 1);
      if (!parentHash) throw new Error(`Missing parent block hash for ${start}`);
      let previousFormula = await readFormulaSnapshot(rpc, parentHash, candidates);
      for (let block = start; block <= end; block++) {
        const hash = block === finalizedBlock ? finalizedHash : await rpc.blockHash(block);
        if (!hash) throw new Error(`Missing block hash for ${block}`);
        const state = await readBlockState(rpc, hash, block, candidates);
        lastTheory = applyTheory(state.payload, state.active, previousFormula, block);
        if (await storeBlock(env, block, hash, state.timestampMs, state.payload)) stored++;
        else if (lastTheory.enriched > 0) await updateBlockPayload(env, block, state.timestampMs, state.payload);
        await setState(env.META_DB, 'last_finalized_block', String(block));
        await setState(env.META_DB, 'last_sync_ms', String(Date.now()));
        if (lastTheory.enriched > 0) await setState(env.META_DB, 'theory_last_block', String(block));
        previousFormula = state.nextFormula;
      }
    }

    await setState(env.META_DB, 'theory_status', lastTheory.enriched > 0 ? 'ok' : 'waiting');
    await setState(env.META_DB, 'theory_last_enriched', String(lastTheory.enriched));
    await setState(env.META_DB, 'theory_last_missing_root', String(lastTheory.missingRoot));
    await setState(env.META_DB, 'theory_last_missing_alpha', String(lastTheory.missingAlpha));
    await setState(env.META_DB, 'theory_last_missing_price', String(lastTheory.missingPrice));

    try { await backfillTheory(env, rpc); }
    catch (error) {
      await setState(env.META_DB, 'theory_backfill_status', 'error');
      await setState(env.META_DB, 'theory_backfill_error', error instanceof Error ? error.message : String(error));
    }

    const latestTimestampMs = Number(decodeU64((await rpc.queryStorage([TIMESTAMP_NOW_KEY], finalizedHash)).get(TIMESTAMP_NOW_KEY) ?? null));
    if (latestTimestampMs > 0) await ensureClosedHourlySummaries(env, latestTimestampMs);
    await setState(env.META_DB, 'rpc_status', 'ok');
    await setState(env.META_DB, 'chain_finalized_block', String(finalizedBlock));
    await env.META_DB.prepare("DELETE FROM sync_state WHERE key='last_error'").run();
    return { finalizedBlock, scanned: start <= end ? end-start+1 : 0, stored, activeSubnets: latestActive.length, addedSubnets: registry.added, removedSubnets: registry.removed };
  } catch (error) {
    await setState(env.META_DB, 'rpc_status', 'error');
    await setState(env.META_DB, 'last_error', error instanceof Error ? error.message : String(error));
    throw error;
  } finally { rpc.close(); }
}
