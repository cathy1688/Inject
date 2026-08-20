import type { BlockPayload, Env, SubnetRecord } from './types';
import { SubtensorRpc, parseBlockNumber } from './rpc';
import {
  PREFIX,
  TIMESTAMP_NOW_KEY,
  decodeBool,
  decodeFirstVecUtf8,
  decodeSubnetPrices,
  decodeU64,
  decodeU96F32Raw,
  netuidFromBlake2ConcatStorageKey,
  netuidFromIdentityStorageKey,
  storageKeys
} from './storage';
import { calculateTheoreticalInjectedRao } from './theory';
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
const THEORY_FORMULA_VERSION = '5';
const THEORY_BACKFILL_BATCH = 12;

type FormulaSnapshot = {
  rootByNetuid: Map<number, bigint>;
  priceByNetuid: Map<number, bigint>;
};

type BlockState = {
  timestampMs: number;
  payload: BlockPayload;
  active: number[];
  alphaEmissionByNetuid: Map<number, bigint>;
  currentRootByNetuid: Map<number, bigint>;
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

async function readFormulaSnapshot(rpc: SubtensorRpc, hash: string, candidates: number[]): Promise<FormulaSnapshot> {
  const rootKeys = candidates.map(storageKeys.rootProp);
  const [rootState, prices] = await Promise.all([rpc.queryStorage(rootKeys, hash), readAlphaPrices(rpc, hash)]);
  const rootByNetuid = new Map<number,bigint>();
  for (const netuid of candidates) {
    // Zero is a valid RootProp. Presence of the netuid in the map means the value was read.
    rootByNetuid.set(netuid, decodeU96F32Raw(rootState.get(storageKeys.rootProp(netuid)) ?? null));
  }
  return { rootByNetuid, priceByNetuid: prices };
}

async function readBlockState(rpc: SubtensorRpc, hash: string, blockNumber: number, candidates: number[]): Promise<BlockState> {
  const keys: string[] = [TIMESTAMP_NOW_KEY];
  for (const n of candidates) {
    keys.push(storageKeys.networksAdded(n), storageKeys.taoInEmission(n), storageKeys.excessTao(n), storageKeys.alphaOutEmission(n), storageKeys.rootProp(n));
  }
  const state = await rpc.queryStorage(keys, hash);
  const timestampMs = Number(decodeU64(state.get(TIMESTAMP_NOW_KEY) ?? null));
  if (!Number.isSafeInteger(timestampMs) || timestampMs <= 0) throw new Error(`Invalid Timestamp.Now at block ${blockNumber}`);

  const payload: BlockPayload = {};
  const active: number[] = [];
  const alphaEmissionByNetuid = new Map<number,bigint>();
  const currentRootByNetuid = new Map<number,bigint>();

  for (const netuid of candidates) {
    if (!decodeBool(state.get(storageKeys.networksAdded(netuid)) ?? null)) continue;
    active.push(netuid);
    const actual = decodeU64(state.get(storageKeys.taoInEmission(netuid)) ?? null);
    const chainBuy = decodeU64(state.get(storageKeys.excessTao(netuid)) ?? null);
    payload[String(netuid)] = [actual.toString(), chainBuy.toString(), null];
    // AlphaEmission=0 and RootProp=0 are valid formula inputs, not missing values.
    alphaEmissionByNetuid.set(netuid, decodeU64(state.get(storageKeys.alphaOutEmission(netuid)) ?? null));
    currentRootByNetuid.set(netuid, decodeU96F32Raw(state.get(storageKeys.rootProp(netuid)) ?? null));
  }
  return { timestampMs, payload, active, alphaEmissionByNetuid, currentRootByNetuid };
}

async function readFormulaInputsAtBlock(rpc: SubtensorRpc, hash: string, active: number[]) {
  const keys: string[] = [];
  for (const netuid of active) keys.push(storageKeys.alphaOutEmission(netuid), storageKeys.rootProp(netuid));
  const state = await rpc.queryStorage(keys, hash);
  const alphaEmissionByNetuid = new Map<number,bigint>();
  const currentRootByNetuid = new Map<number,bigint>();
  for (const netuid of active) {
    alphaEmissionByNetuid.set(netuid, decodeU64(state.get(storageKeys.alphaOutEmission(netuid)) ?? null));
    currentRootByNetuid.set(netuid, decodeU96F32Raw(state.get(storageKeys.rootProp(netuid)) ?? null));
  }
  return { alphaEmissionByNetuid, currentRootByNetuid };
}

function applyTheory(payload: BlockPayload, active: number[], alphaEmissionByNetuid: Map<number,bigint>, previous: FormulaSnapshot): TheoryStats {
  let enriched = 0, missingRoot = 0, missingAlpha = 0, missingPrice = 0;
  for (const netuid of active) {
    const value = payload[String(netuid)];
    if (!value) continue;
    const rootRaw = previous.rootByNetuid.get(netuid);
    const alphaEmission = alphaEmissionByNetuid.get(netuid);
    const price = previous.priceByNetuid.get(netuid);
    if (rootRaw == null) { missingRoot++; continue; }
    if (alphaEmission == null) { missingAlpha++; continue; }
    if (price == null || price <= 0n) { missingPrice++; continue; }
    const theory = calculateTheoreticalInjectedRao({
      availableTaoEmissionRao: BigInt(value[0]) + BigInt(value[1]),
      rootProportionRaw: rootRaw,
      alphaEmissionRao: alphaEmission,
      priceRaoPerAlpha: price
    });
    if (theory == null) continue;
    value[2] = theory.toString();
    enriched++;
  }
  return { enriched, missingRoot, missingAlpha, missingPrice };
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

async function backfillTheory(env: Env, rpc: SubtensorRpc, batchSize = THEORY_BACKFILL_BATCH): Promise<void> {
  const formulaVersion = await getState(env.META_DB, 'theory_formula_version');
  let cursorState = await getState(env.META_DB, 'theory_backfill_cursor');
  if (formulaVersion !== THEORY_FORMULA_VERSION || cursorState == null) {
    const earliest = await getEarliestStoredBlockNumber(env);
    await setState(env.META_DB, 'theory_formula_version', THEORY_FORMULA_VERSION);
    if (earliest == null) {
      await setState(env.META_DB, 'theory_backfill_status', 'complete');
      return;
    }
    cursorState = String(earliest - 1);
    await setState(env.META_DB, 'theory_backfill_cursor', cursorState);
    await setState(env.META_DB, 'theory_backfill_status', 'running');
  }

  let cursor = Number(cursorState);
  const rows = await getStoredBlocksAfter(env, cursor, batchSize);
  if (!rows.length) {
    await setState(env.META_DB, 'theory_backfill_status', 'complete');
    await setState(env.META_DB, 'theory_backfill_last_processed', '0');
    return;
  }

  let previousFormula: FormulaSnapshot | null = null;
  let previousBlock = -1;
  let processed = 0;
  const touchedHours = new Set<number>();

  for (const row of rows) {
    const payload = JSON.parse(row.payload) as BlockPayload;
    const active = Object.keys(payload).map(Number).filter(n => Number.isInteger(n) && n > 0);
    if (!active.length) {
      cursor = row.block_number; processed++;
      await setState(env.META_DB, 'theory_backfill_cursor', String(cursor));
      continue;
    }
    if (previousFormula == null || row.block_number !== previousBlock + 1) {
      const header = await rpc.header(row.block_hash);
      previousFormula = await readFormulaSnapshot(rpc, header.parentHash, active);
    }
    const inputs = await readFormulaInputsAtBlock(rpc, row.block_hash, active);
    const stats = applyTheory(payload, active, inputs.alphaEmissionByNetuid, previousFormula);
    await setState(env.META_DB, 'theory_backfill_missing_root', String(stats.missingRoot));
    await setState(env.META_DB, 'theory_backfill_missing_alpha', String(stats.missingAlpha));
    await setState(env.META_DB, 'theory_backfill_missing_price', String(stats.missingPrice));
    if (stats.enriched !== active.length) {
      await setState(env.META_DB, 'theory_backfill_status', 'partial');
      break;
    }

    await updateBlockPayload(env, row.block_number, row.timestamp_ms, payload);
    touchedHours.add(Math.floor(row.timestamp_ms / HOUR_MS) * HOUR_MS);
    previousFormula = { rootByNetuid: inputs.currentRootByNetuid, priceByNetuid: await readAlphaPrices(rpc, row.block_hash) };
    previousBlock = row.block_number;
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
        lastTheory = applyTheory(state.payload, state.active, state.alphaEmissionByNetuid, previousFormula);
        if (await storeBlock(env, block, hash, state.timestampMs, state.payload)) stored++;
        else if (lastTheory.enriched > 0) await updateBlockPayload(env, block, state.timestampMs, state.payload);
        await setState(env.META_DB, 'last_finalized_block', String(block));
        await setState(env.META_DB, 'last_sync_ms', String(Date.now()));
        if (lastTheory.enriched > 0) await setState(env.META_DB, 'theory_last_block', String(block));
        previousFormula = { rootByNetuid: state.currentRootByNetuid, priceByNetuid: await readAlphaPrices(rpc, hash) };
      }
    }

    await setState(env.META_DB, 'theory_status', lastTheory.enriched > 0 ? 'ok' : 'waiting');
    await setState(env.META_DB, 'theory_last_enriched', String(lastTheory.enriched));
    await setState(env.META_DB, 'theory_last_missing_root', String(lastTheory.missingRoot));
    await setState(env.META_DB, 'theory_last_missing_alpha', String(lastTheory.missingAlpha));
    await setState(env.META_DB, 'theory_last_missing_price', String(lastTheory.missingPrice));

    // Backfill is independent from the live cursor. Each invocation advances a
    // bounded batch, so normal collection stays responsive while old null rows disappear.
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
