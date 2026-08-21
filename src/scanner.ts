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
  THEORY_MODEL_VERSION,
  type TheorySnapshot,
  type TheorySubnetState
} from './theory';
import {
  ensureClosedHourlySummaries,
  getState,
  listKnownNetuids,
  setState,
  storeBlock,
  updateBlockPayload,
  upsertSubnets
} from './db';

const DEFAULT_WS = 'wss://entrypoint-finney.opentensor.ai:443';
const ROOT_NETUID = 0;

// A normal invocation usually sees 0-1 new block. If the worker was offline for
// hours, processing 24 historical blocks plus D1/theory work can exceed the
// Cloudflare per-invocation subrequest limit. Catch-up therefore uses small,
// core-only batches. Once close to the head, full independent theory resumes.
const CATCHUP_THRESHOLD_BLOCKS = 64;
const CATCHUP_BATCH_BLOCKS = 8;
const LIVE_BATCH_BLOCKS = 6;

type CoreBlockState = {
  timestampMs: number;
  payload: BlockPayload;
  active: number[];
};

type TheoryStats = {
  enriched: number;
  missingRoot: number;
  missingAlpha: number;
  missingPrice: number;
  equalActual: number;
  differentActual: number;
};

function rpcBigInt(value: unknown): bigint | null {
  if (typeof value === 'bigint') return value >= 0n ? value : null;
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return BigInt(value);
  if (typeof value === 'string') {
    if (/^\d+$/.test(value)) return BigInt(value);
    if (/^0x[0-9a-f]+$/i.test(value)) return BigInt(value);
  }
  return null;
}

async function writeSyncStates(db: D1Database, values: Record<string, string>): Promise<void> {
  const now = Date.now();
  const statements = Object.entries(values).map(([key, value]) => db.prepare(`
    INSERT INTO sync_state(key,value,updated_at_ms) VALUES(?,?,?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at_ms=excluded.updated_at_ms
    WHERE sync_state.value <> excluded.value
  `).bind(key, value, now));
  if (statements.length) await db.batch(statements);
}

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

async function readFormulaSnapshot(rpc: SubtensorRpc, hash: string, candidates: number[]): Promise<TheorySnapshot> {
  const [state, prices] = await Promise.all([
    rpc.queryStorage(formulaStorageKeys(candidates), hash),
    readAlphaPrices(rpc, hash)
  ]);
  return decodeFormulaSnapshot(state, candidates, prices);
}

async function readCoreBlockState(rpc: SubtensorRpc, hash: string, blockNumber: number, candidates: number[]): Promise<CoreBlockState> {
  const keys: string[] = [TIMESTAMP_NOW_KEY];
  for (const netuid of candidates) {
    keys.push(storageKeys.networksAdded(netuid), storageKeys.taoInEmission(netuid), storageKeys.excessTao(netuid));
  }
  const state = await rpc.queryStorage(keys, hash);
  const timestampMs = Number(decodeU64(state.get(TIMESTAMP_NOW_KEY) ?? null));
  if (!Number.isSafeInteger(timestampMs) || timestampMs <= 0) throw new Error(`Invalid Timestamp.Now at block ${blockNumber}`);

  const payload: BlockPayload = {};
  const active: number[] = [];
  for (const netuid of candidates) {
    if (!decodeBool(state.get(storageKeys.networksAdded(netuid)) ?? null)) continue;
    active.push(netuid);
    const actual = decodeU64(state.get(storageKeys.taoInEmission(netuid)) ?? null);
    const chainBuy = decodeU64(state.get(storageKeys.excessTao(netuid)) ?? null);
    payload[String(netuid)] = [actual.toString(), chainBuy.toString(), null];
  }
  return { timestampMs, payload, active };
}

function applyTheory(payload: BlockPayload, active: number[], previous: TheorySnapshot, blockNumber: number): TheoryStats {
  let enriched = 0, missingPrice = 0, equalActual = 0, differentActual = 0;
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
    if (BigInt(value[0]) === theory.theoreticalInjectedRao) equalActual++;
    else differentActual++;
  }
  return { enriched, missingRoot: 0, missingAlpha: 0, missingPrice, equalActual, differentActual };
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
      netuid,
      registration_counter: counter,
      name: names.get(netuid) ?? prev?.name ?? `Subnet ${netuid}`,
      status: isActive ? 'active' : 'deregistered',
      first_seen_block: prev?.first_seen_block ?? blockNumber,
      last_seen_block: blockNumber,
      updated_at_ms: now
    });
  }
  await upsertSubnets(env.META_DB, rows);
  await env.META_DB.prepare('DELETE FROM subnets WHERE netuid = 0').run();
  await writeSyncStates(env.META_DB, {
    registry_added_last: String(added),
    registry_removed_last: String(removed),
    last_registry_sync_block: String(blockNumber)
  });
  return { added, removed };
}

export interface ScanResult {
  finalizedBlock: number;
  processedThroughBlock: number;
  remainingLag: number;
  syncMode: 'live' | 'catchup';
  scanned: number;
  stored: number;
  activeSubnets: number;
  addedSubnets: number;
  removedSubnets: number;
}

export async function scanToFinalized(env: Env, maxBlocks = 24): Promise<ScanResult> {
  const rpc = new SubtensorRpc(env.SUBTENSOR_WS_URL || DEFAULT_WS);
  let finalizedBlock = 0;
  let saved = 0;
  let catchupMode = false;
  try {
    await rpc.connect();
    const finalizedHash = await rpc.finalizedHead();
    const finalizedHeader = await rpc.header(finalizedHash);
    finalizedBlock = parseBlockNumber(finalizedHeader.number);
    saved = Number(await getState(env.META_DB, 'last_finalized_block') ?? '0');

    const lagAtStart = saved > 0 ? Math.max(0, finalizedBlock - saved) : 0;
    catchupMode = saved > 0 && lagAtStart > CATCHUP_THRESHOLD_BLOCKS;
    const requested = Math.max(1, Math.trunc(maxBlocks));
    const effectiveMax = catchupMode
      ? Math.min(requested, CATCHUP_BATCH_BLOCKS)
      : Math.min(requested, LIVE_BATCH_BLOCKS);
    const start = saved > 0 ? saved + 1 : finalizedBlock;
    const end = Math.min(finalizedBlock, start + effectiveMax - 1);

    // Publish the real chain head immediately. Even if catch-up later fails,
    // /api/status can distinguish chain head from the durable local cursor.
    await writeSyncStates(env.META_DB, {
      chain_finalized_block: String(finalizedBlock),
      sync_mode: catchupMode ? 'catchup' : 'live',
      sync_lag_blocks: String(lagAtStart)
    });

    const formulaVersionBefore = await getState(env.META_DB, 'theory_formula_version');
    if (formulaVersionBefore !== THEORY_MODEL_VERSION) {
      await setState(env.META_DB, 'theory_live_start_block', String(start));
    }

    const known = await listKnownNetuids(env.META_DB);
    const candidates = await discoverCandidateNetuids(rpc, finalizedHash, known);
    const latestActive = await readActiveNetuids(rpc, finalizedHash, candidates);

    // Registry metadata is current-head information. During a large historical
    // catch-up it does not need to be rewritten every 12 seconds.
    const lastRegistrySync = Number(await getState(env.META_DB, 'last_registry_sync_block') ?? '0');
    const shouldSyncRegistry = !catchupMode || lastRegistrySync === 0 || finalizedBlock - lastRegistrySync >= 120;
    const registry = shouldSyncRegistry
      ? await syncRegistry(env, rpc, finalizedHash, finalizedBlock, candidates, latestActive)
      : { added: 0, removed: 0 };

    let stored = 0;
    let processedThrough = saved;
    let lastProcessedTimestampMs = 0;
    let lastTheoryStats: TheoryStats | null = null;
    let lastTheoryBlock = 0;
    let theoryError: string | null = null;
    let parentHash = start === finalizedBlock ? finalizedHeader.parentHash : (start <= end ? await rpc.blockHash(start - 1) : null);

    for (let block = start; block <= end; block++) {
      const hash = block === finalizedBlock ? finalizedHash : await rpc.blockHash(block);
      if (!hash) throw new Error(`Missing block hash for ${block}`);
      if (!parentHash) {
        const header = await rpc.header(hash);
        parentHash = header.parentHash;
      }

      const state = await readCoreBlockState(rpc, hash, block, candidates);

      // While far behind, prioritize durable observed data. Independent theory
      // is intentionally deferred to the archive backfiller, avoiding duplicate
      // historical RPC + D1 writes in the same invocation.
      if (!catchupMode) {
        try {
          const previousFormula = await readFormulaSnapshot(rpc, parentHash, candidates);
          const stats = applyTheory(state.payload, state.active, previousFormula, block);
          lastTheoryStats = stats;
          lastTheoryBlock = block;
          theoryError = null;
        } catch (error) {
          theoryError = error instanceof Error ? error.message : String(error);
        }
      }

      if (await storeBlock(env, block, hash, state.timestampMs, state.payload)) stored++;
      else if (!catchupMode && lastTheoryStats?.enriched) {
        await updateBlockPayload(env, block, state.timestampMs, state.payload);
      }

      processedThrough = block;
      lastProcessedTimestampMs = state.timestampMs;
      parentHash = hash;
    }

    // Hourly summaries must only advance to the time actually collected. Using
    // the current chain-head timestamp while thousands of blocks are missing
    // would create empty hours and can itself exhaust Cloudflare subrequests.
    if (lastProcessedTimestampMs > 0) {
      await ensureClosedHourlySummaries(env, lastProcessedTimestampMs);
    }

    const remainingLag = Math.max(0, finalizedBlock - Math.max(processedThrough, saved));
    const finalMode: 'live' | 'catchup' = remainingLag > CATCHUP_THRESHOLD_BLOCKS ? 'catchup' : 'live';
    const stateValues: Record<string,string> = {
      last_finalized_block: String(Math.max(processedThrough, saved)),
      last_sync_ms: String(Date.now()),
      rpc_status: 'ok',
      sync_mode: finalMode,
      sync_lag_blocks: String(remainingLag)
    };

    if (lastTheoryStats && lastTheoryBlock > 0) {
      stateValues.theory_status = lastTheoryStats.enriched > 0 ? 'ok' : 'partial';
      stateValues.theory_last_block = String(lastTheoryBlock);
      stateValues.theory_last_enriched = String(lastTheoryStats.enriched);
      stateValues.theory_last_missing_root = String(lastTheoryStats.missingRoot);
      stateValues.theory_last_missing_alpha = String(lastTheoryStats.missingAlpha);
      stateValues.theory_last_missing_price = String(lastTheoryStats.missingPrice);
      stateValues.theory_last_equal_actual = String(lastTheoryStats.equalActual);
      stateValues.theory_last_different_actual = String(lastTheoryStats.differentActual);
    } else if (theoryError) {
      stateValues.theory_status = 'partial';
      stateValues.theory_last_error = theoryError;
    }

    await writeSyncStates(env.META_DB, stateValues);
    await env.META_DB.prepare("DELETE FROM sync_state WHERE key='last_error'").run();
    if (lastTheoryStats && !theoryError) {
      await env.META_DB.prepare("DELETE FROM sync_state WHERE key='theory_last_error'").run();
    }

    return {
      finalizedBlock,
      processedThroughBlock: Math.max(processedThrough, saved),
      remainingLag,
      syncMode: finalMode,
      scanned: start <= end ? end - start + 1 : 0,
      stored,
      activeSubnets: latestActive.length,
      addedSubnets: registry.added,
      removedSubnets: registry.removed
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const remainingLag = finalizedBlock > 0 ? Math.max(0, finalizedBlock - saved) : 0;
    await writeSyncStates(env.META_DB, {
      rpc_status: 'error',
      last_error: message,
      ...(finalizedBlock > 0 ? { chain_finalized_block: String(finalizedBlock) } : {}),
      sync_mode: catchupMode ? 'catchup' : 'live',
      sync_lag_blocks: String(remainingLag)
    });
    throw error;
  } finally {
    rpc.close();
  }
}
