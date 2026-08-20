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
  getState,
  listKnownNetuids,
  setState,
  storeBlock,
  updateBlockPayload,
  upsertSubnets
} from './db';

const DEFAULT_WS = 'wss://entrypoint-finney.opentensor.ai:443';
const ROOT_NETUID = 0;
const THEORY_TIMEOUT_MS = 8_000;

async function discoverCandidateNetuids(rpc: SubtensorRpc, hash: string, known: number[]): Promise<number[]> {
  const keys = await rpc.keysPaged(PREFIX.networksAdded, hash);
  const found = keys
    .map(netuidFromIdentityStorageKey)
    .filter((n): n is number => n != null && n !== ROOT_NETUID);
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
  if (typeof value === 'string' && /^\d+$/.test(value)) return BigInt(value);
  return null;
}

async function readAlphaPrices(rpc: SubtensorRpc, atHash: string): Promise<Map<number,bigint>> {
  let decodedError: string | null = null;
  try {
    const records = await rpc.currentAlphaPricesAll(atHash);
    const prices = new Map<number,bigint>();
    for (const record of records ?? []) {
      const netuid = Number(record?.netuid);
      const price = rpcBigInt(record?.price);
      if (Number.isInteger(netuid) && netuid > 0 && price != null) prices.set(netuid, price);
    }
    if (prices.size > 0) return prices;
    decodedError = 'swap_currentAlphaPriceAll returned no subnet prices';
  } catch (error) {
    decodedError = error instanceof Error ? error.message : String(error);
  }

  try {
    const prices = decodeSubnetPrices(await rpc.currentAlphaPricesAllScale(atHash));
    if (prices.size > 0) return prices;
    throw new Error('runtime API returned no subnet prices');
  } catch (error) {
    const rawError = error instanceof Error ? error.message : String(error);
    throw new Error(`alpha price lookup failed; decoded RPC: ${decodedError ?? 'unknown'}; runtime API: ${rawError}`);
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`theory lookup timed out after ${ms}ms`)), ms);
    promise.then(
      value => { clearTimeout(timer); resolve(value); },
      error => { clearTimeout(timer); reject(error); }
    );
  });
}

async function readBlockState(
  rpc: SubtensorRpc,
  hash: string,
  blockNumber: number,
  candidates: number[]
): Promise<{ timestampMs: number; payload: BlockPayload; active: number[] }> {
  const keys: string[] = [TIMESTAMP_NOW_KEY];
  for (const n of candidates) {
    keys.push(storageKeys.networksAdded(n), storageKeys.taoInEmission(n), storageKeys.excessTao(n));
  }
  const state = await rpc.queryStorage(keys, hash);
  const timestampMs = Number(decodeU64(state.get(TIMESTAMP_NOW_KEY) ?? null));
  if (!Number.isSafeInteger(timestampMs) || timestampMs <= 0) throw new Error(`Invalid Timestamp.Now at block ${blockNumber}`);

  const payload: BlockPayload = {};
  const active: number[] = [];
  for (const netuid of candidates) {
    const isActive = decodeBool(state.get(storageKeys.networksAdded(netuid)) ?? null);
    if (!isActive) continue;
    active.push(netuid);
    const actual = decodeU64(state.get(storageKeys.taoInEmission(netuid)) ?? null);
    const chainBuy = decodeU64(state.get(storageKeys.excessTao(netuid)) ?? null);
    payload[String(netuid)] = [actual.toString(), chainBuy.toString(), null];
  }
  return { timestampMs, payload, active };
}

async function tryEnrichTheoryForBlock(
  env: Env,
  rpc: SubtensorRpc,
  hash: string,
  parentHash: string,
  blockNumber: number,
  payload: BlockPayload,
  active: number[]
): Promise<number> {
  if (!active.length) return 0;

  try {
    const alphaKeys = active.map(storageKeys.alphaOutEmission);
    const rootKeys = active.map(storageKeys.rootProp);
    const [alphaState, parentRootState, prices] = await withTimeout(
      Promise.all([
        rpc.queryStorage(alphaKeys, hash),
        rpc.queryStorage(rootKeys, parentHash),
        readAlphaPrices(rpc, parentHash)
      ]),
      THEORY_TIMEOUT_MS
    );

    let changed = 0;
    let missingAlpha = 0;
    let missingRoot = 0;
    let missingPrice = 0;

    for (const netuid of active) {
      const value = payload[String(netuid)];
      if (!value) continue;

      const alphaHex = alphaState.get(storageKeys.alphaOutEmission(netuid)) ?? null;
      const rootHex = parentRootState.get(storageKeys.rootProp(netuid)) ?? null;
      const priceRaoPerAlpha = prices.get(netuid);
      if (!alphaHex) { missingAlpha++; continue; }
      if (!rootHex) { missingRoot++; continue; }
      if (priceRaoPerAlpha == null || priceRaoPerAlpha <= 0n) { missingPrice++; continue; }

      const alphaEmissionRao = decodeU64(alphaHex);
      const rootProportionRaw = decodeU96F32Raw(rootHex);
      if (alphaEmissionRao <= 0n || rootProportionRaw <= 0n) continue;

      const theory = calculateTheoreticalInjectedRao({
        blockNumber,
        netuid,
        actualRao: BigInt(value[0]),
        chainBuyRao: BigInt(value[1]),
        rootProportionRaw,
        alphaEmissionRao,
        priceRaoPerAlpha
      });
      value[2] = theory.toString();
      changed++;
    }

    await setState(env.META_DB, 'theory_status', changed > 0 ? 'ok' : 'empty');
    await setState(env.META_DB, 'theory_last_block', String(blockNumber));
    await setState(env.META_DB, 'theory_last_enriched', String(changed));
    await setState(env.META_DB, 'theory_last_missing_alpha', String(missingAlpha));
    await setState(env.META_DB, 'theory_last_missing_root', String(missingRoot));
    await setState(env.META_DB, 'theory_last_missing_price', String(missingPrice));
    if (changed > 0) {
      await env.META_DB.prepare("DELETE FROM sync_state WHERE key='theory_last_error'").run();
      await env.META_DB.prepare("DELETE FROM sync_state WHERE key='last_error'").run();
    } else {
      const detail = `Theory produced no rows: active=${active.length}, missingAlpha=${missingAlpha}, missingRoot=${missingRoot}, missingPrice=${missingPrice}`;
      await setState(env.META_DB, 'theory_last_error', detail);
      await setState(env.META_DB, 'last_error', detail);
    }
    return changed;
  } catch (error) {
    const detail = `Theory: ${error instanceof Error ? error.message : String(error)}`;
    await setState(env.META_DB, 'theory_status', 'error');
    await setState(env.META_DB, 'theory_last_block', String(blockNumber));
    await setState(env.META_DB, 'theory_last_enriched', '0');
    await setState(env.META_DB, 'theory_last_error', detail);
    await setState(env.META_DB, 'last_error', detail);
    return 0;
  }
}

async function syncRegistry(env: Env, rpc: SubtensorRpc, hash: string, blockNumber: number, candidates: number[], active: number[]): Promise<{ added: number; removed: number }> {
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
  await setState(env.META_DB, 'registry_added_last', String(added));
  await setState(env.META_DB, 'registry_removed_last', String(removed));
  await setState(env.META_DB, 'last_registry_sync_block', String(blockNumber));
  return { added, removed };
}

export interface ScanResult {
  finalizedBlock: number;
  scanned: number;
  stored: number;
  activeSubnets: number;
  addedSubnets: number;
  removedSubnets: number;
}

export async function scanToFinalized(env: Env, maxBlocks = 24): Promise<ScanResult> {
  const rpc = new SubtensorRpc(env.SUBTENSOR_WS_URL || DEFAULT_WS);
  try {
    await rpc.connect();
    const finalizedHash = await rpc.finalizedHead();
    const finalizedHeader = await rpc.header(finalizedHash);
    const finalizedBlock = parseBlockNumber(finalizedHeader.number);

    const saved = Number(await getState(env.META_DB, 'last_finalized_block') ?? '0');
    const theoryLast = Number(await getState(env.META_DB, 'theory_last_block') ?? '0');

    let start: number;
    if (saved > 0 && theoryLast === 0) {
      start = Math.max(1, Math.min(saved, finalizedBlock) - Math.max(1, maxBlocks) + 1);
    } else {
      start = saved > 0 ? saved + 1 : finalizedBlock;
    }
    start = Math.max(start, finalizedBlock - Math.max(1, maxBlocks) + 1);
    if (start > finalizedBlock) start = finalizedBlock;

    const known = await listKnownNetuids(env.META_DB);
    const candidates = await discoverCandidateNetuids(rpc, finalizedHash, known);

    const latestActive = await readActiveNetuids(rpc, finalizedHash, candidates);
    const registry = await syncRegistry(env, rpc, finalizedHash, finalizedBlock, candidates, latestActive);

    let stored = 0;
    let parentHash = start === finalizedBlock ? finalizedHeader.parentHash : await rpc.blockHash(start - 1);
    if (!parentHash) throw new Error(`Missing parent block hash for ${start}`);

    for (let block = start; block <= finalizedBlock; block++) {
      const hash = block === finalizedBlock ? finalizedHash : await rpc.blockHash(block);
      if (!hash) throw new Error(`Missing block hash for ${block}`);

      const state = await readBlockState(rpc, hash, block, candidates);
      if (await storeBlock(env, block, hash, state.timestampMs, state.payload)) stored++;

      await setState(env.META_DB, 'last_finalized_block', String(block));
      await setState(env.META_DB, 'last_sync_ms', String(Date.now()));

      const enriched = await tryEnrichTheoryForBlock(env, rpc, hash, parentHash, block, state.payload, state.active);
      if (enriched > 0) await updateBlockPayload(env, block, state.timestampMs, state.payload);
      parentHash = hash;
    }

    const latestTimestampMs = Number(decodeU64((await rpc.queryStorage([TIMESTAMP_NOW_KEY], finalizedHash)).get(TIMESTAMP_NOW_KEY) ?? null));
    if (latestTimestampMs > 0) await ensureClosedHourlySummaries(env, latestTimestampMs);

    await setState(env.META_DB, 'rpc_status', 'ok');
    await setState(env.META_DB, 'chain_finalized_block', String(finalizedBlock));
    return {
      finalizedBlock,
      scanned: Math.max(0, finalizedBlock - start + 1),
      stored,
      activeSubnets: latestActive.length,
      addedSubnets: registry.added,
      removedSubnets: registry.removed
    };
  } catch (error) {
    await setState(env.META_DB, 'rpc_status', 'error');
    await setState(env.META_DB, 'last_error', error instanceof Error ? error.message : String(error));
    throw error;
  } finally {
    rpc.close();
  }
}
