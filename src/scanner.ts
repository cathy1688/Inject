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
import { ensureClosedHourlySummaries, getState, listKnownNetuids, setState, storeBlock, upsertSubnets } from './db';

const DEFAULT_WS = 'wss://entrypoint-finney.opentensor.ai:443';
const ROOT_NETUID = 0;

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

function rpcBigInt(value: unknown): bigint | null {
  if (typeof value === 'bigint') return value >= 0n ? value : null;
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return BigInt(value);
  if (typeof value === 'string' && /^\d+$/.test(value)) return BigInt(value);
  return null;
}

/**
 * Price lookup is deliberately non-fatal. Actual injection / Chain Buy archival
 * must continue even if an endpoint temporarily disables one price RPC.
 */
async function readAlphaPrices(rpc: SubtensorRpc, atHash: string): Promise<Map<number,bigint>> {
  try {
    const records = await rpc.currentAlphaPricesAll(atHash);
    const prices = new Map<number,bigint>();
    for (const record of records ?? []) {
      const netuid = Number(record?.netuid);
      const price = rpcBigInt(record?.price);
      if (Number.isInteger(netuid) && netuid > 0 && price != null) prices.set(netuid, price);
    }
    if (prices.size > 0) return prices;
  } catch {
    // Fall through to the raw runtime API below.
  }

  try {
    return decodeSubnetPrices(await rpc.currentAlphaPricesAllScale(atHash));
  } catch {
    return new Map();
  }
}

/**
 * Read one finalized block plus the pre-coinbase state used by the runtime formula.
 * RootProp is updated after coinbase, so block N theory uses RootProp at block N-1.
 * The alpha spot price is also read at block N-1.
 */
async function readBlockState(
  rpc: SubtensorRpc,
  hash: string,
  parentHash: string,
  blockNumber: number,
  candidates: number[]
): Promise<{ timestampMs: number; payload: BlockPayload; active: number[] }> {
  const currentKeys: string[] = [TIMESTAMP_NOW_KEY];
  for (const n of candidates) {
    currentKeys.push(
      storageKeys.networksAdded(n),
      storageKeys.taoInEmission(n),
      storageKeys.excessTao(n),
      storageKeys.alphaOutEmission(n)
    );
  }
  const parentRootKeys = candidates.map(storageKeys.rootProp);

  // Price lookup catches its own failures, so theory can never block actual chain collection.
  const [state, parentRootState, prices] = await Promise.all([
    rpc.queryStorage(currentKeys, hash),
    rpc.queryStorage(parentRootKeys, parentHash),
    readAlphaPrices(rpc, parentHash)
  ]);

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
    const alphaEmission = decodeU64(state.get(storageKeys.alphaOutEmission(netuid)) ?? null);
    const rootProportionRaw = decodeU96F32Raw(parentRootState.get(storageKeys.rootProp(netuid)) ?? null);
    const priceRaoPerAlpha = prices.get(netuid);

    const theory = priceRaoPerAlpha == null
      ? null
      : calculateTheoreticalInjectedRao({
          blockNumber,
          netuid,
          actualRao: actual,
          chainBuyRao: chainBuy,
          rootProportionRaw,
          alphaEmissionRao: alphaEmission,
          priceRaoPerAlpha
        });

    payload[String(netuid)] = [actual.toString(), chainBuy.toString(), theory?.toString() ?? null];
  }
  return { timestampMs, payload, active };
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
    let start = saved > 0 ? saved + 1 : finalizedBlock;
    start = Math.max(start, finalizedBlock - Math.max(1, maxBlocks) + 1);

    const known = await listKnownNetuids(env.META_DB);
    const candidates = await discoverCandidateNetuids(rpc, finalizedHash, known);
    let stored = 0;
    let lastActive: number[] = [];
    let parentHash = start === finalizedBlock ? finalizedHeader.parentHash : await rpc.blockHash(start - 1);
    if (!parentHash) throw new Error(`Missing parent block hash for ${start}`);

    for (let block = start; block <= finalizedBlock; block++) {
      const hash = block === finalizedBlock ? finalizedHash : await rpc.blockHash(block);
      if (!hash) throw new Error(`Missing block hash for ${block}`);
      const state = await readBlockState(rpc, hash, parentHash, block, candidates);
      lastActive = state.active;
      if (await storeBlock(env, block, hash, state.timestampMs, state.payload)) stored++;
      await setState(env.META_DB, 'last_finalized_block', String(block));
      await setState(env.META_DB, 'last_sync_ms', String(Date.now()));
      parentHash = hash;
    }

    const latestTimestampMs = Number(decodeU64((await rpc.queryStorage([TIMESTAMP_NOW_KEY], finalizedHash)).get(TIMESTAMP_NOW_KEY) ?? null));
    if (latestTimestampMs > 0) await ensureClosedHourlySummaries(env, latestTimestampMs);
    const registry = await syncRegistry(env, rpc, finalizedHash, finalizedBlock, candidates, lastActive);
    await setState(env.META_DB, 'rpc_status', 'ok');
    await setState(env.META_DB, 'chain_finalized_block', String(finalizedBlock));
    await env.META_DB.prepare("DELETE FROM sync_state WHERE key='last_error'").run();
    return {
      finalizedBlock,
      scanned: Math.max(0, finalizedBlock - start + 1),
      stored,
      activeSubnets: lastActive.filter(n => n !== ROOT_NETUID).length,
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
