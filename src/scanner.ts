import type { BlockPayload, Env, SubnetRecord } from './types';
import { SubtensorRpc, parseBlockNumber } from './rpc';
import { PREFIX, TIMESTAMP_NOW_KEY, decodeBool, decodeFirstVecUtf8, decodeU64, netuidFromBlake2ConcatStorageKey, netuidFromIdentityStorageKey, storageKeys } from './storage';
import { calculateTheoreticalInjectedRao } from './theory';
import { getState, listKnownNetuids, setState, storeBlock, updateSummariesForNewBlocks, upsertSubnets } from './db';

const DEFAULT_WS = 'wss://entrypoint-finney.opentensor.ai:443';

async function discoverCandidateNetuids(rpc: SubtensorRpc, hash: string, known: number[]): Promise<number[]> {
  const keys = await rpc.keysPaged(PREFIX.networksAdded, hash);
  const found = keys.map(netuidFromIdentityStorageKey).filter((n): n is number => n != null);
  return [...new Set([...known, ...found])].sort((a,b) => a-b);
}

async function readSubnetNames(rpc: SubtensorRpc, hash: string): Promise<Map<number,string>> {
  const keys = await rpc.keysPaged(PREFIX.subnetIdentitiesV3, hash);
  if (!keys.length) return new Map();
  const values = await rpc.queryStorage(keys, hash);
  const names = new Map<number,string>();
  for (const key of keys) {
    const netuid = netuidFromBlake2ConcatStorageKey(key);
    if (netuid == null) continue;
    const name = decodeFirstVecUtf8(values.get(key) ?? null);
    if (name) names.set(netuid, name);
  }
  return names;
}

async function readBlockState(rpc: SubtensorRpc, hash: string, blockNumber: number, candidates: number[]): Promise<{ timestampMs: number; payload: BlockPayload; active: number[] }> {
  const keys: string[] = [TIMESTAMP_NOW_KEY];
  for (const n of candidates) keys.push(storageKeys.networksAdded(n), storageKeys.taoInEmission(n), storageKeys.excessTao(n));
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
    const theory = calculateTheoreticalInjectedRao({ blockNumber, netuid, actualRao: actual, chainBuyRao: chainBuy });
    payload[String(netuid)] = [actual.toString(), chainBuy.toString(), theory?.toString() ?? null];
  }
  return { timestampMs, payload, active };
}

async function syncRegistry(env: Env, rpc: SubtensorRpc, hash: string, blockNumber: number, candidates: number[], active: number[]): Promise<{ added: number; removed: number }> {
  const names = await readSubnetNames(rpc, hash);
  const counterKeys = active.map(storageKeys.registeredSubnetCounter);
  const counters = await rpc.queryStorage(counterKeys, hash);
  const now = Date.now();
  const previous = await env.META_DB.prepare('SELECT netuid,status,first_seen_block,name FROM subnets').all<{netuid:number;status:string;first_seen_block:number;name:string}>();
  const prevMap = new Map(previous.results.map(r => [Number(r.netuid), r]));
  const activeSet = new Set(active);
  let added = 0, removed = 0;
  const rows: SubnetRecord[] = [];

  for (const netuid of candidates) {
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
    // Safety: never make a single free-plan invocation do an unbounded catch-up.
    start = Math.max(start, finalizedBlock - Math.max(1, maxBlocks) + 1);

    const known = await listKnownNetuids(env.META_DB);
    const candidates = await discoverCandidateNetuids(rpc, finalizedHash, known);
    let stored = 0;
    const newBlocks: Array<{timestampMs:number;payload:BlockPayload}> = [];
    let lastActive: number[] = [];

    for (let block = start; block <= finalizedBlock; block++) {
      const hash = block === finalizedBlock ? finalizedHash : await rpc.blockHash(block);
      if (!hash) throw new Error(`Missing block hash for ${block}`);
      const state = await readBlockState(rpc, hash, block, candidates);
      lastActive = state.active;
      if (await storeBlock(env, block, hash, state.timestampMs, state.payload)) {
        stored++;
        newBlocks.push({timestampMs:state.timestampMs,payload:state.payload});
      }
      await setState(env.META_DB, 'last_finalized_block', String(block));
      await setState(env.META_DB, 'last_sync_ms', String(Date.now()));
    }

    await updateSummariesForNewBlocks(env.META_DB, newBlocks);
    const registry = await syncRegistry(env, rpc, finalizedHash, finalizedBlock, candidates, lastActive);
    await setState(env.META_DB, 'rpc_status', 'ok');
    await setState(env.META_DB, 'chain_finalized_block', String(finalizedBlock));
    return { finalizedBlock, scanned: Math.max(0, finalizedBlock - start + 1), stored, activeSubnets: lastActive.length, addedSubnets: registry.added, removedSubnets: registry.removed };
  } catch (error) {
    await setState(env.META_DB, 'rpc_status', 'error');
    await setState(env.META_DB, 'last_error', error instanceof Error ? error.message : String(error));
    throw error;
  } finally {
    rpc.close();
  }
}
