import type { BlockPayload, Env, SubnetRecord } from './types';
import { SubtensorRpc, parseBlockNumber } from './rpc';
import {
  PREFIX,
  TIMESTAMP_NOW_KEY,
  decodeBool,
  decodeFirstVecUtf8,
  decodeU64,
  netuidFromBlake2ConcatStorageKey,
  netuidFromIdentityStorageKey,
  storageKeys
} from './storage';
import {
  ensureClosedHourlySummaries,
  getState,
  listKnownNetuids,
  storeBlock,
  upsertSubnets,
  writeSyncStates
} from './db';

const DEFAULT_WS = 'wss://entrypoint-finney.opentensor.ai:443';
const ROOT_NETUID = 0;

async function discoverCandidateNetuids(rpc: SubtensorRpc, hash: string, known: number[]): Promise<number[]> {
  const keys = await rpc.keysPaged(PREFIX.networksAdded, hash);
  const found = keys.map(netuidFromIdentityStorageKey).filter((n): n is number => n != null && n !== ROOT_NETUID);
  return [...new Set([...known.filter(n=>n!==ROOT_NETUID),...found])].sort((a,b)=>a-b);
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
    if (name) names.set(netuid,name);
  }
  return names;
}

async function readActiveNetuids(rpc: SubtensorRpc, hash: string, candidates: number[]): Promise<number[]> {
  const state = await rpc.queryStorage(candidates.map(storageKeys.networksAdded),hash);
  return candidates.filter(netuid=>decodeBool(state.get(storageKeys.networksAdded(netuid)) ?? null));
}

async function readBlockState(rpc: SubtensorRpc, hash: string, blockNumber: number, candidates: number[]): Promise<{timestampMs:number;payload:BlockPayload;active:number[]}> {
  const keys:string[]=[TIMESTAMP_NOW_KEY];
  for(const netuid of candidates) keys.push(storageKeys.networksAdded(netuid),storageKeys.taoInEmission(netuid),storageKeys.excessTao(netuid));
  const state=await rpc.queryStorage(keys,hash);
  const timestampMs=Number(decodeU64(state.get(TIMESTAMP_NOW_KEY) ?? null));
  if(!Number.isSafeInteger(timestampMs)||timestampMs<=0) throw new Error(`Invalid Timestamp.Now at block ${blockNumber}`);
  const payload:BlockPayload={};
  const active:number[]=[];
  for(const netuid of candidates){
    if(!decodeBool(state.get(storageKeys.networksAdded(netuid)) ?? null))continue;
    active.push(netuid);
    const actual=decodeU64(state.get(storageKeys.taoInEmission(netuid)) ?? null);
    const chainBuy=decodeU64(state.get(storageKeys.excessTao(netuid)) ?? null);
    payload[String(netuid)]=[actual.toString(),chainBuy.toString()];
  }
  return {timestampMs,payload,active};
}

async function syncRegistry(env:Env,rpc:SubtensorRpc,hash:string,blockNumber:number,candidates:number[],active:number[]){
  const names=await readSubnetNames(rpc,hash);
  const safeActive=active.filter(n=>n!==ROOT_NETUID);
  const counters=await rpc.queryStorage(safeActive.map(storageKeys.registeredSubnetCounter),hash);
  const previous=await env.META_DB.prepare('SELECT netuid,status,first_seen_block,name FROM subnets WHERE netuid > 0').all<{netuid:number;status:string;first_seen_block:number;name:string}>();
  const prevMap=new Map(previous.results.map(r=>[Number(r.netuid),r]));
  const activeSet=new Set(safeActive);
  const rows:SubnetRecord[]=[];let added=0,removed=0;const now=Date.now();
  for(const netuid of candidates){
    if(netuid===ROOT_NETUID)continue;
    const prev=prevMap.get(netuid),isActive=activeSet.has(netuid);
    if(isActive&&(!prev||prev.status!=='active'))added++;
    if(!isActive&&prev?.status==='active')removed++;
    if(!prev&&!isActive)continue;
    rows.push({
      netuid,
      registration_counter:isActive?decodeU64(counters.get(storageKeys.registeredSubnetCounter(netuid)) ?? null).toString():null,
      name:names.get(netuid)??prev?.name??`Subnet ${netuid}`,
      status:isActive?'active':'deregistered',
      first_seen_block:prev?.first_seen_block??blockNumber,
      last_seen_block:blockNumber,
      updated_at_ms:now
    });
  }
  await upsertSubnets(env.META_DB,rows);
  await env.META_DB.prepare('DELETE FROM subnets WHERE netuid = 0').run();
  return {added,removed};
}

export interface ScanResult { finalizedBlock:number; scanned:number; stored:number; activeSubnets:number; addedSubnets:number; removedSubnets:number; }

/**
 * Observed-data-only collector.
 * Deep gaps are intentionally skipped. On first boot we start at the current
 * finalized block; afterwards we only catch the most recent `maxBlocks` so a
 * short outage does not lose blocks, while old backlog is never chased.
 */
export async function scanToFinalized(env: Env, maxBlocks = 8): Promise<ScanResult> {
  const rpc=new SubtensorRpc(env.SUBTENSOR_WS_URL||DEFAULT_WS);
  try{
    await rpc.connect();
    const finalizedHash=await rpc.finalizedHead();
    const finalizedHeader=await rpc.header(finalizedHash);
    const finalizedBlock=parseBlockNumber(finalizedHeader.number);
    const saved=Number(await getState(env.META_DB,'last_finalized_block') ?? '0');
    let start=saved>0?saved+1:finalizedBlock;
    start=Math.max(start,finalizedBlock-Math.max(1,Math.trunc(maxBlocks))+1);

    const known=await listKnownNetuids(env.META_DB);
    const candidates=await discoverCandidateNetuids(rpc,finalizedHash,known);
    const latestActive=await readActiveNetuids(rpc,finalizedHash,candidates);
    const registry=await syncRegistry(env,rpc,finalizedHash,finalizedBlock,candidates,latestActive);

    let stored=0,lastTimestampMs=0;
    for(let block=start;block<=finalizedBlock;block++){
      const hash=block===finalizedBlock?finalizedHash:await rpc.blockHash(block);
      if(!hash)continue;
      const state=await readBlockState(rpc,hash,block,candidates);
      if(await storeBlock(env,block,hash,state.timestampMs,state.payload))stored++;
      lastTimestampMs=state.timestampMs;
    }
    if(lastTimestampMs>0)await ensureClosedHourlySummaries(env,lastTimestampMs);

    await writeSyncStates(env.META_DB,{
      last_finalized_block:String(finalizedBlock),
      chain_finalized_block:String(finalizedBlock),
      last_sync_ms:String(Date.now()),
      rpc_status:'ok',
      registry_added_last:String(registry.added),
      registry_removed_last:String(registry.removed),
      last_registry_sync_block:String(finalizedBlock)
    });
    await env.META_DB.prepare("DELETE FROM sync_state WHERE key='last_error'").run();
    return {finalizedBlock,scanned:Math.max(0,finalizedBlock-start+1),stored,activeSubnets:latestActive.length,addedSubnets:registry.added,removedSubnets:registry.removed};
  }catch(error){
    await writeSyncStates(env.META_DB,{rpc_status:'error',last_error:error instanceof Error?error.message:String(error)});
    throw error;
  }finally{rpc.close();}
}
