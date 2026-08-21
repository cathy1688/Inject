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
  listKnownNetuids,
  storeBlock,
  upsertSubnets,
  writeSyncStates
} from './db';

const DEFAULT_WS = 'wss://entrypoint-finney.opentensor.ai:443';
const ROOT_NETUID = 0;
const REGISTRY_REFRESH_INTERVAL_BLOCKS = 25; // ~5 minutes.
const ZERO_STREAK_TO_PAUSE = 3;
const UTC8_OFFSET_MS = 8 * 60 * 60 * 1000;

interface EmissionValue {
  actual: bigint;
  chainBuy: bigint;
}

function utc8DayKey(now = Date.now()): string {
  return new Date(now + UTC8_OFFSET_MS).toISOString().slice(0,10);
}

function isUtc8MidnightHour(now = Date.now()): boolean {
  return new Date(now + UTC8_OFFSET_MS).getUTCHours() === 0;
}

function parseNetuidList(raw: string | undefined): number[] {
  if (!raw) return [];
  try {
    const value = JSON.parse(raw);
    if (!Array.isArray(value)) return [];
    return value
      .map(Number)
      .filter(n => Number.isInteger(n) && n > 0 && n <= 65535);
  } catch {
    return [];
  }
}

function parseZeroStreaks(raw: string | undefined): Record<string, number> {
  if (!raw) return {};
  try {
    const value = JSON.parse(raw) as Record<string, unknown>;
    const out: Record<string, number> = {};
    for (const [key, item] of Object.entries(value ?? {})) {
      const n = Number(item);
      if (/^\d+$/.test(key) && Number.isFinite(n) && n >= 0) out[key] = Math.trunc(n);
    }
    return out;
  } catch {
    return {};
  }
}

async function readScannerState(env: Env): Promise<Map<string,string>> {
  const rows = await env.META_DB.prepare(`
    SELECT key,value FROM sync_state
    WHERE key IN (
      'last_finalized_block','last_registry_sync_block','emitting_netuids',
      'emission_probe_day','emission_probe_block','emission_zero_streaks'
    )
  `).all<{key:string;value:string}>();
  return new Map(rows.results.map(row => [String(row.key), String(row.value)]));
}

async function readDbActiveNetuids(env: Env): Promise<number[]> {
  const rows = await env.META_DB.prepare("SELECT netuid FROM subnets WHERE status='active' AND netuid > 0 ORDER BY netuid")
    .all<{netuid:number}>();
  return rows.results.map(row => Number(row.netuid)).filter(n => Number.isInteger(n) && n > 0);
}

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
  if (!candidates.length) return [];
  const state = await rpc.queryStorage(candidates.map(storageKeys.networksAdded),hash);
  return candidates.filter(netuid=>decodeBool(state.get(storageKeys.networksAdded(netuid)) ?? null));
}

async function readEmissionValues(rpc: SubtensorRpc, hash: string, netuids: number[]): Promise<Map<number,EmissionValue>> {
  const values = new Map<number,EmissionValue>();
  if (!netuids.length) return values;
  const keys:string[]=[];
  for (const netuid of netuids) keys.push(storageKeys.taoInEmission(netuid),storageKeys.excessTao(netuid));
  const state = await rpc.queryStorage(keys,hash);
  for (const netuid of netuids) {
    values.set(netuid,{
      actual: decodeU64(state.get(storageKeys.taoInEmission(netuid)) ?? null),
      chainBuy: decodeU64(state.get(storageKeys.excessTao(netuid)) ?? null)
    });
  }
  return values;
}

async function readBlockState(
  rpc: SubtensorRpc,
  hash: string,
  blockNumber: number,
  monitored: number[]
): Promise<{timestampMs:number;payload:BlockPayload;values:Map<number,EmissionValue>}> {
  const keys:string[]=[TIMESTAMP_NOW_KEY];
  for(const netuid of monitored) keys.push(storageKeys.taoInEmission(netuid),storageKeys.excessTao(netuid));
  const state=await rpc.queryStorage(keys,hash);
  const timestampMs=Number(decodeU64(state.get(TIMESTAMP_NOW_KEY) ?? null));
  if(!Number.isSafeInteger(timestampMs)||timestampMs<=0) throw new Error(`Invalid Timestamp.Now at block ${blockNumber}`);

  const payload:BlockPayload={};
  const values=new Map<number,EmissionValue>();
  for(const netuid of monitored){
    const actual=decodeU64(state.get(storageKeys.taoInEmission(netuid)) ?? null);
    const chainBuy=decodeU64(state.get(storageKeys.excessTao(netuid)) ?? null);
    values.set(netuid,{actual,chainBuy});
    if(actual<=0n && chainBuy<=0n) continue;
    payload[String(netuid)]=[actual.toString(),chainBuy.toString()];
  }
  return {timestampMs,payload,values};
}

async function syncRegistry(env:Env,rpc:SubtensorRpc,hash:string,blockNumber:number,candidates:number[],active:number[]){
  const names=await readSubnetNames(rpc,hash);
  const safeActive=active.filter(n=>n!==ROOT_NETUID);
  const counters=safeActive.length
    ? await rpc.queryStorage(safeActive.map(storageKeys.registeredSubnetCounter),hash)
    : new Map<string,string|null>();
  const previous=await env.META_DB.prepare('SELECT netuid,status,first_seen_block,name FROM subnets WHERE netuid > 0').all<{netuid:number;status:string;first_seen_block:number;name:string}>();
  const prevMap=new Map(previous.results.map(r=>[Number(r.netuid),r]));
  const activeSet=new Set(safeActive);
  const rows:SubnetRecord[]=[];
  const newlyActive:number[]=[];
  let added=0,removed=0;
  const now=Date.now();
  for(const netuid of candidates){
    if(netuid===ROOT_NETUID)continue;
    const prev=prevMap.get(netuid),isActive=activeSet.has(netuid);
    if(isActive&&(!prev||prev.status!=='active')){added++;newlyActive.push(netuid);}
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
  return {added,removed,newlyActive};
}

export interface ScanResult {
  finalizedBlock:number;
  scanned:number;
  stored:number;
  activeSubnets:number;
  emittingSubnets:number;
  pausedZeroEmissionSubnets:number;
  emissionProbeDay:string | null;
  addedSubnets:number;
  removedSubnets:number;
}

/**
 * Emission-aware observed-data collector.
 *
 * - Subnets currently emitting TAO/Chain Buy stay on the per-block fast path.
 * - A subnet is paused after 3 consecutive zero-emission blocks.
 * - Paused subnets are probed once per day during the 00:00 hour in UTC+8.
 * - Newly registered/reenabled subnets are checked immediately once.
 * - Registry discovery/names are refreshed every 25 blocks (~5 minutes).
 * - Deep gaps are intentionally skipped; only recent maxBlocks are considered.
 */
export async function scanToFinalized(env: Env, maxBlocks = 8): Promise<ScanResult> {
  const rpc=new SubtensorRpc(env.SUBTENSOR_WS_URL||DEFAULT_WS);
  try{
    await rpc.connect();
    const finalizedHash=await rpc.finalizedHead();
    const finalizedHeader=await rpc.header(finalizedHash);
    const finalizedBlock=parseBlockNumber(finalizedHeader.number);

    const scannerState=await readScannerState(env);
    const saved=Number(scannerState.get('last_finalized_block') ?? '0');
    const lastRegistryBlock=Number(scannerState.get('last_registry_sync_block') ?? '0');
    const hadEmissionState=scannerState.has('emitting_netuids');
    const zeroStreaks=parseZeroStreaks(scannerState.get('emission_zero_streaks'));
    let probeDay=scannerState.get('emission_probe_day') ?? '';

    let start=saved>0?saved+1:finalizedBlock;
    start=Math.max(start,finalizedBlock-Math.max(1,Math.trunc(maxBlocks))+1);

    const known=await listKnownNetuids(env.META_DB);
    const registryDue=known.length===0 || lastRegistryBlock===0 || finalizedBlock-lastRegistryBlock>=REGISTRY_REFRESH_INTERVAL_BLOCKS;
    let latestActive:number[];
    let registry={added:0,removed:0,newlyActive:[] as number[]};
    let registrySyncBlock=lastRegistryBlock;

    if(registryDue){
      const candidates=await discoverCandidateNetuids(rpc,finalizedHash,known);
      latestActive=await readActiveNetuids(rpc,finalizedHash,candidates);
      registry=await syncRegistry(env,rpc,finalizedHash,finalizedBlock,candidates,latestActive);
      registrySyncBlock=finalizedBlock;
    }else{
      latestActive=await readDbActiveNetuids(env);
    }

    const activeSet=new Set(latestActive);
    const monitored=new Set(parseNetuidList(scannerState.get('emitting_netuids')).filter(n=>activeSet.has(n)));
    for(const key of Object.keys(zeroStreaks)) if(!monitored.has(Number(key))) delete zeroStreaks[key];

    const today=utc8DayKey();
    const dailyProbeDue=isUtc8MidnightHour() && probeDay!==today;
    const probeTargets=new Set<number>();

    // First initialization needs one full check to discover the emitting set.
    if(!hadEmissionState){
      for(const netuid of latestActive) if(!monitored.has(netuid)) probeTargets.add(netuid);
    }

    // Newly registered/reenabled subnets are checked immediately once.
    for(const netuid of registry.newlyActive) if(!monitored.has(netuid)) probeTargets.add(netuid);

    // Existing paused subnets are otherwise checked only once at UTC+8 midnight.
    if(dailyProbeDue){
      for(const netuid of latestActive) if(!monitored.has(netuid)) probeTargets.add(netuid);
    }

    if(probeTargets.size>0){
      const targets=[...probeTargets].sort((a,b)=>a-b);
      const probe=await readEmissionValues(rpc,finalizedHash,targets);
      for(const netuid of targets){
        const value=probe.get(netuid);
        if(value && (value.actual>0n || value.chainBuy>0n)){
          monitored.add(netuid);
          zeroStreaks[String(netuid)]=0;
        }
      }
    }
    if(dailyProbeDue) probeDay=today;

    let stored=0,lastTimestampMs=0;
    for(let block=start;block<=finalizedBlock;block++){
      const hash=block===finalizedBlock?finalizedHash:await rpc.blockHash(block);
      if(!hash)continue;
      const watched=[...monitored].sort((a,b)=>a-b);
      const state=await readBlockState(rpc,hash,block,watched);
      for(const netuid of watched){
        const value=state.values.get(netuid);
        if(value && (value.actual>0n || value.chainBuy>0n)) zeroStreaks[String(netuid)]=0;
        else zeroStreaks[String(netuid)]=(zeroStreaks[String(netuid)] ?? 0)+1;
      }
      if(Object.keys(state.payload).length>0 && await storeBlock(env,block,hash,state.timestampMs,state.payload)) stored++;
      lastTimestampMs=state.timestampMs;
    }

    for(const netuid of [...monitored]){
      if((zeroStreaks[String(netuid)] ?? 0)>=ZERO_STREAK_TO_PAUSE){
        monitored.delete(netuid);
        delete zeroStreaks[String(netuid)];
      }
    }

    if(lastTimestampMs>0)await ensureClosedHourlySummaries(env,lastTimestampMs);

    const emitting=[...monitored].filter(n=>activeSet.has(n)).sort((a,b)=>a-b);
    const cleanStreaks:Record<string,number>={};
    for(const netuid of emitting){
      const streak=zeroStreaks[String(netuid)] ?? 0;
      if(streak>0) cleanStreaks[String(netuid)]=streak;
    }
    const pausedCount=Math.max(0,latestActive.length-emitting.length);

    await writeSyncStates(env.META_DB,{
      last_finalized_block:String(finalizedBlock),
      chain_finalized_block:String(finalizedBlock),
      last_sync_ms:String(Date.now()),
      rpc_status:'ok',
      registry_added_last:String(registry.added),
      registry_removed_last:String(registry.removed),
      last_registry_sync_block:String(registrySyncBlock),
      emitting_netuids:JSON.stringify(emitting),
      emitting_subnet_count:String(emitting.length),
      zero_emission_subnet_count:String(pausedCount),
      emission_probe_day:probeDay,
      emission_probe_schedule:'00:00 UTC+8 daily',
      emission_zero_streaks:JSON.stringify(cleanStreaks),
      emission_zero_pause_blocks:String(ZERO_STREAK_TO_PAUSE),
      scanner_mode:'emission-aware-daily-probe'
    });
    if(scannerState.has('emission_probe_block')){
      await env.META_DB.prepare("DELETE FROM sync_state WHERE key IN ('emission_probe_block','emission_probe_interval_blocks')").run();
    }
    await env.META_DB.prepare("DELETE FROM sync_state WHERE key='last_error'").run();
    return {
      finalizedBlock,
      scanned:Math.max(0,finalizedBlock-start+1),
      stored,
      activeSubnets:latestActive.length,
      emittingSubnets:emitting.length,
      pausedZeroEmissionSubnets:pausedCount,
      emissionProbeDay:probeDay||null,
      addedSubnets:registry.added,
      removedSubnets:registry.removed
    };
  }catch(error){
    await writeSyncStates(env.META_DB,{rpc_status:'error',last_error:error instanceof Error?error.message:String(error)});
    throw error;
  }finally{rpc.close();}
}
