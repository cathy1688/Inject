import type { Env, SummaryPayload } from './types';
import { addBlockToSummary, blockDatabases, DAY_MS, HOUR_MS, mergeSummaries } from './db';
import { getState } from './db';
import { scanToFinalized } from './scanner';
import { THEORY_MODEL_VERSION } from './theory';

const RAO_PER_TAO = 1_000_000_000;

function json(data: unknown, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  headers.set('content-type', 'application/json; charset=utf-8');
  headers.set('cache-control', 'no-store');
  return new Response(JSON.stringify(data), { ...init, headers });
}
function bad(message: string, status = 400): Response { return json({ error: message }, { status }); }
function parseTime(value: string | null, fallback: number): number {
  if (!value) return fallback;
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric > 1_000_000_000_000) return Math.trunc(numeric);
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}
function num(v: unknown): number { const n = Number(v ?? 0); return Number.isFinite(n) ? n : 0; }
function optionalNum(v: unknown): number | null { if (v == null) return null; const n=Number(v); return Number.isFinite(n) ? n : null; }
function ceilTo(value:number, unit:number):number { return Math.ceil(value/unit)*unit; }
function floorTo(value:number, unit:number):number { return Math.floor(value/unit)*unit; }

interface TheoryValidity {
  versionMatches: boolean;
  complete: boolean;
  cursor: number;
  liveStart: number;
}

async function theoryValidity(env: Env): Promise<TheoryValidity> {
  const [version,status,cursor,liveStart] = await Promise.all([
    getState(env.META_DB,'theory_formula_version'),
    getState(env.META_DB,'theory_backfill_status'),
    getState(env.META_DB,'theory_backfill_cursor'),
    getState(env.META_DB,'theory_live_start_block')
  ]);
  const versionMatches = version === THEORY_MODEL_VERSION;
  return {
    versionMatches,
    complete: versionMatches && status === 'complete',
    cursor: versionMatches ? Number(cursor ?? 0) : 0,
    liveStart: versionMatches ? Number(liveStart ?? 0) : 0
  };
}

function theoryValidForBlock(blockNumber: number, validity: TheoryValidity): boolean {
  if (!validity.versionMatches) return false;
  if (validity.complete) return true;
  if (blockNumber <= validity.cursor) return true;
  return validity.liveStart > 0 && blockNumber >= validity.liveStart;
}

async function status(env: Env): Promise<Response> {
  const db=env.META_DB;
  const keys = [
    'last_finalized_block','chain_finalized_block','last_sync_ms','rpc_status','last_error',
    'registry_added_last','registry_removed_last','theory_status','theory_last_error','theory_last_block','theory_last_enriched',
    'theory_last_missing_root','theory_last_missing_alpha','theory_last_missing_price',
    'theory_last_equal_actual','theory_last_different_actual','theory_formula_version','theory_live_start_block',
    'theory_backfill_status','theory_backfill_cursor','theory_backfill_last_processed','theory_backfill_error'
  ];
  const values = await Promise.all(keys.map(key => getState(db,key)));
  const [
    last,chain,syncMs,rpcStatus,error,added,removed,theoryStatus,theoryLastError,theoryBlock,theoryEnriched,
    missingRoot,missingAlpha,missingPrice,equalActual,differentActual,formulaVersion,liveStart,
    backfillStatus,backfillCursor,backfillProcessed,backfillError
  ]=values;
  const active = await db.prepare("SELECT COUNT(*) AS n FROM subnets WHERE status='active' AND netuid > 0").first<{n:number}>();
  const versionMatches=formulaVersion===THEORY_MODEL_VERSION;
  return json({
    rpcStatus: rpcStatus ?? 'unknown',
    lastFinalizedBlock: Number(last ?? 0),
    chainFinalizedBlock: Number(chain ?? last ?? 0),
    lastSyncMs: Number(syncMs ?? 0),
    activeSubnets: Number(active?.n ?? 0),
    addedLastSync: Number(added ?? 0),
    removedLastSync: Number(removed ?? 0),
    lastError: error ?? null,
    theoryEnabled: true,
    theoryMode: 'independent-protocol-reconstruction',
    theoryModelVersion: THEORY_MODEL_VERSION,
    theoryFormulaVersion: formulaVersion ?? null,
    theoryVersionMatches: versionMatches,
    theoryStatus: theoryStatus ?? 'waiting',
    theoryLastError: theoryLastError ?? null,
    theoryLastBlock: Number(theoryBlock ?? 0),
    theoryLastEnriched: Number(theoryEnriched ?? 0),
    theoryMissingRoot: Number(missingRoot ?? 0),
    theoryMissingAlpha: Number(missingAlpha ?? 0),
    theoryMissingPrice: Number(missingPrice ?? 0),
    theoryLastEqualActual: Number(equalActual ?? 0),
    theoryLastDifferentActual: Number(differentActual ?? 0),
    theoryLiveStartBlock: Number(liveStart ?? 0),
    theoryBackfillStatus: versionMatches ? (backfillStatus ?? 'waiting') : 'waiting-for-v7',
    theoryBackfillCursor: versionMatches ? Number(backfillCursor ?? 0) : 0,
    theoryBackfillLastProcessed: versionMatches ? Number(backfillProcessed ?? 0) : 0,
    theoryBackfillError: backfillError ?? null,
    theoryHistoryReady: versionMatches && backfillStatus === 'complete',
    retentionDays: Number(env.RETENTION_DAYS ?? '30')
  });
}

async function subnets(env: Env): Promise<Response> {
  const result = await env.META_DB.prepare('SELECT netuid,registration_counter,name,status,first_seen_block,last_seen_block,updated_at_ms FROM subnets WHERE netuid > 0 ORDER BY netuid').all();
  return json({ items: result.results });
}

async function mergeHourlyRows(env: Env, startInclusive:number, endExclusive:number, target:SummaryPayload):Promise<void>{
  if(endExclusive<=startInclusive) return;
  const result=await env.META_DB.prepare(`SELECT payload FROM hourly_summary WHERE period_start_ms>=? AND period_start_ms<? ORDER BY period_start_ms`)
    .bind(startInclusive,endExclusive).all<{payload:string}>();
  for(const row of result.results) mergeSummaries(target,JSON.parse(row.payload) as SummaryPayload);
}
async function mergeRawBlocks(env:Env,startInclusive:number,endExclusive:number,target:SummaryPayload):Promise<void>{
  if(endExclusive<=startInclusive) return;
  for(const db of blockDatabases(env)){
    const result=await db.prepare('SELECT payload FROM blocks WHERE timestamp_ms>=? AND timestamp_ms<? ORDER BY timestamp_ms')
      .bind(startInclusive,endExclusive).all<{payload:string}>();
    for(const row of result.results) addBlockToSummary(target,JSON.parse(row.payload));
  }
}
async function aggregateRange(env:Env,from:number,to:number):Promise<SummaryPayload>{
  const target:SummaryPayload={}; const end=to+1; if(end<=from) return target;
  const fullStart=Math.min(end,ceilTo(from,HOUR_MS)); const fullEnd=Math.max(fullStart,floorTo(end,HOUR_MS));
  await mergeRawBlocks(env,from,fullStart,target); await mergeHourlyRows(env,fullStart,fullEnd,target); await mergeRawBlocks(env,fullEnd,end,target); return target;
}

async function subnetsSummary(req: Request, env: Env): Promise<Response> {
  const url = new URL(req.url); const now = Date.now();
  const from = parseTime(url.searchParams.get('from'), now - DAY_MS); const to = parseTime(url.searchParams.get('to'), now);
  if (from > to) return bad('from must be <= to');
  const [summary,validity]=await Promise.all([aggregateRange(env,from,to),theoryValidity(env)]);
  const subnetRows = await env.META_DB.prepare('SELECT netuid,name,status FROM subnets WHERE netuid > 0 ORDER BY netuid').all<{netuid:number;name:string;status:string}>();
  const exposeTheory=validity.complete;
  const items=subnetRows.results.map(meta=>{
    const v=summary[String(meta.netuid)] ?? ['0','0',null,0];
    const actual=Number(v[0]), chain=Number(v[1]), theory=exposeTheory&&v[2]!=null?Number(v[2]):null;
    return {netuid:Number(meta.netuid),name:meta.name,status:meta.status,actualRao:actual,theoryRao:theory,chainBuyRao:chain,totalEmissionRao:actual+chain,deviationPct:theory!=null&&theory!==0?(actual-theory)/theory*100:null,blockCount:v[3]};
  });
  return json({from,to,theoryHistoryReady:validity.complete,items});
}

function blockWhere(netuid:number, q:string):{sql:string;params:unknown[]}{
  const key=`$."${netuid}"`; let sql='timestamp_ms BETWEEN ? AND ? AND json_extract(payload, ?) IS NOT NULL'; const params:unknown[]=[key];
  if(q){ const clean=q.replace(/#/g,'').replace(/,/g,'').trim(); sql+=` AND (CAST(block_number AS TEXT) LIKE ? OR strftime('%Y-%m-%d %H:%M:%S', timestamp_ms/1000, 'unixepoch') LIKE ?)`; params.push(`%${clean}%`,`%${clean}%`); }
  return {sql,params};
}
interface Segment { db:D1Database; count:number; min:number; max:number; }
async function blockSegments(env:Env,netuid:number,from:number,to:number,q:string):Promise<Segment[]>{
  const segments:Segment[]=[]; const where=blockWhere(netuid,q);
  for(const db of blockDatabases(env)){
    const row=await db.prepare(`SELECT COUNT(*) AS n, MIN(timestamp_ms) AS min_ts, MAX(timestamp_ms) AS max_ts FROM blocks WHERE ${where.sql}`).bind(from,to,...where.params).first<{n:number;min_ts:number|null;max_ts:number|null}>();
    if(row&&Number(row.n)>0) segments.push({db,count:Number(row.n),min:Number(row.min_ts),max:Number(row.max_ts)});
  }
  segments.sort((a,b)=>a.min-b.min); return segments;
}

async function blocks(req: Request, env: Env): Promise<Response> {
  const url = new URL(req.url); const netuid = Number(url.searchParams.get('netuid'));
  if (!Number.isInteger(netuid)||netuid<=0||netuid>65535) return bad('invalid subnet netuid');
  const now=Date.now(); const from=parseTime(url.searchParams.get('from'),now-DAY_MS); const to=parseTime(url.searchParams.get('to'),now); if(from>to)return bad('from must be <= to');
  const limit=Math.min(5000,Math.max(1,Number(url.searchParams.get('limit')??1000))); const offset=Math.max(0,Number(url.searchParams.get('offset')??0)); const q=(url.searchParams.get('q')??'').slice(0,50);
  const [segments,validity]=await Promise.all([blockSegments(env,netuid,from,to,q),theoryValidity(env)]); const total=segments.reduce((s,x)=>s+x.count,0); const key=`$."${netuid}"`; const where=blockWhere(netuid,q);
  let skip=offset,remaining=limit; const items:Array<Record<string,unknown>>=[];
  for(const segment of segments){
    if(remaining<=0)break; if(skip>=segment.count){skip-=segment.count;continue;} const take=Math.min(remaining,segment.count-skip);
    const result=await segment.db.prepare(`SELECT block_number,block_hash,timestamp_ms,CAST(json_extract(payload, ? || '[0]') AS INTEGER) AS actual_rao,CAST(json_extract(payload, ? || '[1]') AS INTEGER) AS chain_buy_rao,CASE WHEN json_extract(payload, ? || '[2]') IS NULL THEN NULL ELSE CAST(json_extract(payload, ? || '[2]') AS INTEGER) END AS theory_rao FROM blocks WHERE ${where.sql} ORDER BY timestamp_ms ASC, block_number ASC LIMIT ? OFFSET ?`)
      .bind(key,key,key,key,from,to,...where.params,take,skip).all();
    for(const row of result.results as Array<Record<string,unknown>>){
      if(!theoryValidForBlock(Number(row.block_number),validity)) row.theory_rao=null;
      items.push(row);
    }
    remaining-=take; skip=0;
  }
  return json({from,to,netuid,total,offset,limit,theoryHistoryReady:validity.complete,items});
}

async function chart(req: Request, env: Env): Promise<Response> {
  const url=new URL(req.url); const netuid=Number(url.searchParams.get('netuid')); if(!Number.isInteger(netuid)||netuid<=0)return bad('invalid subnet netuid');
  const now=Date.now(); const from=parseTime(url.searchParams.get('from'),now-DAY_MS),to=parseTime(url.searchParams.get('to'),now); const key=`$."${netuid}"`;
  const [result,validity]=await Promise.all([
    env.META_DB.prepare(`SELECT period_start_ms,CAST(json_extract(payload, ? || '[0]') AS INTEGER) AS actual_rao,CAST(json_extract(payload, ? || '[1]') AS INTEGER) AS chain_buy_rao,CASE WHEN json_extract(payload, ? || '[2]') IS NULL THEN NULL ELSE CAST(json_extract(payload, ? || '[2]') AS INTEGER) END AS theory_rao,CAST(json_extract(payload, ? || '[3]') AS INTEGER) AS subnet_block_count FROM hourly_summary WHERE period_start_ms BETWEEN ? AND ? AND json_extract(payload, ?) IS NOT NULL ORDER BY period_start_ms ASC`).bind(key,key,key,key,key,from,to,key).all(),
    theoryValidity(env)
  ]);
  const items=(result.results as Array<Record<string,unknown>>).map(row=>validity.complete?row:{...row,theory_rao:null});
  return json({from,to,intervalMs:HOUR_MS,theoryHistoryReady:validity.complete,items});
}

async function exportCsv(req: Request, env: Env): Promise<Response> {
  const url=new URL(req.url); const netuid=Number(url.searchParams.get('netuid')); if(!Number.isInteger(netuid)||netuid<=0)return bad('invalid subnet netuid');
  const now=Date.now(),from=parseTime(url.searchParams.get('from'),now-DAY_MS),to=parseTime(url.searchParams.get('to'),now); const fakeUrl=new URL(req.url); fakeUrl.pathname='/api/blocks'; fakeUrl.searchParams.set('limit','5000'); fakeUrl.searchParams.set('offset','0');
  const all:Array<Record<string,unknown>>=[]; let offset=0,total=Infinity;
  while(offset<total){ fakeUrl.searchParams.set('offset',String(offset)); const response=await blocks(new Request(fakeUrl.toString()),env); const payload=await response.json() as {items:Array<Record<string,unknown>>;total:number}; all.push(...payload.items); total=payload.total; offset+=payload.items.length; if(!payload.items.length)break; }
  const lines=['区块高度,区块时间,链上实际注入TAO,理论计算注入TAO,Chain Buys TAO,Total Emission TAO,偏差%'];
  for(const r of all){ const a=num(r.actual_rao),c=num(r.chain_buy_rao),t=optionalNum(r.theory_rao); const d=t&&t!==0?(a-t)/t*100:null; lines.push([r.block_number,new Date(num(r.timestamp_ms)).toISOString(),(a/RAO_PER_TAO).toFixed(9),t==null?'':(t/RAO_PER_TAO).toFixed(9),(c/RAO_PER_TAO).toFixed(9),((a+c)/RAO_PER_TAO).toFixed(9),d==null?'':d.toFixed(6)].join(',')); }
  return new Response('\ufeff'+lines.join('\n'),{headers:{'content-type':'text/csv; charset=utf-8','content-disposition':`attachment; filename="SN${netuid}_emission.csv"`}});
}

export async function handleApi(req: Request, env: Env): Promise<Response> {
  const url=new URL(req.url);
  try {
    if(url.pathname==='/api/status') return status(env);
    if(url.pathname==='/api/subnets') return subnets(env);
    if(url.pathname==='/api/subnets/summary') return subnetsSummary(req,env);
    if(url.pathname==='/api/blocks') return blocks(req,env);
    if(url.pathname==='/api/chart') return chart(req,env);
    if(url.pathname==='/api/export.csv') return exportCsv(req,env);
    if(url.pathname==='/api/sync'&&req.method==='POST'){
      const max=Math.min(24,Math.max(1,Number(url.searchParams.get('max')??24)));
      return json(await scanToFinalized(env,max));
    }
    return bad('not found',404);
  } catch(error){ return json({error:error instanceof Error?error.message:String(error)},{status:500}); }
}
