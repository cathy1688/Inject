import type { Env, SummaryPayload } from './types';
import { addBlockToSummary, blockDatabases, DAY_MS, HOUR_MS, mergeSummaries, getState } from './db';
import { scanToFinalized } from './scanner';

const RAO_PER_TAO=1_000_000_000;
function json(data:unknown,init:ResponseInit={}):Response{const h=new Headers(init.headers);h.set('content-type','application/json; charset=utf-8');h.set('cache-control','no-store');return new Response(JSON.stringify(data),{...init,headers:h});}
function bad(message:string,status=400):Response{return json({error:message},{status});}
function parseTime(value:string|null,fallback:number):number{if(!value)return fallback;const n=Number(value);if(Number.isFinite(n)&&n>1_000_000_000_000)return Math.trunc(n);const p=Date.parse(value);return Number.isFinite(p)?p:fallback;}
function num(v:unknown):number{const n=Number(v??0);return Number.isFinite(n)?n:0;}
function ceilTo(v:number,u:number){return Math.ceil(v/u)*u;} function floorTo(v:number,u:number){return Math.floor(v/u)*u;}

async function status(env:Env):Promise<Response>{
  const keys=['last_finalized_block','chain_finalized_block','last_sync_ms','rpc_status','last_error','registry_added_last','registry_removed_last'];
  const [last,chain,syncMs,rpcStatus,error,added,removed]=await Promise.all(keys.map(k=>getState(env.META_DB,k)));
  const active=await env.META_DB.prepare("SELECT COUNT(*) AS n FROM subnets WHERE status='active' AND netuid > 0").first<{n:number}>();
  return json({rpcStatus:rpcStatus??'unknown',lastFinalizedBlock:Number(last??0),chainFinalizedBlock:Number(chain??last??0),lastSyncMs:Number(syncMs??0),activeSubnets:Number(active?.n??0),addedLastSync:Number(added??0),removedLastSync:Number(removed??0),lastError:error??null,retentionDays:Number(env.RETENTION_DAYS??'30')});
}

async function subnets(env:Env):Promise<Response>{const r=await env.META_DB.prepare('SELECT netuid,registration_counter,name,status,first_seen_block,last_seen_block,updated_at_ms FROM subnets WHERE netuid > 0 ORDER BY netuid').all();return json({items:r.results});}

async function mergeHourlyRows(env:Env,start:number,end:number,target:SummaryPayload){if(end<=start)return;const r=await env.META_DB.prepare('SELECT payload FROM hourly_summary WHERE period_start_ms>=? AND period_start_ms<? ORDER BY period_start_ms').bind(start,end).all<{payload:string}>();for(const row of r.results)mergeSummaries(target,JSON.parse(row.payload) as SummaryPayload);}
async function mergeRawBlocks(env:Env,start:number,end:number,target:SummaryPayload){if(end<=start)return;for(const db of blockDatabases(env)){const r=await db.prepare('SELECT payload FROM blocks WHERE timestamp_ms>=? AND timestamp_ms<? ORDER BY timestamp_ms').bind(start,end).all<{payload:string}>();for(const row of r.results)addBlockToSummary(target,JSON.parse(row.payload));}}
async function aggregateRange(env:Env,from:number,to:number):Promise<SummaryPayload>{const target:SummaryPayload={};const end=to+1;if(end<=from)return target;const fullStart=Math.min(end,ceilTo(from,HOUR_MS)),fullEnd=Math.max(fullStart,floorTo(end,HOUR_MS));await mergeRawBlocks(env,from,fullStart,target);await mergeHourlyRows(env,fullStart,fullEnd,target);await mergeRawBlocks(env,fullEnd,end,target);return target;}

async function subnetsSummary(req:Request,env:Env):Promise<Response>{
  const u=new URL(req.url),now=Date.now(),from=parseTime(u.searchParams.get('from'),now-DAY_MS),to=parseTime(u.searchParams.get('to'),now);if(from>to)return bad('from must be <= to');
  const summary=await aggregateRange(env,from,to);const metas=await env.META_DB.prepare('SELECT netuid,name,status FROM subnets WHERE netuid > 0 ORDER BY netuid').all<{netuid:number;name:string;status:string}>();
  const items=metas.results.map(m=>{const v=summary[String(m.netuid)]??['0','0',0];const actual=Number(v[0]),chain=Number(v[1]);return {netuid:Number(m.netuid),name:m.name,status:m.status,actualRao:actual,chainBuyRao:chain,totalEmissionRao:actual+chain,blockCount:v[2]};});
  return json({from,to,items});
}

function blockWhere(netuid:number,q:string){const key=`$."${netuid}"`;let sql='timestamp_ms BETWEEN ? AND ? AND json_extract(payload, ?) IS NOT NULL';const params:unknown[]=[key];if(q){const clean=q.replace(/#/g,'').replace(/,/g,'').trim();sql+=` AND (CAST(block_number AS TEXT) LIKE ? OR strftime('%Y-%m-%d %H:%M:%S', timestamp_ms/1000, 'unixepoch') LIKE ?)`;params.push(`%${clean}%`,`%${clean}%`);}return {sql,params};}
interface Segment{db:D1Database;count:number;min:number;max:number;}
async function blockSegments(env:Env,netuid:number,from:number,to:number,q:string):Promise<Segment[]>{const out:Segment[]=[];const where=blockWhere(netuid,q);for(const db of blockDatabases(env)){const row=await db.prepare(`SELECT COUNT(*) AS n,MIN(timestamp_ms) AS min_ts,MAX(timestamp_ms) AS max_ts FROM blocks WHERE ${where.sql}`).bind(from,to,...where.params).first<{n:number;min_ts:number|null;max_ts:number|null}>();if(row&&Number(row.n)>0)out.push({db,count:Number(row.n),min:Number(row.min_ts),max:Number(row.max_ts)});}out.sort((a,b)=>a.min-b.min);return out;}

async function blocks(req:Request,env:Env):Promise<Response>{
  const u=new URL(req.url),netuid=Number(u.searchParams.get('netuid'));if(!Number.isInteger(netuid)||netuid<=0||netuid>65535)return bad('invalid subnet netuid');
  const now=Date.now(),from=parseTime(u.searchParams.get('from'),now-DAY_MS),to=parseTime(u.searchParams.get('to'),now);if(from>to)return bad('from must be <= to');
  const limit=Math.min(5000,Math.max(1,Number(u.searchParams.get('limit')??1000))),offset=Math.max(0,Number(u.searchParams.get('offset')??0)),q=(u.searchParams.get('q')??'').slice(0,50);
  const segments=await blockSegments(env,netuid,from,to,q),total=segments.reduce((s,x)=>s+x.count,0),key=`$."${netuid}"`,where=blockWhere(netuid,q);let skip=offset,remaining=limit;const items:Array<Record<string,unknown>>=[];
  for(const segment of segments){if(remaining<=0)break;if(skip>=segment.count){skip-=segment.count;continue;}const take=Math.min(remaining,segment.count-skip);const r=await segment.db.prepare(`SELECT block_number,block_hash,timestamp_ms,CAST(json_extract(payload, ? || '[0]') AS INTEGER) AS actual_rao,CAST(json_extract(payload, ? || '[1]') AS INTEGER) AS chain_buy_rao FROM blocks WHERE ${where.sql} ORDER BY timestamp_ms ASC,block_number ASC LIMIT ? OFFSET ?`).bind(key,key,from,to,...where.params,take,skip).all();items.push(...r.results as Array<Record<string,unknown>>);remaining-=take;skip=0;}
  return json({from,to,netuid,total,offset,limit,items});
}

async function chart(req:Request,env:Env):Promise<Response>{
  const u=new URL(req.url),netuid=Number(u.searchParams.get('netuid'));if(!Number.isInteger(netuid)||netuid<=0)return bad('invalid subnet netuid');const now=Date.now(),from=parseTime(u.searchParams.get('from'),now-DAY_MS),to=parseTime(u.searchParams.get('to'),now),key=`$."${netuid}"`;
  const r=await env.META_DB.prepare(`SELECT period_start_ms,CAST(json_extract(payload, ? || '[0]') AS INTEGER) AS actual_rao,CAST(json_extract(payload, ? || '[1]') AS INTEGER) AS chain_buy_rao,CAST(json_extract(payload, ? || '[2]') AS INTEGER) AS subnet_block_count FROM hourly_summary WHERE period_start_ms BETWEEN ? AND ? AND json_extract(payload, ?) IS NOT NULL ORDER BY period_start_ms ASC`).bind(key,key,key,from,to,key).all();return json({from,to,intervalMs:HOUR_MS,items:r.results});
}

async function exportCsv(req:Request,env:Env):Promise<Response>{
  const u=new URL(req.url),netuid=Number(u.searchParams.get('netuid'));if(!Number.isInteger(netuid)||netuid<=0)return bad('invalid subnet netuid');const fake=new URL(req.url);fake.pathname='/api/blocks';fake.searchParams.set('limit','5000');fake.searchParams.set('offset','0');const all:Array<Record<string,unknown>>=[];let offset=0,total=Infinity;while(offset<total){fake.searchParams.set('offset',String(offset));const response=await blocks(new Request(fake.toString()),env);const p=await response.json() as {items:Array<Record<string,unknown>>;total:number};all.push(...p.items);total=p.total;offset+=p.items.length;if(!p.items.length)break;}
  const lines=['区块高度,区块时间,链上实际注入TAO,Chain Buys TAO,Total Emission TAO'];for(const r of all){const a=num(r.actual_rao),c=num(r.chain_buy_rao);lines.push([r.block_number,new Date(num(r.timestamp_ms)).toISOString(),(a/RAO_PER_TAO).toFixed(9),(c/RAO_PER_TAO).toFixed(9),((a+c)/RAO_PER_TAO).toFixed(9)].join(','));}return new Response('\ufeff'+lines.join('\n'),{headers:{'content-type':'text/csv; charset=utf-8','content-disposition':`attachment; filename="SN${netuid}_emission.csv"`}});
}

export async function handleApi(req:Request,env:Env):Promise<Response>{const u=new URL(req.url);try{if(u.pathname==='/api/status')return status(env);if(u.pathname==='/api/subnets')return subnets(env);if(u.pathname==='/api/subnets/summary')return subnetsSummary(req,env);if(u.pathname==='/api/blocks')return blocks(req,env);if(u.pathname==='/api/chart')return chart(req,env);if(u.pathname==='/api/export.csv')return exportCsv(req,env);if(u.pathname==='/api/sync'&&req.method==='POST'){const max=Math.min(8,Math.max(1,Number(u.searchParams.get('max')??8)));return json(await scanToFinalized(env,max));}return bad('not found',404);}catch(error){return json({error:error instanceof Error?error.message:String(error)},{status:500});}}
