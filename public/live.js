const LIVE_REFRESH_MS=12_000;
const LIVE_SYNC_MAX_BLOCKS=8;
let liveBusy=false;
let liveLastFinalized=0;
let liveLastRegistryRefresh=0;
let liveAdjustingRange=false;

function livePreset(){return document.querySelector('.preset.active')?.dataset?.range||'custom';}
function liveRangeMs(mode){if(mode==='24h')return DAY_MS;if(mode==='7d')return 7*DAY_MS;if(mode==='30d')return 30*DAY_MS;return null;}
function liveRollRange(){const span=liveRangeMs(livePreset());if(span==null)return false;const end=new Date(),start=new Date(end.getTime()-span);liveAdjustingRange=true;document.getElementById('startTime').value=toLocalInput(start);document.getElementById('endTime').value=toLocalInput(end);liveAdjustingRange=false;return true;}

function liveApplyStatus(s){const ok=s.rpcStatus==='ok';document.getElementById('activeCount').textContent=Number(s.activeSubnets||0).toLocaleString();document.getElementById('addedCount').textContent=Number(s.addedLastSync||0).toLocaleString();document.getElementById('removedCount').textContent=Number(s.removedLastSync||0).toLocaleString();document.getElementById('syncBlock').textContent=Number(s.lastFinalizedBlock||0).toLocaleString();document.getElementById('rpcStatusText').textContent=ok?'链上连接正常':s.rpcStatus==='error'?'链上连接异常':'等待首次同步';document.getElementById('rpcDot').style.background=ok?'var(--good)':'var(--bad)';document.getElementById('syncStateLabel').textContent=ok?'实时同步':'异常';document.getElementById('syncTime').textContent=s.lastSyncMs?`最后更新：${fmtDate(s.lastSyncMs).slice(11)} · 12 秒自动刷新`:'等待首次同步';}

function liveRecomputeRows(from,to){rows=rows.filter(r=>r.time.getTime()>=from&&r.time.getTime()<=to).sort((a,b)=>a.time-b.time||a.block-b.block);let cumA=0;for(let i=0;i<rows.length;i++){const r=rows[i];r.i=i+1;cumA+=r.actual;r.cumA=cumA;}}
function liveRefreshFilteredRows(){const q=document.getElementById('blockSearch').value.trim().toLowerCase().replace(/,/g,'').replace('#','');filteredRows=!q?rows.slice():rows.filter(r=>String(r.block).includes(q)||String(r.i)===q||fmtDate(r.time).toLowerCase().includes(q));sortBlockRows();const oldScroll=list.scrollTop;resetVirtual(false);list.scrollTop=oldScroll;renderVirtual();document.querySelector('.table-foot > div:first-child').textContent=rows.length?'实际链上数据已更新 · 连续滚动':'该时间范围暂无区块数据';}

async function liveAppendDetail(from,to){
  if(!rows.length){await loadDetail();return;}
  const lastTime=rows[rows.length-1].time.getTime(),queryFrom=Math.max(from,lastTime+1);if(queryFrom>to){liveRecomputeRows(from,to);liveRefreshFilteredRows();return;}
  const data=await apiJson('/api/blocks?'+queryString({netuid:selectedSubnet,from:queryFrom,to,offset:0,limit:100}));const seen=new Set(rows.slice(-32).map(r=>r.block));
  for(const item of data.items||[]){const block=Number(item.block_number);if(seen.has(block))continue;const actual=tao(item.actual_rao),chainBuy=tao(item.chain_buy_rao);rows.push({i:0,block,time:new Date(Number(item.timestamp_ms)),actual,chainBuy,totalEmission:actual+chainBuy,cumA:0});seen.add(block);}
  liveRecomputeRows(from,to);liveRefreshFilteredRows();document.getElementById('rangeText').textContent=`${fmtDate(from)} — ${fmtDate(to)}`;await loadChart();
}

async function liveTick(){
  if(liveBusy||document.visibilityState==='hidden')return;liveBusy=true;
  try{
    // Establish a baseline from the already-saved D1 state so the first live tick
    // does not mistake the entire existing dataset for a new update and reload it.
    if(liveLastFinalized===0){const baseline=await apiJson('/api/status');liveLastFinalized=Number(baseline.lastFinalizedBlock||0);liveApplyStatus(baseline);}
    const sync=await apiJson(`/api/sync?max=${LIVE_SYNC_MAX_BLOCKS}`,{method:'POST'});const finalized=Number(sync.finalizedBlock||0),advanced=finalized>liveLastFinalized;if(finalized>0)liveLastFinalized=finalized;
    const rolling=liveRollRange(),status=await apiJson('/api/status');liveApplyStatus(status);
    if(Date.now()-liveLastRegistryRefresh>5*60_000||Number(status.activeSubnets)!==subnetRegistry.filter(x=>x.status==='active').length){await loadSubnets();liveLastRegistryRefresh=Date.now();}
    if(rolling&&advanced){await loadOverview();const {from,to}=selectedRange();await liveAppendDetail(from,to);}
  }catch(error){console.warn('live refresh failed',error);document.getElementById('syncStateLabel').textContent='实时同步重试中';}
  finally{liveBusy=false;}
}

['startTime','endTime'].forEach(id=>document.getElementById(id)?.addEventListener('change',()=>{if(liveAdjustingRange)return;document.querySelectorAll('.preset').forEach(b=>b.classList.toggle('active',b.dataset.range==='custom'));}));
window.addEventListener('load',()=>{setTimeout(()=>liveTick(),LIVE_REFRESH_MS);setInterval(()=>liveTick(),LIVE_REFRESH_MS);});
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')liveTick();});
