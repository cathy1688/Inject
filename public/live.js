const LIVE_REFRESH_MS=12_000;
const LIVE_OVERVIEW_REFRESH_MS=12_000;
let liveBusy=false;
let liveLastFinalized=0;
let liveLastRegistryRefresh=0;
let liveLastOverviewRefresh=0;
let liveOverviewBusy=false;
let liveAdjustingRange=false;

function livePreset(){return document.querySelector('.preset.active')?.dataset?.range||'custom';}
function liveRangeMs(mode){if(mode==='24h')return DAY_MS;if(mode==='7d')return 7*DAY_MS;if(mode==='30d')return 30*DAY_MS;return null;}
function liveRollRange(){const span=liveRangeMs(livePreset());if(span==null)return false;const end=new Date(),start=new Date(end.getTime()-span);liveAdjustingRange=true;document.getElementById('startTime').value=toLocalInput(start);document.getElementById('endTime').value=toLocalInput(end);liveAdjustingRange=false;return true;}

function liveApplyStatus(s){const ok=s.rpcStatus==='ok';document.getElementById('activeCount').textContent=Number(s.activeSubnets||0).toLocaleString();document.getElementById('addedCount').textContent=Number(s.addedLastSync||0).toLocaleString();document.getElementById('removedCount').textContent=Number(s.removedLastSync||0).toLocaleString();document.getElementById('syncBlock').textContent=Number(s.lastFinalizedBlock||0).toLocaleString();document.getElementById('rpcStatusText').textContent=ok?'链上连接正常':s.rpcStatus==='error'?'链上连接异常':'等待首次同步';document.getElementById('rpcDot').style.background=ok?'var(--good)':'var(--bad)';document.getElementById('syncStateLabel').textContent=ok?'实时同步':'异常';document.getElementById('syncTime').textContent=s.lastSyncMs?`最后更新：${fmtDate(s.lastSyncMs).slice(11)} · 后台 12 秒采集 · 页面 12 秒刷新`:'等待首次同步';}

function liveRecomputeRows(from,to){rows=rows.filter(r=>r.time.getTime()>=from&&r.time.getTime()<=to).sort((a,b)=>a.time-b.time||a.block-b.block);let cumA=0;for(let i=0;i<rows.length;i++){const r=rows[i];r.i=i+1;cumA+=r.actual;r.cumA=cumA;}}
function liveRefreshFilteredRows(){const q=document.getElementById('blockSearch').value.trim().toLowerCase().replace(/,/g,'').replace('#','');filteredRows=!q?rows.slice():rows.filter(r=>String(r.block).includes(q)||String(r.i)===q||fmtDate(r.time).toLowerCase().includes(q));sortBlockRows();const oldScroll=list.scrollTop;resetVirtual(false);list.scrollTop=oldScroll;renderVirtual();document.querySelector('.table-foot > div:first-child').textContent=rows.length?'实际链上数据已更新 · 连续滚动':'该时间范围暂无区块数据';}

function liveUpdateSelectedMetrics(){
  const actual=rows.reduce((sum,r)=>sum+r.actual,0),chain=rows.reduce((sum,r)=>sum+r.chainBuy,0),total=actual+chain,count=rows.length;
  const meta=subnetRegistry.find(x=>x.netuid===selectedSubnet);
  document.getElementById('totalActual').textContent=count?'τ '+fmtNum(actual,6):'—';
  document.getElementById('totalChainBuy').textContent=count?'τ '+fmtNum(chain,6):'—';
  document.getElementById('totalEmission').textContent=count?'τ '+fmtNum(total,6):'—';
  document.getElementById('blockCount').textContent=count.toLocaleString();
  document.getElementById('coverage').textContent=count?`${count.toLocaleString()} 个 finalized 区块`:'该时间范围暂无区块数据';
  document.getElementById('avgActual').textContent=count?'τ '+fmtNum(actual/count,8):'—';
  document.getElementById('trendTitle').textContent=`SN${selectedSubnet} · ${meta?.name||''}｜实际注入趋势`;
}

async function liveAppendDetail(from,to){
  const queryFrom=rows.length?Math.max(from,rows[rows.length-1].time.getTime()+1):Math.max(from,to-5*60_000);
  if(queryFrom>to){liveRecomputeRows(from,to);liveRefreshFilteredRows();liveUpdateSelectedMetrics();return;}
  const data=await apiJson('/api/blocks?'+queryString({netuid:selectedSubnet,from:queryFrom,to,offset:0,limit:100}));const seen=new Set(rows.slice(-64).map(r=>r.block));
  for(const item of data.items||[]){const block=Number(item.block_number);if(seen.has(block))continue;const actual=tao(item.actual_rao),chainBuy=tao(item.chain_buy_rao);rows.push({i:0,block,time:new Date(Number(item.timestamp_ms)),actual,chainBuy,totalEmission:actual+chainBuy,cumA:0});seen.add(block);}
  liveRecomputeRows(from,to);liveRefreshFilteredRows();liveUpdateSelectedMetrics();document.getElementById('rangeText').textContent=`${fmtDate(from)} — ${fmtDate(to)}`;await loadChart();
}

function liveRefreshOverviewInBackground(){
  if(liveOverviewBusy||Date.now()-liveLastOverviewRefresh<LIVE_OVERVIEW_REFRESH_MS)return;
  liveOverviewBusy=true;liveLastOverviewRefresh=Date.now();
  loadOverview().catch(error=>console.warn('overview refresh failed',error)).finally(()=>{liveOverviewBusy=false;});
}

async function liveTick(){
  if(liveBusy)return;liveBusy=true;
  try{
    // The chain is collected by a Durable Object alarm in the backend. The
    // browser only reads D1 state, so switching tabs or closing the page no
    // longer controls whether blocks are captured.
    const status=await apiJson('/api/status');
    const finalized=Number(status.lastFinalizedBlock||0);
    const rolling=liveRollRange();

    if(liveLastFinalized===0){
      liveLastFinalized=finalized;
      liveApplyStatus(status);
      if(rolling)liveRefreshOverviewInBackground();
      return;
    }

    const advanced=finalized>liveLastFinalized;
    if(finalized>0)liveLastFinalized=finalized;

    // Current subnet detail remains first priority whenever a new finalized
    // block has arrived in D1.
    if(rolling&&advanced){const {from,to}=selectedRange();await liveAppendDetail(from,to);}

    liveApplyStatus(status);

    if(Date.now()-liveLastRegistryRefresh>5*60_000||Number(status.activeSubnets)!==subnetRegistry.filter(x=>x.status==='active').length){await loadSubnets();liveLastRegistryRefresh=Date.now();}

    // All-subnet summary is requested every 12 seconds as well, but stays
    // asynchronous so it cannot delay the selected subnet detail table.
    if(rolling)liveRefreshOverviewInBackground();
  }catch(error){console.warn('live refresh failed',error);document.getElementById('syncStateLabel').textContent='实时同步重试中';}
  finally{liveBusy=false;}
}

function datetimeSeconds(value){
  const match=String(value||'').match(/T\d{2}:\d{2}:(\d{2})/);
  return match?match[1]:null;
}

['startTime','endTime'].forEach(id=>{
  const input=document.getElementById(id);
  if(!input)return;
  let lastSeconds=datetimeSeconds(input.value);
  const rememberSeconds=()=>{lastSeconds=datetimeSeconds(input.value);};
  input.addEventListener('focus',rememberSeconds);
  input.addEventListener('pointerdown',rememberSeconds);

  input.addEventListener('input',()=>{
    if(!liveAdjustingRange)document.querySelectorAll('.preset').forEach(b=>b.classList.toggle('active',b.dataset.range==='custom'));
    const seconds=datetimeSeconds(input.value);
    if(seconds!=null&&lastSeconds!=null&&seconds!==lastSeconds){lastSeconds=seconds;requestAnimationFrame(()=>input.blur());return;}
    lastSeconds=seconds;
  });

  input.addEventListener('change',()=>{
    if(!liveAdjustingRange)document.querySelectorAll('.preset').forEach(b=>b.classList.toggle('active',b.dataset.range==='custom'));
  });
  input.addEventListener('keydown',event=>{if(event.key==='Enter')input.blur();});
});
window.addEventListener('load',()=>{setTimeout(()=>liveTick(),LIVE_REFRESH_MS);setInterval(()=>liveTick(),LIVE_REFRESH_MS);});
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')liveTick();});
