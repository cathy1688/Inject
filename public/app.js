const RAO_PER_TAO=1_000_000_000;
const DAY_MS=86_400_000;
const ROW_H=48;
const OVERSCAN=8;
const PAGE_SIZE=5000;

let subnetRegistry=[];
let overviewItems=[];
let selectedSubnet=128;
let rows=[];
let filteredRows=[];
let blockSortKey='time';
let blockSortDir='desc';
let loadToken=0;

const list=document.getElementById('virtualList');
const spacer=document.getElementById('virtualSpacer');
const win=document.getElementById('virtualWindow');

function pad(n){return String(n).padStart(2,'0');}
function fmtDate(value){const d=value instanceof Date?value:new Date(value);return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;}
function toLocalInput(d){return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;}
function fmtNum(n,d=6){return Number(n||0).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});}
function tao(rao){return rao==null?0:Number(rao)/RAO_PER_TAO;}
function selectedRange(){return {from:new Date(document.getElementById('startTime').value).getTime(),to:new Date(document.getElementById('endTime').value).getTime()};}
function queryString(params){const q=new URLSearchParams();Object.entries(params).forEach(([k,v])=>{if(v!=null)q.set(k,String(v));});return q.toString();}

async function apiJson(path,options){const response=await fetch(path,options);const data=await response.json().catch(()=>({error:`HTTP ${response.status}`}));if(!response.ok)throw new Error(data.error||`HTTP ${response.status}`);return data;}

function setDefaultRange(){const end=new Date(),start=new Date(end.getTime()-DAY_MS);document.getElementById('startTime').value=toLocalInput(start);document.getElementById('endTime').value=toLocalInput(end);}

async function syncChain(){document.getElementById('syncStateLabel').textContent='同步中';try{await apiJson('/api/sync?max=8',{method:'POST'});}catch(error){console.warn('sync failed',error);}}

async function loadStatus(){
  try{
    const s=await apiJson('/api/status');
    document.getElementById('activeCount').textContent=Number(s.activeSubnets||0).toLocaleString();
    document.getElementById('addedCount').textContent=Number(s.addedLastSync||0).toLocaleString();
    document.getElementById('removedCount').textContent=Number(s.removedLastSync||0).toLocaleString();
    document.getElementById('syncBlock').textContent=Number(s.lastFinalizedBlock||0).toLocaleString();
    document.getElementById('syncTime').textContent=s.lastSyncMs?`最后更新：${fmtDate(s.lastSyncMs).slice(0,16)}`:'尚未开始采集';
    const ok=s.rpcStatus==='ok';
    document.getElementById('rpcStatusText').textContent=ok?'链上连接正常':s.rpcStatus==='error'?'链上连接异常':'等待首次同步';
    document.getElementById('syncStateLabel').textContent=ok?'实时同步':s.rpcStatus==='error'?'异常':'等待同步';
    document.getElementById('rpcDot').style.background=ok?'var(--good)':'var(--bad)';
  }catch{
    document.getElementById('rpcStatusText').textContent='服务连接异常';
    document.getElementById('syncStateLabel').textContent='异常';
    document.getElementById('rpcDot').style.background='var(--bad)';
  }
}

async function loadSubnets(){const data=await apiJson('/api/subnets');subnetRegistry=(data.items||[]).map(x=>({...x,netuid:Number(x.netuid)}));const active=subnetRegistry.filter(x=>x.status==='active');if(!active.some(x=>x.netuid===selectedSubnet))selectedSubnet=active.find(x=>x.netuid===128)?.netuid??active[0]?.netuid??0;fillSubnetSelect();}
function fillSubnetSelect(){const select=document.getElementById('subnetSelect');select.innerHTML='';subnetRegistry.filter(x=>x.status==='active').forEach(x=>{const o=document.createElement('option');o.value=x.netuid;o.textContent=`SN${x.netuid} · ${x.name}`;o.selected=x.netuid===selectedSubnet;select.appendChild(o);});}

async function loadOverview(){const {from,to}=selectedRange();const data=await apiJson('/api/subnets/summary?'+queryString({from,to}));overviewItems=data.items||[];renderOverview();updateSelectedMetrics();}

function renderOverview(){
  const q=document.getElementById('subnetSearch').value.trim().toLowerCase(),container=document.getElementById('overviewRows');
  const items=overviewItems.filter(x=>!q||(`sn${x.netuid} ${x.name}`).toLowerCase().includes(q));
  container.innerHTML=items.map(x=>`<div class="overview-row" data-sn="${x.netuid}">
    <div class="sn">SN${x.netuid} · ${x.name}</div>
    <div>${fmtNum(tao(x.actualRao),5)} TAO</div>
    <div class="muted">${fmtNum(tao(x.chainBuyRao),5)} TAO</div>
    <div>${fmtNum(tao(x.totalEmissionRao),5)} TAO</div>
    <div class="muted">${x.status==='active'?'活跃':'已注销'}</div>
  </div>`).join('');
  container.querySelectorAll('.overview-row').forEach(el=>el.onclick=async()=>{selectedSubnet=Number(el.dataset.sn);document.getElementById('subnetSelect').value=String(selectedSubnet);updateSelectedMetrics();await loadDetail();document.querySelector('.metrics').scrollIntoView({behavior:'smooth',block:'start'});});
}

function updateSelectedMetrics(){
  const s=overviewItems.find(x=>Number(x.netuid)===selectedSubnet),meta=subnetRegistry.find(x=>x.netuid===selectedSubnet);
  if(!s){['totalActual','totalChainBuy','totalEmission','avgActual'].forEach(id=>document.getElementById(id).textContent='—');document.getElementById('blockCount').textContent='0';document.getElementById('coverage').textContent='该时间范围暂无区块数据';return;}
  const actual=tao(s.actualRao),chain=tao(s.chainBuyRao),total=tao(s.totalEmissionRao),count=Number(s.blockCount||0);
  document.getElementById('totalActual').textContent='τ '+fmtNum(actual,6);
  document.getElementById('totalChainBuy').textContent='τ '+fmtNum(chain,6);
  document.getElementById('totalEmission').textContent='τ '+fmtNum(total,6);
  document.getElementById('blockCount').textContent=count.toLocaleString();
  document.getElementById('coverage').textContent=count?`${count.toLocaleString()} 个 finalized 区块`:'该时间范围暂无区块数据';
  document.getElementById('avgActual').textContent=count?'τ '+fmtNum(actual/count,8):'—';
  document.getElementById('trendTitle').textContent=`SN${selectedSubnet} · ${meta?.name||''}｜实际注入趋势`;
}

async function loadDetail(){
  const token=++loadToken;rows=[];filteredRows=[];resetVirtual();document.getElementById('blockSearch').value='';
  const {from,to}=selectedRange();document.getElementById('rangeText').textContent=`${fmtDate(from)} — ${fmtDate(to)}`;document.querySelector('.table-foot > div:first-child').textContent='正在读取区块数据…';
  let offset=0,total=Infinity,cumA=0;
  while(offset<total&&token===loadToken){
    const data=await apiJson('/api/blocks?'+queryString({netuid:selectedSubnet,from,to,offset,limit:PAGE_SIZE}));total=Number(data.total||0);
    for(const item of data.items||[]){const actual=tao(item.actual_rao),chainBuy=tao(item.chain_buy_rao),totalEmission=actual+chainBuy;cumA+=actual;rows.push({i:rows.length+1,block:Number(item.block_number),time:new Date(Number(item.timestamp_ms)),actual,chainBuy,totalEmission,cumA});}
    offset+=Number((data.items||[]).length);filteredRows=rows.slice();sortBlockRows();resetVirtual(false);document.querySelector('.table-foot > div:first-child').textContent=`已读取 ${rows.length.toLocaleString()} / ${total.toLocaleString()} 个区块`;if(!(data.items||[]).length)break;
  }
  if(token!==loadToken)return;document.querySelector('.table-foot > div:first-child').textContent=rows.length?'完整实际区块数据已载入':'该时间范围暂无区块数据';await loadChart();
}

function sortBlockRows(){const dir=blockSortDir==='asc'?1:-1;filteredRows.sort((a,b)=>{let av=a[blockSortKey],bv=b[blockSortKey];if(blockSortKey==='time'){av=a.time.getTime();bv=b.time.getTime();}return av<bv?-dir:av>bv?dir:0;});updateSortHeader();}
function updateSortHeader(){document.querySelectorAll('#blockTableHead .sort-cell').forEach(cell=>{const active=cell.dataset.key===blockSortKey;cell.classList.toggle('active',active);cell.querySelector('.sort-mark').textContent=active?(blockSortDir==='asc'?'↑':'↓'):'';});}
function applyBlockSearch(){const q=document.getElementById('blockSearch').value.trim().toLowerCase().replace(/,/g,'').replace('#','');filteredRows=!q?rows.slice():rows.filter(r=>String(r.block).includes(q)||String(r.i)===q||fmtDate(r.time).toLowerCase().includes(q));sortBlockRows();resetVirtual();}
function resetVirtual(resetScroll=true){spacer.style.height=(filteredRows.length*ROW_H)+'px';if(resetScroll)list.scrollTop=0;renderVirtual();}
function renderVirtual(){
  const start=Math.max(0,Math.floor(list.scrollTop/ROW_H)-OVERSCAN),visible=Math.ceil(list.clientHeight/ROW_H)+OVERSCAN*2,end=Math.min(filteredRows.length,start+visible);win.style.transform=`translateY(${start*ROW_H}px)`;let html='';
  for(let k=start;k<end;k++){const r=filteredRows[k];html+=`<div class="row"><div class="idx">${r.i.toLocaleString()}</div><div class="block">#${r.block.toLocaleString()}</div><div class="time">${fmtDate(r.time)}</div><div class="num actual">${fmtNum(r.actual,8)} TAO</div><div class="num">${fmtNum(r.chainBuy,8)} TAO</div><div class="num">${fmtNum(r.totalEmission,8)} TAO</div><div class="num cum">${fmtNum(r.cumA,6)} TAO</div></div>`;}
  win.innerHTML=html;const first=filteredRows.length?Math.min(filteredRows.length,Math.floor(list.scrollTop/ROW_H)+1):0,last=filteredRows.length?Math.min(filteredRows.length,first+Math.ceil(list.clientHeight/ROW_H)-1):0;document.getElementById('visibleRange').textContent=`${first.toLocaleString()}–${last.toLocaleString()} / ${filteredRows.length.toLocaleString()}`;
}

async function loadChart(){
  const mode=document.getElementById('chartMode').value;
  if(mode==='raw'&&rows.length){const maxPoints=1200,step=Math.max(1,Math.ceil(rows.length/maxPoints));drawChart(rows.filter((_,i)=>i%step===0).map(r=>({actual_rao:r.actual*RAO_PER_TAO})));document.getElementById('chartHint').textContent=`原始区块完整保存 · 图表抽样 ${Math.min(rows.length,maxPoints).toLocaleString()} 点`;return;}
  const {from,to}=selectedRange();try{const data=await apiJson('/api/chart?'+queryString({netuid:selectedSubnet,from,to}));drawChart(data.items||[]);document.getElementById('chartHint').textContent='按小时准确汇总';}catch{drawChart([]);}
}
function drawChart(items){
  const canvas=document.getElementById('chart'),rect=canvas.getBoundingClientRect(),dpr=window.devicePixelRatio||1;canvas.width=rect.width*dpr;canvas.height=rect.height*dpr;const ctx=canvas.getContext('2d');ctx.scale(dpr,dpr);const w=rect.width,h=rect.height,pL=52,pR=16,pT=18,pB=30;ctx.clearRect(0,0,w,h);
  if(!items.length){ctx.fillStyle='#40566d';ctx.font='13px Source Han Sans SC,Microsoft YaHei';ctx.fillText('所选时间范围暂无趋势数据',pL,pT+24);return;}
  const A=items.map(x=>tao(x.actual_rao));let min=Math.min(...A),max=Math.max(...A);if(min===max){min-=Math.max(.000000001,min*.01);max+=Math.max(.000000001,max*.01);}const x=i=>pL+i*(w-pL-pR)/Math.max(A.length-1,1),y=v=>pT+(max-v)*(h-pT-pB)/(max-min||1);
  ctx.strokeStyle='#eee9e1';ctx.fillStyle='#40566d';ctx.lineWidth=1;ctx.font='10px Source Han Sans SC,Microsoft YaHei';for(let g=0;g<4;g++){const yy=pT+g*(h-pT-pB)/3;ctx.beginPath();ctx.moveTo(pL,yy);ctx.lineTo(w-pR,yy);ctx.stroke();ctx.fillText((max-g*(max-min)/3).toFixed(6),4,yy+3);}ctx.beginPath();A.forEach((v,i)=>{const xx=x(i),yy=y(v);if(i)ctx.lineTo(xx,yy);else ctx.moveTo(xx,yy);});ctx.strokeStyle='#63283a';ctx.lineWidth=1.8;ctx.stroke();
}

function setRange(type){document.querySelectorAll('.preset').forEach(b=>b.classList.toggle('active',b.dataset.range===type));if(type==='custom')return;const end=new Date(),days=type==='24h'?1:type==='7d'?7:30,start=new Date(end.getTime()-days*DAY_MS);document.getElementById('startTime').value=toLocalInput(start);document.getElementById('endTime').value=toLocalInput(end);}
async function runQuery(){await syncChain();await Promise.all([loadStatus(),loadSubnets()]);await loadOverview();await loadDetail();}

list.addEventListener('scroll',renderVirtual,{passive:true});
document.getElementById('subnetSearch').addEventListener('input',renderOverview);
document.getElementById('blockSearch').addEventListener('input',applyBlockSearch);
document.getElementById('subnetSelect').addEventListener('change',async e=>{selectedSubnet=Number(e.target.value);updateSelectedMetrics();await loadDetail();});
document.querySelectorAll('.preset').forEach(b=>b.onclick=()=>setRange(b.dataset.range));
document.getElementById('applyBtn').onclick=runQuery;
document.getElementById('syncBtn').onclick=runQuery;
document.getElementById('chartMode').addEventListener('change',()=>loadChart().catch(()=>{}));
document.querySelectorAll('#blockTableHead .sort-cell').forEach(cell=>cell.addEventListener('click',()=>{const key=cell.dataset.key;if(blockSortKey===key)blockSortDir=blockSortDir==='asc'?'desc':'asc';else{blockSortKey=key;blockSortDir='asc';}filteredRows=filteredRows.slice();sortBlockRows();resetVirtual();}));
document.getElementById('exportBtn').onclick=()=>{const {from,to}=selectedRange();location.href='/api/export.csv?'+queryString({netuid:selectedSubnet,from,to});};
window.addEventListener('resize',()=>loadChart().catch(()=>{}));

(async function init(){setDefaultRange();updateSortHeader();await syncChain();await loadStatus();await loadSubnets();await loadOverview();await loadDetail();})().catch(error=>{console.error(error);document.getElementById('rpcStatusText').textContent='初始化失败';document.getElementById('rpcDot').style.background='var(--bad)';});
