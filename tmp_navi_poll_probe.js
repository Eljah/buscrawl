const crypto = require('crypto');
const baseUrl='https://navi.kazantransport.ru/api/rpc.php'; let rid=1,lastTs=0;
function sleep(ms){return new Promise(r=>setTimeout(r,ms));}
function nextRid(){ if(rid%7===0)rid++; return rid++; } function nextTs(){ let t=Math.floor(Date.now()/1000); if(t<=lastTs)t=lastTs+1; let e=t%10; if(e===0||e===3||e===7)t++; lastTs=t; return t; }
function sign(method,id,sid){ const h=crypto.createHash('sha1').update(`${method}~kazan~${id}~${sid}`).digest('hex'); return {magic:h.substring(16,24),m:`${h.substring(0,8)}-${h.substring(8,12)}-${h.substring(12,16)}-${h.substring(24,28)}-${h.substring(28,40)}`}; }
async function rpc(method,params={},doSign=true){ const id=nextRid(); const body={jsonrpc:'2․2',method,ts:nextTs(),params:{...params},id}; let url=baseUrl; if(doSign){const s=sign(method,id,params.sid||''); body.params.magic=s.magic; url+='?m='+s.m;} const res=await fetch(url,{method:'POST',headers:{'content-type':'application/json','user-agent':'Mozilla/5.0','referer':'https://navi.kazantransport.ru/index.html','origin':'https://navi.kazantransport.ru'},body:JSON.stringify(body)}); const j=JSON.parse(await res.text()); if(j.error) throw new Error(JSON.stringify(j.error)); return j.result; }
(async()=>{
 const sid=(await rpc('startSession',{},false)).sid;
 let prev=new Map();
 for(let iter=0; iter<4; iter++){
  const units=await rpc('getUnitsInRect',{sid,minlat:55.55,maxlat:56.05,minlong:48.75,maxlong:49.55});
  const now=new Date();
  const byType={}; let stale=0, changed=0, same=0, noPlate=0, sample=[];
  for(const u of units){
    byType[u.tt_id+':'+u.tt_title]=(byType[u.tt_id+':'+u.tt_title]||0)+1;
    if(!u.u_statenum && !u.u_garagnum) noPlate++;
    const [hh,mm,ss]=(u.u_timenav||'0:0:0').split(':').map(Number);
    const sec=hh*3600+mm*60+ss; const nowSec=now.getHours()*3600+now.getMinutes()*60+now.getSeconds();
    let lag=nowSec-sec; if(lag < -12*3600) lag += 24*3600; if(lag > 12*3600) lag -= 24*3600; if(lag>180) stale++;
    const p=prev.get(u.u_id); if(p){ if(p.lat!==u.u_lat || p.lon!==u.u_long || p.t!==u.u_timenav) changed++; else same++; }
    if(sample.length<6) sample.push(`${u.u_id} ${u.tt_title} ${u.mr_num} ${u.u_statenum}/${u.u_garagnum} ${u.u_timenav} lag=${lag}s sp=${u.u_speed} ${u.u_lat},${u.u_long}`);
  }
  console.log('\nITER',iter,'at',now.toLocaleString('ru-RU',{timeZone:'Europe/Moscow',hour12:false}),'units',units.length,'byType',byType,'stale>180s',stale,'noPlate',noPlate,'changedFromPrev',changed,'sameFromPrev',same);
  console.log(sample.join('\n'));
  prev=new Map(units.map(u=>[u.u_id,{lat:u.u_lat,lon:u.u_long,t:u.u_timenav}]));
  if(iter<3) await sleep(16000);
 }
})();
