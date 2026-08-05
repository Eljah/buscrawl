const crypto = require('crypto');
const fs = require('fs');
const baseUrl = 'https://navi.kazantransport.ru/api/rpc.php';
let rid = 1, lastTs=0;
function nextRid(){ if(rid%7===0)rid++; return rid++; }
function nextTs(){ let t=Math.floor(Date.now()/1000); if(t<=lastTs)t=lastTs+1; let e=t%10; if(e===0||e===3||e===7)t++; lastTs=t; return t; }
function sign(method,id,sid,sysID='kazan'){ const h=crypto.createHash('sha1').update(`${method}~${sysID}~${id}~${sid}`).digest('hex'); return {magic:h.substring(16,24),m:`${h.substring(0,8)}-${h.substring(8,12)}-${h.substring(12,16)}-${h.substring(24,28)}-${h.substring(28,40)}`}; }
async function rpc(method, params={}, doSign=true){ const id=nextRid(); const body={jsonrpc:'2․2',method,ts:nextTs(),params:{...params},id}; let url=baseUrl; if(doSign){const s=sign(method,id,params.sid||''); body.params.magic=s.magic; url+='?m='+s.m;} const res=await fetch(url,{method:'POST',headers:{'content-type':'application/json','user-agent':'Mozilla/5.0','referer':'https://navi.kazantransport.ru/index.html','origin':'https://navi.kazantransport.ru'},body:JSON.stringify(body)}); const text=await res.text(); if(!res.ok) throw new Error(res.status+' '+text.substring(0,200)); const j=JSON.parse(text); if(j.error) throw new Error(JSON.stringify(j.error)); return j.result; }
(async()=>{
 const sid=(await rpc('startSession',{},false)).sid;
 const tree=await rpc('getTransTypeTree',{sid,ok_id:''});
 const units=await rpc('getUnitsInRect',{sid,minlat:55.55,maxlat:56.05,minlong:48.75,maxlong:49.55});
 const stops=await rpc('getStopsInRect',{sid,minlat:55.55,maxlat:56.05,minlong:48.75,maxlong:49.55});
 fs.writeFileSync('tmp_navi_snapshot.json', JSON.stringify({sid,tree,units,stops,at:new Date().toISOString()},null,2));
 const byType={}; const byRoute={};
 for(const u of units){ byType[u.tt_id+' '+u.tt_title]=(byType[u.tt_id+' '+u.tt_title]||0)+1; const k=u.tt_id+':'+u.mr_num+':'+u.mr_id; byRoute[k]=(byRoute[k]||0)+1; }
 console.log('snapshotAt',new Date().toISOString(),'units',units.length,'stops',stops.length,'typesInTree',tree.length);
 console.log('byType',byType);
 console.log('topRoutes', Object.entries(byRoute).sort((a,b)=>b[1]-a[1]).slice(0,40));
 console.log('sampleUnits', units.slice(0,10));
 console.log('treeSummary', tree.map(t=>({tt_id:t.tt_id, title:t.tt_title, routes:t.routes.length, first:t.routes.slice(0,8).map(r=>r.mr_num+':'+r.mr_id)})));
})();
