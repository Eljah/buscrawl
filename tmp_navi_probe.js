const crypto = require('crypto');
const baseUrl = 'https://navi.kazantransport.ru/api/rpc.php';
let rid = 1;
let lastTs = 0;
function nextRid(){ if (rid % 7 === 0) rid++; return rid++; }
function nextTs(){ let t=Math.floor(Date.now()/1000); if(t<=lastTs)t=lastTs+1; let e=t%10; if(e===0||e===3||e===7)t++; lastTs=t; return t; }
function sign(method,id,sid,sysID='kazan'){
  const h=crypto.createHash('sha1').update(`${method}~${sysID}~${id}~${sid}`).digest('hex');
  return {magic:h.substring(16,24), m:`${h.substring(0,8)}-${h.substring(8,12)}-${h.substring(12,16)}-${h.substring(24,28)}-${h.substring(28,40)}`};
}
async function rpc(method, params={}, doSign=true){
  const id=nextRid();
  const body={jsonrpc:'2․2', method, ts:nextTs(), params:{...params}, id};
  let url=baseUrl;
  if(doSign){ const s=sign(method,id,params.sid||''); body.params.magic=s.magic; url += '?m=' + s.m; }
  const res=await fetch(url,{method:'POST',headers:{'content-type':'application/json','user-agent':'Mozilla/5.0','referer':'https://navi.kazantransport.ru/index.html','origin':'https://navi.kazantransport.ru'},body:JSON.stringify(body)});
  const text=await res.text();
  console.log('\nRPC',method,'status',res.status,'url',url,'body',JSON.stringify(body));
  console.log(text.substring(0,3000));
  let j; try{j=JSON.parse(text)}catch(e){return null}
  return j;
}
(async()=>{
 const st=await rpc('startSession',{},false);
 const sid=st?.result?.sid;
 console.log('SID',sid);
 if(!sid) return;
 await rpc('getOkatoList',{sid});
 await rpc('getTransTypeTree',{sid,ok_id:''});
 await rpc('getUnitsInRect',{sid,minlat:55.55,maxlat:56.05,minlong:48.75,maxlong:49.55});
 await rpc('getStopsInRect',{sid,minlat:55.55,maxlat:56.05,minlong:48.75,maxlong:49.55});
})();
