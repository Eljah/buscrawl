const fs=require('fs');
const routes=JSON.parse(fs.readFileSync('src/main/resources/routes.json','utf8')).bus;
const map={};
for(const [id,a] of Object.entries(routes)){
 const num=a[1]; const tt=a[5]||0; const key=(tt===1?'trolley':tt===2?'tram':'bus')+':'+String(num).toLowerCase();
 (map[key]??=[]).push(id);
}
const snap=JSON.parse(fs.readFileSync('tmp_navi_snapshot.json','utf8'));
let total=0, matched=0, multi=0, missing=[];
for(const t of snap.tree){ const kind=t.tt_id==='2'?'trolley':t.tt_id==='3'?'tram':'bus'; for(const r of t.routes){ total++; const key=kind+':'+String(r.mr_num).toLowerCase(); if(map[key]){matched++; if(map[key].length>1) multi++;} else missing.push(`${key} ${r.mr_id} ${r.mr_title}`); }}
console.log({total,matched,multi,missingCount:missing.length});
console.log('missing',missing.slice(0,80));
console.log('sample matched 45',map['bus:45'],'10',map['bus:10'],'10a',map['bus:10а'],'tram5a',map['tram:5а'],'trolley1',map['trolley:1']);
