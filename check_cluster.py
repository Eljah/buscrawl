# -*- coding: utf-8 -*-
import json, math, heapq
from collections import defaultdict
ACC='/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map.json'; ROADS='/home/eljah/apps/buscrawl/osm-cache/overpass-kazan-roads.json'
WALK_SPEED=1.3; WALK_RADIUS=2000.0; CONNECT_RADIUS=180.0; CLUSTER=180.0; CELL=0.002
EXCLUDE=set(['motorway','motorway_link','construction','proposed','platform','elevator'])
def hav(a,b,c,d):
 R=6371000.0; p1=math.radians(a); p2=math.radians(c); dp=math.radians(c-a); dl=math.radians(d-b); x=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2; return R*2*math.atan2(math.sqrt(x),math.sqrt(1-x))
def key(lat,lon): return f'{lat:.7f}|{lon:.7f}'
def cell(lat,lon): return (int(lat/CELL),int(lon/CELL))
def find(p,i):
 while p[i]!=i:
  p[i]=p[p[i]]; i=p[i]
 return i
def union(p,a,b):
 ra,rb=find(p,a),find(p,b)
 if ra!=rb: p[rb]=ra
acc=json.load(open(ACC,encoding='utf-8'))
stops=[dict(id=s['stopId'],name=s['stopName'],lat=float(s['latitude']),lon=float(s['longitude']),transport=float(s['transportSeconds'])) for s in acc['stops']]
by=defaultdict(list)
for s in stops: by[s['name'].strip().lower()].append(s)
clustered=[]
for arr in by.values():
 p=list(range(len(arr)))
 for i,a in enumerate(arr):
  for j in range(i+1,len(arr)):
   b=arr[j]
   if hav(a['lat'],a['lon'],b['lat'],b['lon'])<=CLUSTER: union(p,i,j)
 best={}
 for i,s in enumerate(arr): best[find(p,i)]=min(best.get(find(p,i),10**18),s['transport'])
 for i,s in enumerate(arr):
  t=dict(s); t['transport']=best[find(p,i)]; clustered.append(t)
roads=[]; raw=json.load(open(ROADS,encoding='utf-8'))
for e in raw.get('elements',[]):
 tags=e.get('tags') or {}; hw=tags.get('highway'); geom=e.get('geometry') or []
 if not hw or hw in EXCLUDE or len(geom)<2: continue
 roads.append((e.get('id'),tags.get('name',''),hw,[(float(x['lat']),float(x['lon'])) for x in geom]))
node_id={}; nodes=[]; grid=defaultdict(list); edges=defaultdict(list)
def nid(pt):
 k=key(*pt)
 if k not in node_id:
  i=len(nodes); node_id[k]=i; nodes.append(pt); grid[cell(*pt)].append(i)
 return node_id[k]
for rid,name,hw,pts in roads:
 ids=[nid(p) for p in pts]
 for a,b in zip(ids,ids[1:]):
  la,lo=nodes[a]; lb,lblo=nodes[b]; d=hav(la,lo,lb,lblo); edges[a].append((b,d)); edges[b].append((a,d))
def closest(lat,lon):
 ci,cj=cell(lat,lon); arr=[]
 for r in range(1,8):
  for di in range(-r,r+1):
   for dj in range(-r,r+1):
    for i in grid.get((ci+di,cj+dj),[]):
     la,lo=nodes[i]; d=hav(lat,lon,la,lo)
     if d<=CONNECT_RADIUS: arr.append((d,i))
  if arr: return sorted(arr)
 return []
def dijkstra(seed):
 best={}; pq=[]
 for st in seed:
  for dist,i in closest(st['lat'],st['lon']):
   walk=dist; total=st['transport']+walk/WALK_SPEED; metric=walk
   if i not in best or metric<best[i]['metric']:
    best[i]=dict(metric=metric,stop=st,walk=walk,total=total); heapq.heappush(pq,(metric,i,st['id'],walk,total,st))
 while pq:
  metric,i,_,walk,total,st=heapq.heappop(pq); cur=best.get(i)
  if cur is None or metric>cur['metric']+1e-6: continue
  for j,d in edges.get(i,[]):
   nw=walk+d
   if nw>WALK_RADIUS: continue
   nt=st['transport']+nw/WALK_SPEED; nm=nw
   if j not in best or nm<best[j]['metric']:
    best[j]=dict(metric=nm,stop=st,walk=nw,total=nt); heapq.heappush(pq,(nm,j,st['id'],nw,nt,st))
 return best
best=dijkstra(clustered)
def fmt(r):
 if not r: return 'none'
 s=r['stop']; return f"{s['id']} {s['name']} transport={s['transport']/60:.1f} walk={r['walk']/WALK_SPEED/60:.1f} total={r['total']/60:.1f}"
for target in ['Брюсова','Казахская']:
 print('\n',target)
 for rid,name,hw,pts in roads:
  if target in name:
   for lat,lon in pts[:min(5,len(pts))]: print(lat,lon,fmt(best.get(node_id.get(key(lat,lon)))))
   break
print('json cluster radius',acc.get('stopClusterRadiusMeters'),'updated',acc.get('updatedAt'))
