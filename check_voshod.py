# -*- coding: utf-8 -*-
import json, math, heapq
from collections import defaultdict
ACC='/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map.json'
ROADS='/home/eljah/apps/buscrawl/osm-cache/overpass-kazan-roads.json'
WALK_SPEED=1.3
WALK_RADIUS=2000.0
CONNECT_RADIUS=180.0
FALLBACK_N=3
CELL=0.002
EXCLUDE=set(['motorway','motorway_link','construction','proposed','platform','elevator'])
def hav(a,b,c,d):
    R=6371000.0
    p1=math.radians(a); p2=math.radians(c); dp=math.radians(c-a); dl=math.radians(d-b)
    x=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R*2*math.atan2(math.sqrt(x),math.sqrt(1-x))
def key(lat,lon): return f'{lat:.7f}|{lon:.7f}'
def cell(lat,lon): return (int(lat/CELL), int(lon/CELL))
acc=json.load(open(ACC,encoding='utf-8'))
stops=[dict(id=s['stopId'],name=s['stopName'],lat=float(s['latitude']),lon=float(s['longitude']),transport=float(s['transportSeconds'])) for s in acc['stops']]
origins={s['stopId'] for s in acc['originStops']}
roads_raw=json.load(open(ROADS,encoding='utf-8'))
roads=[]
for e in roads_raw.get('elements',[]):
    tags=e.get('tags') or {}; hw=tags.get('highway'); geom=e.get('geometry') or []
    if not hw or hw in EXCLUDE or len(geom)<2: continue
    roads.append((e.get('id'),tags.get('name',''),hw,[(float(p['lat']),float(p['lon'])) for p in geom]))
node_id={}; nodes=[]; edges=defaultdict(list); grid=defaultdict(list)
def nid(pt):
    k=key(*pt)
    if k not in node_id:
        i=len(nodes); node_id[k]=i; nodes.append(pt); grid[cell(*pt)].append(i)
    return node_id[k]
for rid,name,hw,pts in roads:
    ids=[nid(p) for p in pts]
    for a,b in zip(ids,ids[1:]):
        la,lo=nodes[a]; lb,lblo=nodes[b]
        dist=hav(la,lo,lb,lblo)
        edges[a].append((b,dist)); edges[b].append((a,dist))
def closest(lat,lon,rad=CONNECT_RADIUS):
    ci,cj=cell(lat,lon); arr=[]
    for radius in range(1,8):
        for di in range(-radius,radius+1):
            for dj in range(-radius,radius+1):
                for i in grid.get((ci+di,cj+dj),[]):
                    la,lo=nodes[i]; dist=hav(lat,lon,la,lo)
                    if dist<=rad: arr.append((dist,i))
        if arr: return sorted(arr)
    cand=[]
    for radius in range(1,18):
        for di in range(-radius,radius+1):
            for dj in range(-radius,radius+1):
                for i in grid.get((ci+di,cj+dj),[]):
                    la,lo=nodes[i]; cand.append((hav(lat,lon,la,lo),i))
        if len(cand)>=FALLBACK_N: break
    return sorted(cand)[:FALLBACK_N]
def dijkstra(seed_stops, mode_total=True):
    best={}; pq=[]
    for st in seed_stops:
        for dist,i in closest(st['lat'],st['lon']):
            walk=dist; total=st['transport']+dist/WALK_SPEED
            metric=total if mode_total else walk
            if i not in best or metric<best[i]['metric']:
                best[i]=dict(metric=metric,stop=st,walk=walk,total=total)
                heapq.heappush(pq,(metric,i,st['id'],walk,total,st))
    while pq:
        metric,i,_,walk,total,st=heapq.heappop(pq)
        cur=best.get(i)
        if cur is None or metric>cur['metric']+1e-6: continue
        for j,dist in edges.get(i,[]):
            nw=walk+dist
            if nw>WALK_RADIUS: continue
            nt=st['transport']+nw/WALK_SPEED
            nm=nt if mode_total else nw
            if j not in best or nm<best[j]['metric']:
                best[j]=dict(metric=nm,stop=st,walk=nw,total=nt)
                heapq.heappush(pq,(nm,j,st['id'],nw,nt,st))
    return best
all_total=dijkstra(stops, True)
origin_walk=dijkstra([s for s in stops if s['id'] in origins], False)
voshod=[s for s in stops if s['name']=='Восход' or 'Восход' in s['name']]
print('VOSHOD STOPS')
for s in voshod: print(s)
print('\nNODES WITHIN 250M EUCLIDEAN OF VOSHOD STOP(S)')
seen=[]
for s in voshod:
    for i,(lat,lon) in enumerate(nodes):
        if hav(s['lat'],s['lon'],lat,lon)<=250:
            seen.append((hav(s['lat'],s['lon'],lat,lon),i,lat,lon))
seen=sorted(seen)[:25]
def fmt(r):
    if not r: return 'none'
    st=r['stop']; return f"{st['id']} {st['name']} transport={st['transport']/60:.1f} walk={r['walk']/WALK_SPEED/60:.1f} total={r['total']/60:.1f} walkm={r['walk']:.0f}"
for dist,i,lat,lon in seen:
    rt=all_total.get(i); ow=origin_walk.get(i)
    direct=(ow['walk']/WALK_SPEED/60) if ow else None
    print(f'{lat:.7f} {lon:.7f} distToVoshod={dist:.0f}m | totalBest {fmt(rt)} | directWalkFromTasma={direct:.1f} min' if direct is not None else f'{lat:.7f} {lon:.7f} distToVoshod={dist:.0f}m | totalBest {fmt(rt)} | directWalkFromTasma=none')
