import re, json
text=open('tmp_navi_bundle.js',encoding='utf-8',errors='replace').read()
patterns=[
 r'https?://[^"\'`<> ]+',
 r'wss?://[^"\'`<> ]+',
 r'\b[\w./-]+\.(?:php|json|ashx|asmx|svc|aspx)\b(?:\?[^"\'`<> ]*)?',
 r'\b(?:ajax|api|transport|vehicle|route|monitor|gps|markers|marsh|rasp|stop)[A-Za-z0-9_./?-]{0,120}',
]
vals=[]
for p in patterns:
    vals += re.findall(p,text,re.I)
seen=[]
for v in vals:
    v=v.strip('\\')
    if v not in seen:
        seen.append(v)
for v in seen[:500]: print(v)
print('COUNT',len(seen))
