
"""Extra analytics helpers for IRRExplorer API.

These helpers build on top of the existing fetch_* primitives in
tools.irrexplorer.irrexplorer_aux.
"""
from datetime import datetime, timezone
from collections import Counter
from typing import Dict, List, Tuple
from tools.irrexplorer.irrexplorer_aux import (
    fetch_asn_data, fetch_ip_data, fetch_as_set_data, fetch_route_set_data
)

# ------------------------------------------------------------------ #
# 1. Route object freshness (days since last-modified)
# ------------------------------------------------------------------ #
def route_freshness_days(asn:str)->List[Tuple[str,int]]:
    data = fetch_asn_data(str(asn), query_type="prefix")
    results=[]
    for p in data['directOrigin']:
        lm=p['messages'][0]['lastModified'] if p['messages'] else None
        if not lm:
            continue
        dt=datetime.strptime(lm, "%Y-%m-%dT%H:%M:%S%z")
        age=(datetime.now(timezone.utc)-dt).days
        results.append((p['prefix'], age))
    return results

# ------------------------------------------------------------------ #
# 2. IRR vs RPKI mismatch ratio
# ------------------------------------------------------------------ #
def irr_rpki_mismatch_ratio(asn:str)->float:
    data=fetch_asn_data(str(asn),query_type="prefix")
    total=len(data['directOrigin'])
    mismatch=sum(1 for p in data['directOrigin']
                 if p['rpkiRoutes'] and p['rpkiRoutes'][0]['rpkiStatus']!='VALID')
    return mismatch/total if total else 0.0

# ------------------------------------------------------------------ #
# 3. Prefix length distribution
# ------------------------------------------------------------------ #
def prefix_length_hist(asn:str)->Dict[int,int]:
    c=Counter()
    data=fetch_asn_data(str(asn),query_type="prefix")
    for p in data['directOrigin']:
        plen=int(p['prefix'].split('/')[1])
        c[plen]+=1
    return dict(c)

# ------------------------------------------------------------------ #
# 4. Registry distribution (which IRR)
# ------------------------------------------------------------------ #
def registry_distribution(asn:str)->Dict[str,int]:
    c=Counter()
    data=fetch_asn_data(str(asn),query_type="prefix")
    for p in data['directOrigin']:
        for irr in p['irrRoutes']:
            c[irr]+=1
    return dict(c)

# ------------------------------------------------------------------ #
# 5. Hygiene score (simple heuristic)
# ------------------------------------------------------------------ #
def asn_hygiene_score(asn:str)->float:
    mismatch = irr_rpki_mismatch_ratio(asn)
    fresh    = route_freshness_days(asn)
    old      = sum(1 for _,age in fresh if age>365)
    total    = len(fresh) or 1
    score = 1 - (0.5*mismatch + 0.5*old/total)
    return round(max(score,0.0),3)

# ------------------------------------------------------------------ #
# 6. AS-SET overlap checker
# ------------------------------------------------------------------ #
def as_set_overlap(as_set1:str, as_set2:str)->Dict[str,List[str]]:
    m1=set(fetch_as_set_data(as_set1)[0]['members'])
    m2=set(fetch_as_set_data(as_set2)[0]['members'])
    return {
        "only_first": sorted(m1-m2),
        "only_second": sorted(m2-m1),
        "intersection": sorted(m1&m2)
    }
