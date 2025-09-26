import bz2
import io
import pathlib
import urllib.request

class AsRelIndex:
    def __init__(self):
        self.p2c = {}   
        self.c2p = {}   
        self.p2p = set()

    def add_p2c(self, provider:int, customer:int):
        self.p2c.setdefault(customer, set()).add(provider)
        self.c2p.setdefault(provider, set()).add(customer)

    def add_p2p(self, a:int, b:int):
        self.p2p.add(frozenset((a,b)))

    def get_relationship(self, a:int, b:int):
        # a->customers contains b  => a is provider of b
        if a in self.c2p and b in self.c2p[a]:
            return {"relationship": "p2c", "provider": a, "customer": b}
        # b->customers contains a  => b is provider of a
        if b in self.c2p and a in self.c2p[b]:
            return {"relationship": "p2c", "provider": b, "customer": a}
        if frozenset((a, b)) in self.p2p:
            return {"relationship": "p2p"}
        return {"relationship": "unknown"}

def load_asrel2(url_or_path:str) -> AsRelIndex:
    idx = AsRelIndex()
    # load either local .bz2 or remote .bz2
    data: bytes
    if url_or_path.startswith("http"):
        req = urllib.request.Request(url_or_path, headers={"User-Agent":"python-urllib/3"})
        with urllib.request.urlopen(req) as resp:
            data = resp.read()
    else:
        data = pathlib.Path(url_or_path).read_bytes()
    text = bz2.decompress(data).decode("utf-8", errors="replace")
    for line in io.StringIO(text):
        if not line or line[0] == '#':  # comments/metadata
            continue
        parts = line.strip().split('|')
        if len(parts) < 3: 
            continue
        a, b, rel = int(parts[0]), int(parts[1]), int(parts[2])
        if rel == -1:         # provider|customer|-1
            idx.add_p2c(provider=a, customer=b)
        elif rel == 0:        # peer|peer|0(|source)
            idx.add_p2p(a, b)
    return idx

def init_asrel_tool(snapshot_tag:str):
    """snapshot_tag example: '20250801' (YYYYMM01)."""
    _ASREL_IDX = None
    _ASREL_SNAPSHOT = None
    _ASREL_URL = None
    base = "https://publicdata.caida.org/datasets/as-relationships/serial-2/"
    _ASREL_URL = f"{base}{snapshot_tag}.as-rel2.txt.bz2"
    _ASREL_IDX = load_asrel2(_ASREL_URL)
    _ASREL_SNAPSHOT = snapshot_tag
    return _ASREL_IDX

def get_tor(asn1, asn2):
    asn_1 = int(asn1)
    asn_2 = int(asn2)
    # Initialize once per process with the monthly snapshot tag (YYYYMM01)
    _ASREL_IDX = init_asrel_tool("20250801")  # points to .../20250801.as-rel2.txt.bz2
    if _ASREL_IDX is None:
        raise RuntimeError("AS-REL tool not initialized. Call init_asrel_tool(...) first.")
    res = _ASREL_IDX.get_relationship(asn_1, asn_2)
    # Query both directions (the tool returns the correct provider/customer side)
    return res
    