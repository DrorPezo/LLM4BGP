import json
from random import Random
from pathlib import Path
from typing import Any, Union, List, Optional
import networkx as nx 


# Check if a certain path is the shortest path between two ASNs
def is_shortest_path(source, target, path):
    """Check if a given path is the shortest path between source and target in graph G."""
    as_graph = load_graph_from_pickle('datasets/routes/as_graph.pkl')
    try:
        shortest_length = nx.shortest_path_length(as_graph, source, target)
        return len(path) - 1 == shortest_length
    except nx.NetworkXNoPath:
        return False

        # Find all paths with exact length in the AS graph
def paths_with_exact_length(source, target, length):
    """
    Returns all simple paths in an undirected graph G from source to target
    that have exactly `length` edges.
    """
    as_graph = load_graph_from_pickle('datasets/routes/as_graph.pkl')
    all_paths = nx.all_simple_paths(as_graph, source, target, cutoff=length)
    return [path for path in all_paths if len(path) - 1 == length]

def as_degree_from_as_graph(asn):
    '''
    Returns the degree of an ASN from BGPStream indirected AS graph.
    Input: asn (int | str)
    Output: degree (int)
    '''
    as_graph = load_graph_from_pickle('datasets/routes/as_graph.pkl')
    return as_graph.degree(str(asn))

def read_asn_json(asn: Union[int, str], base_dir: str = "tools/routes/routes") -> Any:
    asn_str = str(asn).strip()
    if asn_str.lower().startswith("as"):
        asn_str = asn_str[2:]
    if not asn_str.isdigit():
        raise ValueError(f"ASN must be numeric (got {asn!r}).")

    path = Path(base_dir) / f"{asn_str}.json"
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path.resolve()}")

    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON in {path}: {e}") from e

def _norm_asn(a: Union[int, str]) -> str:
    s = str(a).strip()
    if s.lower().startswith("as"):
        s = s[2:]
    if not s.isdigit():
        raise ValueError(f"ASN must be numeric (got {a!r}).")
    return s

def find_routes_for_asns(x: Union[int, str],
                         y: Union[int, str],
                         base_dir: str = "tools/routes/routes",
                         unique: bool = True) -> List[list]:
    x_str, y_str = _norm_asn(x), _norm_asn(y)
    path = Path(base_dir) / f"{x_str}.json"

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path.resolve()}")

    routes = json.loads(path.read_text(encoding="utf-8"))
    out, seen = [], set()

    for route in routes:
        route = [str(a) for a in route]
        n = len(route)
        # Check if x is adjacent to y anywhere in the path
        match = any(
            (route[i] == x_str and ((i > 0 and route[i-1] == y_str) or (i+1 < n and route[i+1] == y_str)))
            for i in range(n)
        )
        if match:
            if unique:
                key = tuple(route)
                if key not in seen:
                    seen.add(key)
                    out.append(route)
            else:
                out.append(route)
    return out

def choose_random_routes(asn1, asn2, m: int = 100, seed: Optional[int] = None) -> List[List[str]]:
    adj = find_routes_for_asns(asn1, asn2)
    rnd = Random(seed)
    n = len(adj)
    if n == 0:
        return []
    k = min(m, n)
    if k == n:
        out = list(adj)
        rnd.shuffle(out)
        return out
    return rnd.sample(list(adj), k)
