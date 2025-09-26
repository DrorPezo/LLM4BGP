# import json
# import random
# from pathlib import Path
# import requests
# import ipaddress
# import pybgpstream
# from neo4j import GraphDatabase, RoutingControl 
# import neo4j
# from typing import List, Dict, Set, Any, Tuple, Iterable
# from collections import defaultdict
# from datetime import datetime, timedelta
# import re
# import bz2
# from tqdm.auto import tqdm 
# from datetime import datetime
# from tools.asrank.as_rank_aux import *
# from tools.as2org.as2org_aux import *
# from tools.iyp.iyp_aux import *
# from tools.peeringdb.peeringdb_aux import *

import pybgpstream
import networkx as nx
from itertools import groupby
import time
import json
import pickle


def sample_collectors(
    from_time: str,
    until_time: str,
    rows_per_collector: int | None = 10,
) -> None:
    """
    Fetch and print a sample of BGP routes from each collector in the list.
    
    Parameters:
        from_time (str): The start of the time window (UTC), e.g. "2024-08-01 00:00:00".
        until_time (str): The end of the time window (UTC).
        collectors (list): A list of collector names (e.g. ["rrc00", "route-views.eqix"]).
        rows_per_collector (int|None): Max number of routes to print per collector.
            Use None for no limit.
    """
    # Example usage:
    collectors = [
        "route-views2",
        "route-views.eqix",
        "route-views.chicago",
        "route-views.linx",
        "route-views.sydney",
        "route-views.saopaulo",
        "rrc00",
        "rrc01",
        "rrc03",
        "rrc04",
        "rrc06",
        "rrc10",
        "rrc11",
        "rrc14",
        "rrc19",
    ]
    for col in collectors:
        print(f"\n--- {col} ---")
        stream = pybgpstream.BGPStream(
            from_time=from_time,
            until_time=until_time,
            collectors=[col],
            record_type="ribs",
            data_interface="broker",
        )
        row_count = 0
        for rec in stream.records():
            for elem in rec:
                pfx = elem.fields.get("prefix")
                as_path = elem.fields.get("as-path")
                print(f"{pfx} | {as_path}")
                row_count += 1
                if rows_per_collector is not None and row_count >= rows_per_collector:
                    break
            if rows_per_collector is not None and row_count >= rows_per_collector:
                break
        print("Rows total:", row_count)

def build_as_graph(
    from_time: str,
    until_time: str,
    max_rows_per_collector: int | None = None,
) -> nx.Graph:

    collectors = [
        # Route Views collectors
        "route-views2", "route-views2.saopaulo", "route-views3", "route-views4",
        "route-views5", "route-views6",
        "route-views.amsix", "route-views.bdix", "route-views.bknix",
        "route-views.chicago", "route-views.chile", "route-views.eqix",
        "route-views.flix", "route-views.fortaleza", "route-views.gixa",
        "route-views.gorex", "route-views.isc", "route-views.jinx",
        "route-views.kixp", "route-views.linx", "route-views.mwix",
        "route-views.napafrica", "route-views.nwax", "route-views.ny",
        "route-views.perth", "route-views.peru", "route-views.phoix",
        "route-views.rio", "route-views.saopaulo", "route-views.sfmix",
        "route-views.sg", "route-views.siex", "route-views.soxrs",
        "route-views.sydney", "route-views.telxatl", "route-views.uaeix",
        "route-views.wide",
        # RIPE RIS collectors (there is no rrc17)
        "rrc00", "rrc01", "rrc02", "rrc03", "rrc04", "rrc05", "rrc06",
        "rrc07", "rrc08", "rrc09", "rrc10", "rrc11", "rrc12", "rrc13",
        "rrc14", "rrc15", "rrc16", "rrc18", "rrc19", "rrc20", "rrc21",
        "rrc22", "rrc23", "rrc24", "rrc25", "rrc26"
    ]
    G = nx.Graph()
    for col in collectors:
        row_count = 0
        stream = pybgpstream.BGPStream(
            from_time=from_time,
            until_time=until_time,
            collectors=[col],
            record_type="ribs",
            data_interface="broker",
        )
        for rec in stream.records():
            for elem in rec:
                # Split and de-duplicate AS path (remove repeated prepends):contentReference[oaicite:1]{index=1}
                as_path = [k for k, _ in groupby(elem.fields.get("as-path", "").split())]
                if len(as_path) < 2:
                    continue
                # Add edges for consecutive ASNs
                for a, b in zip(as_path, as_path[1:]):
                    if a != b:
                        G.add_edge(a, b)
                row_count += 1
                if max_rows_per_collector is not None and row_count >= max_rows_per_collector:
                    break
            if max_rows_per_collector is not None and row_count >= max_rows_per_collector:
                break
    return G

def group_routes_by_start_asn(
    from_time: str,
    until_time: str,
    max_rows_per_collector: int | None = None,
) -> dict[str, list[list[str]]]:

    collectors = [
        # Route Views collectors
        "route-views2", "route-views2.saopaulo", "route-views3", "route-views4",
        "route-views5", "route-views6",
        "route-views.amsix", "route-views.bdix", "route-views.bknix",
        "route-views.chicago", "route-views.chile", "route-views.eqix",
        "route-views.flix", "route-views.fortaleza", "route-views.gixa",
        "route-views.gorex", "route-views.isc", "route-views.jinx",
        "route-views.kixp", "route-views.linx", "route-views.mwix",
        "route-views.napafrica", "route-views.nwax", "route-views.ny",
        "route-views.perth", "route-views.peru", "route-views.phoix",
        "route-views.rio", "route-views.saopaulo", "route-views.sfmix",
        "route-views.sg", "route-views.siex", "route-views.soxrs",
        "route-views.sydney", "route-views.telxatl", "route-views.uaeix",
        "route-views.wide",
        # RIPE RIS collectors (there is no rrc17)
        "rrc00", "rrc01", "rrc02", "rrc03", "rrc04", "rrc05", "rrc06",
        "rrc07", "rrc08", "rrc09", "rrc10", "rrc11", "rrc12", "rrc13",
        "rrc14", "rrc15", "rrc16", "rrc18", "rrc19", "rrc20", "rrc21",
        "rrc22", "rrc23", "rrc24", "rrc25", "rrc26"
    ]
    routes_by_asn: dict[str, list[list[str]]] = {}
    for col in collectors:
        row_count = 0
        stream = pybgpstream.BGPStream(
            from_time=from_time,
            until_time=until_time,
            collectors=[col],
            record_type="ribs",
            data_interface="broker",
        )
        for rec in stream.records():
            for elem in rec:
                # Get the AS path string and split it into a list of ASNs:contentReference[oaicite:1]{index=1}
                as_path_str = elem.fields.get("as-path", "")
                if not as_path_str:
                    continue
                asns = as_path_str.split()
                if not asns:
                    continue
                first_asn = asns[0]
                # Append the entire path (including any prepends) to this ASN's list
                routes_by_asn.setdefault(first_asn, []).append(asns)
                row_count += 1
                if max_rows_per_collector is not None and row_count >= max_rows_per_collector:
                    break
            if max_rows_per_collector is not None and row_count >= max_rows_per_collector:
                break
    return routes_by_asn

def group_routes_by_asn(
    from_time: str,
    until_time: str,
    max_rows_per_collector: int | None = None,
) -> dict[str, list[list[str]]]:
    collectors = [
        # Route Views collectors
        "route-views2", "route-views2.saopaulo", "route-views3", "route-views4",
        "route-views5", "route-views6",
        "route-views.amsix", "route-views.bdix", "route-views.bknix",
        "route-views.chicago", "route-views.chile", "route-views.eqix",
        "route-views.flix", "route-views.fortaleza", "route-views.gixa",
        "route-views.gorex", "route-views.isc", "route-views.jinx",
        "route-views.kixp", "route-views.linx", "route-views.mwix",
        "route-views.napafrica", "route-views.nwax", "route-views.ny",
        "route-views.perth", "route-views.peru", "route-views.phoix",
        "route-views.rio", "route-views.saopaulo", "route-views.sfmix",
        "route-views.sg", "route-views.siex", "route-views.soxrs",
        "route-views.sydney", "route-views.telxatl", "route-views.uaeix",
        "route-views.wide",
        # RIPE RIS collectors (there is no rrc17)
        "rrc00", "rrc01", "rrc02", "rrc03", "rrc04", "rrc05", "rrc06",
        "rrc07", "rrc08", "rrc09", "rrc10", "rrc11", "rrc12", "rrc13",
        "rrc14", "rrc15", "rrc16", "rrc18", "rrc19", "rrc20", "rrc21",
        "rrc22", "rrc23", "rrc24", "rrc25", "rrc26"
    ]
    routes_by_asn: dict[str, list[list[str]]] = {}
    for col in collectors:
        row_count = 0
        stream = pybgpstream.BGPStream(
            from_time=from_time,
            until_time=until_time,
            collectors=[col],
            record_type="ribs",
            data_interface="broker",
        )
        for rec in stream.records():
            for elem in rec:
                as_path_str = elem.fields.get("as-path", "")
                if not as_path_str:
                    continue
                asns = as_path_str.split()
                if not asns:
                    continue
                # For each ASN in the path, append the full path to that ASN's list
                for asn in asns:
                    routes_by_asn.setdefault(asn, []).append(asns)
                row_count += 1
                if max_rows_per_collector is not None and row_count >= max_rows_per_collector:
                    break
            if max_rows_per_collector is not None and row_count >= max_rows_per_collector:
                break
    return routes_by_asn


# caida_dataset_path = 'datasets/caida/caida_dataset.json'
# Relation = Tuple[str, str, str]  # (AS1, AS2, Rel)

# REL_MAP = {
#     'S': 'Siblings',
#     1: 'Siblings',
#     '1': 'Siblings',
#     0: 'Peers',
#     '0': 'Peers',
#     -1: 'Provider to Customer',
#     '-1': 'Provider to Customer',
# }

# URI  = "neo4j://iyp-bolt.ihr.live:7687"
# AUTH = ("neo4j", "password")
# DB   = "neo4j"

# db = GraphDatabase.driver(URI, auth=AUTH)

# # Valley-Free Analysis
# def collect_paths(asn, limit, project, collectors, record_type, hours):
#     until = datetime.utcnow()
#     since = until - timedelta(hours=5*hours)
#     seen = set()
#     out = []
#     for collector in collectors:
#         stream = pybgpstream.BGPStream(
#             project=project,
#             collectors=[collector],
#             record_type=record_type,
#             # from_time=since.strftime("%Y-%m-%d %H:%M:%S"),
#             # until_time=until.strftime("%Y-%m-%d %H:%M:%S UTC"),
#             from_time="2025-08-03 00:00:00",
#             until_time="2025-08-03 00:10:00 UTC",
#             filter=f"aspath _{asn}_"
#         )
#         cnt = 0
#         for elem in stream:
#             # print(elem)
#             path = elem.fields.get("as-path")
#             if not path:
#                 continue
#             if path not in seen:
#                 seen.add(path)
#                 out.append(path)
#                 cnt += 1
#                 if len(out) >= limit:
#                     return out
#     return out

# def get_paths():
#     TARGET_ASNS = [174, 3356, 6810, 8551, 9121, 176, 4657]
#     rv_collectors=("route-views2","route-views.eqix","route-views.linx", "route-views.sg")
#     updates_hours=3
#     ribs_hours=24
#     limit = 15
#     paths = []
#     for asn in TARGET_ASNS:
#         asn_paths = collect_paths(asn, limit, "routeviews", rv_collectors, "updates", updates_hours)
#         paths += asn_paths
#     return paths

# def load_relationships(rel_file: str):
#     """Parse CAIDA as-rel2 text file into a direction-sensitive map."""
#     rel= {}
#     with open(rel_file, "r", encoding="utf-8") as fh:
#         for line in fh:
#             if line.startswith('#'):
#                 continue
#             a, b, r, _ = line.split('|')
#             a, b, r = int(a), int(b), int(r)
#             if r == -1:                        # provider → customer
#                 rel[(a, b)], rel[(b, a)] = -1, +1
#             else:                              # peer
#                 rel[(a, b)] = rel[(b, a)] = 0
#     return rel

# def is_valley_free(path, rel_map):
#     # Define states: 0 (uphill), 1 (flat), 2 (downhill)
#     state = 0
#     for i in range(len(path) - 1):
#         a, b = path[i], path[i+1]
#         rel = rel_map.get((a, b))
#         if rel is None:
#             continue
#         if state == 0:  # uphill
#             if rel == -1:  # downhill starts
#                 state = 2
#             elif rel == 0:  # peer relationship
#                 state = 1
#         elif state == 1:  # flat (peering)
#             if rel == 1:  # invalid transition back to uphill
#                 return False
#             elif rel == -1:  # downhill allowed from flat
#                 state = 2
#         elif state == 2:  # downhill
#             if rel != -1:  # only downhill allowed after downhill
#                 return False
#     return True

# def generate_valley_free_dataset(paths, rel_map, total_paths=50):
#     labeled_paths = []
#     random.shuffle(paths)
#     count_vf = count_non_vf = 0

#     for path_str in paths:
#         if len(labeled_paths) >= total_paths:
#             break
#         path = [int(asn) for asn in path_str.split()]
#         vf = is_valley_free(path, rel_map)
#         labeled_paths.append((path_str, vf))
#         if vf:
#             count_vf += 1
#         else:
#             count_non_vf += 1

#     print(f"Generated {len(labeled_paths)} paths: {count_vf} Valley-Free, {count_non_vf} Non-Valley-Free")
#     return labeled_paths

# def filter_valley_free_only(labeled_paths):
#     return [path for path, is_vf in labeled_paths if is_vf]

# def build_reverse_rel_map(rel_map):
#     """Builds a map from each AS to its neighbors with labeled relationships."""
#     neighbor_map = defaultdict(list)
#     for (a, b), rel in rel_map.items():
#         neighbor_map[a].append((b, rel))
#     return neighbor_map

# def build_non_valley_free_path(neighbor_map, max_len=5):
#     """Builds a single synthetic non-Valley-Free path."""
#     for _ in range(100):  # attempt up to 100 times
#         path = []
#         current = random.choice(list(neighbor_map.keys()))
#         path.append(current)
#         state = 0  # 0 = uphill, 1 = flat, 2 = downhill
#         for _ in range(max_len - 1):
#             neighbors = neighbor_map.get(current, [])
#             if not neighbors:
#                 break
#             random.shuffle(neighbors)
#             for neighbor, rel in neighbors:
#                 if state == 0 and rel == 1:
#                     path.append(neighbor)
#                     current = neighbor
#                     continue
#                 elif state == 0 and rel == 0:
#                     state = 1
#                     path.append(neighbor)
#                     current = neighbor
#                     continue
#                 elif state == 1 and rel == 1:
#                     path.append(neighbor)  # Invalid transition
#                     return path
#                 elif state == 1 and rel == -1:
#                     state = 2
#                     path.append(neighbor)
#                     current = neighbor
#                     continue
#                 elif state == 2 and rel != -1:
#                     path.append(neighbor)  # Invalid transition
#                     return path
#                 elif state == 2 and rel == -1:
#                     path.append(neighbor)
#                     current = neighbor
#                     continue
#             break
#     return None

# def generate_synthetic_non_valley_free_paths(rel_map, total_paths=50):
#     neighbor_map = build_reverse_rel_map(rel_map)
#     paths = []
#     while len(paths) < total_paths:
#         p = build_non_valley_free_path(neighbor_map)
#         if p and not is_valley_free(p, rel_map):
#             paths.append(" ".join(map(str, p)))
#     return paths

# def label_paths(vf_paths, non_vf_paths):
#     labeled = []
#     for p in vf_paths:
#         labeled.append({"path": p, "label": "VF"})
#     for p in non_vf_paths:
#         labeled.append({"path": p, "label": "non-VF"})
#     return labeled

# def create_vf_q_a_dataset(labeled_dataset):
#     # Split VF and non-VF paths
#     vf_paths = [item for item in labeled_dataset if item["label"] == "VF"]
#     non_vf_paths = [item for item in labeled_dataset if item["label"] == "non-VF"]

#     # Sample 25 from each
#     sampled_vf = random.sample(vf_paths, 25)
#     sampled_non_vf = random.sample(non_vf_paths, 25)
#     combined = sampled_vf + sampled_non_vf
#     random.shuffle(combined)

#     # Generate Q&A
#     vf_qas = []
#     for item in combined:
#         path = item["path"]
#         label = item["label"]
#         question = f"Is the following path valley-free: {path}?"
#         answer = (
#             f"The path {path} is valid and valley free"
#             if label == "VF"
#             else f"The path {path} is invalid and not a valley free path"
#         )
#         vf_qas.append({"question": question, "answer": answer})

#     # Save to file
#     vf_qas_file = Path("datasets/valley_free/vf_qas.json")
#     with vf_qas_file.open("w", encoding="utf-8") as f:
#         json.dump(vf_qas, f, indent=2)

#     print(f"Generated {len(vf_qas)} valley-free Q&A pairs → {vf_qas_file}")

# def create_vf_inference_q_a_dataset(dataset_size, as_rel_data_path):
#     rel_map = load_relationships(as_rel_data_path)
#     paths = get_paths()
#     dataset = generate_valley_free_dataset(paths, rel_map, total_paths=dataset_size)
#     # Example usage
#     valley_free_paths = filter_valley_free_only(dataset)
#     synthetic_non_vf_paths = generate_synthetic_non_valley_free_paths(rel_map, total_paths=dataset_size)
#     # Example usage
#     labeled_dataset = label_paths(valley_free_paths, synthetic_non_vf_paths)
#     create_vf_q_a_dataset(labeled_dataset)
