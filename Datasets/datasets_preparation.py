import json
import random
from pathlib import Path
import requests
import ipaddress
from neo4j import GraphDatabase, RoutingControl 
import neo4j
from typing import List, Dict, Set, Any, Tuple, Iterable
from collections import defaultdict
from datetime import datetime, timedelta
import urllib.request
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import io
import re
import bz2
from tqdm.auto import tqdm 
from datetime import datetime
from tools.caida.caida_tools import *
from tools.peeringdb.peeringdb_aux import *


caida_dataset_path = 'datasets/caida/caida_dataset.json'
Relation = Tuple[str, str, str]  # (AS1, AS2, Rel)

REL_MAP = {
    'S': 'Siblings',
    1: 'Siblings',
    '1': 'Siblings',
    0: 'Peers',
    '0': 'Peers',
    -1: 'Provider to Customer',
    '-1': 'Provider to Customer',
}

URI  = "neo4j://iyp-bolt.ihr.live:7687"
AUTH = ("neo4j", "password")
DB   = "neo4j"

db = GraphDatabase.driver(URI, auth=AUTH)

# CAIDA
def create_caida_dataset():
    with open(caida_dataset_path, 'r') as f:
        data = json.load(f)

    asns = list(data.keys())
    caida_data = {}
    for asn in asns:
        as_data = get_as_rank_data(int(asn))
        # Check if as_data and nested keys are present before accessing them
        if as_data and as_data.get('data') and as_data['data'].get('asn'):
            rank = as_data['data']['asn'].get('rank')
            cone = as_data['data']['asn'].get('cone', {}).get('numberAsns') # Also handle potential missing 'cone'
            if rank and cone:
                caida_data[asn] = {
                    'rank': rank,
                    'cone': cone
                }
    return caida_data

def create_qas_datasets_caida(asn_data):
    as_cone_query = lambda asn: f"What is the size of the customer cone of AS{asn}?"
    as_rank_query = lambda asn: f"What is the rank of AS{asn}?"  

    rank_qas = [
      {
          "question": as_rank_query(asn),
          "answer": f"The rank of AS{asn} is: {details['rank']}",
      }
      for asn, details in asn_data.items()
    ]

    cone_qas = [
        {
            "question": as_cone_query(asn),
            "answer": f"The customer cone size of AS{asn} is: {details['cone']}",
        }
        for asn, details in asn_data.items()
    ]
    rank_file = Path("datasets/caida/as_rank_qas.json")
    cone_file = Path("datasets/caida/as_cone_qas.json")

    with rank_file.open("w", encoding="utf-8") as f:
        json.dump(rank_qas, f, indent=2)

    with cone_file.open("w", encoding="utf-8") as f:
        json.dump(cone_qas, f, indent=2)

    print(f"Generated {len(rank_qas)} rank Q&A pairs → {rank_file}, {datetime.now()}")
    print(f"Generated {len(cone_qas)} cone Q&A pairs → {cone_file}, {datetime.now()}")

def prepare_caida_datasets(dataset_size):
    caida_data = create_caida_dataset()
    asn_data = dict(list(caida_data .items())[:dataset_size])
    create_qas_datasets_caida(asn_data)

# Bogons
def fetch_and_validate_ipv4_bogons(url: str) -> List[str]:
    response = requests.get(url)
    response.raise_for_status()

    lines = response.text.strip().splitlines()
    valid_prefixes = []

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            ipaddress.IPv4Network(line, strict=False)
            valid_prefixes.append(line)
        except ValueError:
            print(f"[!] Invalid IPv4 prefix skipped: {line}")

    return valid_prefixes

def fetch_and_validate_ipv6_bogons(url: str) -> List[str]:
    response = requests.get(url)
    response.raise_for_status()

    lines = response.text.strip().splitlines()
    valid_prefixes = []

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            ipaddress.IPv6Network(line, strict=False)
            valid_prefixes.append(line)
        except ValueError:
            print(f"[!] Invalid IPv6 prefix skipped: {line}")

    return valid_prefixes

def generate_random_non_bogon_ipv4_prefix(bogon_prefixes: Set[ipaddress.IPv4Network]) -> str:
    while True:
        # Choose a random prefix length (between /8 and /30 for reasonable subnets)
        prefix_length = random.randint(8, 30)

        # Generate a random base IP address aligned to the prefix
        max_prefix_value = 2**(32 - prefix_length)
        base_ip_int = random.randint(0, (2**32 - 1) // max_prefix_value) * max_prefix_value
        network = ipaddress.IPv4Network((base_ip_int, prefix_length))

        # Check if the prefix overlaps with any bogon
        if any(network.subnet_of(bogon) or bogon.subnet_of(network) for bogon in bogon_prefixes):
            continue  # Try again

        return str(network)

def generate_random_non_bogon_ipv6_prefix(bogon_prefixes: Set[ipaddress.IPv6Network]) -> str:
    while True:
        # Choose a random prefix length (e.g. /32 to /64 are common for global routing)
        prefix_length = random.randint(32, 64)

        # Generate a base address aligned with the prefix length
        num_host_bits = 128 - prefix_length
        base_int = (random.getrandbits(prefix_length) << num_host_bits)
        network = ipaddress.IPv6Network((base_int, prefix_length))

        # Ensure no overlap with any bogon
        if any(network.subnet_of(bogon) or bogon.subnet_of(network) for bogon in bogon_prefixes):
            continue

        return str(network)

def create_ipv4_bogons_dataset(num_of_address_samples):
    ipv4_url = "https://www.team-cymru.org/Services/Bogons/fullbogons-ipv4.txt"
    ipv4_bogons = fetch_and_validate_ipv4_bogons(ipv4_url)
    bogon_prefixes_set_ipv4 = set(ipaddress.IPv4Network(p) for p in ipv4_bogons)
    # clean_ipv4 = generate_random_non_bogon_ipv4_prefix(bogon_prefixes_set_ipv4)
    # print(f"Random non-bogon IP: {clean_ipv4}")
    bogons_dataset = dict()

    for i in range(num_of_address_samples):
        non_bogon_ipv4 = generate_random_non_bogon_ipv4_prefix(bogon_prefixes_set_ipv4)
        bogons_dataset[str(non_bogon_ipv4)] = 'Non-Bogon'
        bogon_ipv4 = random.choice(list(bogon_prefixes_set_ipv4))
        bogons_dataset[str(bogon_ipv4)] = 'Bogon'
    
    bogons_dataset_items = list(bogons_dataset.items())
    random.shuffle(bogons_dataset_items)
    bogons_dataset = dict(bogons_dataset_items)
    return bogons_dataset

def create_ipv6_bogons_dataset(num_of_address_samples):
    ipv6_url = "https://www.team-cymru.org/Services/Bogons/fullbogons-ipv6.txt"
    ipv6_bogons = fetch_and_validate_ipv6_bogons(ipv6_url)
    bogon_prefixes_set_ipv6 = set(ipaddress.IPv6Network(p) for p in ipv6_bogons)
    # clean_ipv6 = generate_random_non_bogon_ipv6_prefix(bogon_prefixes_set_ipv6)
    # print(f"Random non-bogon IPv6: {clean_ipv6}")
    bogons_dataset = dict()

    for i in range(num_of_address_samples):
        non_bogon_ipv6 = generate_random_non_bogon_ipv6_prefix(bogon_prefixes_set_ipv6)
        bogons_dataset[str(non_bogon_ipv6)] = 'Non-Bogon'
        bogon_ipv6 = random.choice(list(bogon_prefixes_set_ipv6))
        bogons_dataset[str(bogon_ipv6)] = 'Bogon'

    bogons_dataset_items = list(bogons_dataset.items())
    random.shuffle(bogons_dataset_items)
    bogons_dataset = dict(bogons_dataset_items)
    return bogons_dataset

def create_ipv4_bogons_qas_dataset(num_of_address_samples):
    ipv4_bogons = create_ipv4_bogons_dataset(num_of_address_samples)
    bogons_data = dict(list(ipv4_bogons.items())[:num_of_address_samples])
    bogons_query = lambda prefix: f"Is the following IP Address a bogon:{prefix}?"
    bogons_qas = [
        {
            "question": bogons_query(prefix),
            "answer": f"The prefix {prefix} is {details.lower()}",
        }
        for prefix, details in bogons_data.items()
    ]
    bogons_file = Path("datasets/bogons/ipv4_bogons_qas.json")

    with bogons_file.open("w", encoding="utf-8") as f:
        json.dump(bogons_qas, f, indent=2)

    print(f"Generated {len(bogons_qas)} IPv4 bogons Q&A pairs → {bogons_file}, {datetime.now()}")

def create_ipv6_bogons_qas_dataset(num_of_address_samples):
    ipv6_bogons = create_ipv6_bogons_dataset(num_of_address_samples)
    bogons_data = dict(list(ipv6_bogons.items())[:num_of_address_samples])
    bogons_query = lambda prefix: f"Is the following IP Address a bogon:{prefix}?"
    bogons_qas = [
        {
            "question": bogons_query(prefix),
            "answer": f"The prefix {prefix} is {details.lower()}",
        }
        for prefix, details in bogons_data.items()
    ]
    bogons_file = Path("datasets/bogons/ipv6_bogons_qas.json")

    with bogons_file.open("w", encoding="utf-8") as f:
        json.dump(bogons_qas, f, indent=2)

    print(f"Generated {len(bogons_qas)} IPv6 Q&A pairs → {bogons_file}, {datetime.now()}")

# AS2Org 
def create_as2org_dataset(dataset_size):
    asns = 'datasets/caida/caida_dataset.json'

    largest_as = dict()
    as_count = dict()
    as2org = dict()

    with open(asns, 'r') as f:
        asns = json.load(f)
    orgs = set()

    for asn in random.sample(list(asns.keys()), dataset_size+10):
        organization = fetch_org(asn)
        print(f"ASN:{asn}, Organization:{organization}")
        if organization:
            orgs.add(organization)
            as2org[asn] = organization
    orgs = list(orgs)

    for organization in orgs:
        if organization:
            largest_asn = findLargestASN(organization)
            print(f"Organization:{organization}, Largest ASN: {largest_asn}")
            if largest_asn:
                largest_as[organization] = largest_asn
            as_count[organization] = len(current_as2org(organization))

    return dict(list(largest_as.items())[:50]), dict(list(as_count.items())[:50]), dict(list(as2org.items())[:50]) 

def prepare_orgs_dataset(dataset_size):
    largest_as2org_query = lambda org: f"What is ASN of the largest AS which {org} owns?"
    num_of_as2org_query = lambda org: f"What is the number of ASes which {org} owns?"
    as2org_query = lambda asn: f"Which organization owns AS{asn}?"

    largest_as, as_count, as2org = create_as2org_dataset(dataset_size)

    largest_as2org_qas = [
        {
            "question": largest_as2org_query(org),
            "answer": f"The largest AS in {org} is: {largest_as[org]}",
        }
        for org in largest_as
    ]

    num_of_as2org_qas = [
        {
            "question": num_of_as2org_query(org),
            "answer": f"The number of ASes in {org} is: {as_count[org]}",
        }
        for org in as_count
    ]

    as2org_qas = [
        {
            "question": as2org_query(asn),
            "answer": f"AS{asn} is owned by: {org}",
        }
        for asn, org in as2org.items()
    ]

    largest_as2orgs_file = Path("datasets/as2org/largest_as2org_qas.json")
    num_of_as2org_file = Path("datasets/as2org/num_of_as2org_qas.json")
    as2org_file = Path("datasets/as2org/as2org_qas.json")

    with largest_as2orgs_file.open("w", encoding="utf-8") as f:
        json.dump(largest_as2org_qas, f, indent=2)

    with num_of_as2org_file.open("w", encoding="utf-8") as f:
        json.dump(num_of_as2org_qas, f, indent=2)

    with as2org_file.open("w", encoding="utf-8") as f:
        json.dump(as2org_qas, f, indent=2)

    print(f"Generated {len(largest_as2org_qas)} largest AS in organization Q&A pairs → {largest_as2orgs_file}, {datetime.now()}")
    print(f"Generated {len(num_of_as2org_qas)} number of ASes in organization pairs → {num_of_as2org_file}, {datetime.now()}")
    print(f"Generated {len(as2org_qas)} AS2Org Q&A pairs → {as2org_file}, {datetime.now()}")

# AS Class Type
def pdb_as_type_info_type(asn):
    as_type = get_as_type_data(asn)
    return list(as_type['info_type'])

def map_as_class(as_type: str) -> str:
    key_raw = as_type.strip().lower()

    if key_raw.replace('-', ' ').replace('_', ' ').replace('/', ' ').strip() == "network services":
        return "Enterprise"

    if key_raw.replace('-', ' ').replace('_', ' ').replace('/', ' ').strip() == "non profit":
        return "Enterprise"

    if key_raw.replace('-', ' ').replace('_', ' ').replace('/', ' ').strip() == "route server":
        return "Enterprise"

    if key_raw.replace('-', ' ').replace('_', ' ').replace('/', ' ').strip() == "route collector":
        return "Enterprise"

    key = (
        key_raw.replace('-', ' ')
               .replace('_', ' ')
               .replace('/', ' ')
               .replace('\\', ' ')
    )
    key_parts = {part for part in key.split() if part}

    enterprise_keys = {
        "enterprise", "education", "research", "educationresearch",
        "edu", "school", "university", "academic",
        "networkservices", "nonprofit", "government",
        "notdisclosed", "routeserver", "routecollector"
    }

    transit_keys = {
        "cable", "dsl", "isp", "nsp", "transit", "access",
        "cabledsl", "cabledslisp", "cableisp"
    }

    content_keys = {"content", "cdn", "ott"}

    if key_parts & enterprise_keys:
        return "Enterprise"
    if key_parts & transit_keys:
        return "Transit / Access"
    if key_parts & content_keys:
        return "Content"

    raise ValueError(f"Unknown AS type: {as_type!r}")

def prepare_as_class_type_datasets(as_type_map: dict[str, set[str]]) -> None:
    as_class_type_question = lambda asn: f"What is the CAIDA AS type of {asn}?"
    caida_as_class_type_question = lambda asn: f"What is the AS type of {asn}?"
    out_dir = Path("datasets/as_class_type")

    caida_qas = [
        {
            "question": as_class_type_question(asn),
            "answer": f"The CAIDA AS type of {asn} is: {types[1]}",
        }
        for asn, types in as_type_map.items()
    ]

    plain_qas = [
        {
            "question": caida_as_class_type_question(asn),
            "answer": f"The AS type of {asn} is: {types[0]}",
        }
        for asn, types in as_type_map.items()
    ]

    caida_file = out_dir / "caida_as_class_type_qas.json"
    plain_file = out_dir / "as_class_type_qas.json"

    with caida_file.open("w", encoding="utf‑8") as f:
        json.dump(caida_qas, f, indent=2)
    with plain_file.open("w", encoding="utf‑8") as f:
        json.dump(plain_qas, f, indent=2)

    now = datetime.now().strftime("%Y‑%m‑%d %H:%M:%S")
    print(f"Generated {len(caida_qas)} CAIDA‑worded Q&As → {caida_file} | {now}")
    print(f"Generated {len(plain_qas)} plain Q&As  → {plain_file} | {now}")

def prepare_as_type_datasets(num_samples):
    as_types_caida_path = "datasets/as_class_type/as_class_type_qas.json"

    with open(as_types_caida_path, "r", encoding="utf‑8") as f:
        as_type_list = json.load(f)

    # Assuming the list contains dictionaries with 'question' and 'answer' keys
    # and the question contains the ASN as a number or string that can be converted to int
    as_type_dict = {}
    for item in as_type_list[:num_samples]:
        # Extract ASN from the question string
        match = re.search(r'\d+', item['question'])
        if match:
            asn = match.group(0)
            as_type_dict[asn] = [item['answer'].split(':')[-1].strip(), ""] # Placeholder for mapped class

    for asn in as_type_dict:
        as_type_from_pdb = pdb_as_type_info_type(int(asn))[0]
        as_class_from_map = map_as_class(as_type_from_pdb)
        as_type_dict[asn] = [
            as_type_from_pdb,
            as_class_from_map
        ]

    prepare_as_class_type_datasets(as_type_dict)

# AS Rels
def download_recent_as_rels():
    BASE_URL = "https://publicdata.caida.org/datasets/as-relationships/serial-2/"

    html = requests.get(BASE_URL).text
    matches = re.findall(r"([0-9]{8}\.as-rel2\.txt\.bz2)", html)
    if not matches:
        raise RuntimeError("No snapshot files found on the CAIDA page.")

    latest = sorted(set(matches))[-1]        # most recent file name, e.g. 20250701.as-rel2.txt.bz2
    print(f"Latest snapshot   : {latest}")

    dest_dir  = Path("datasets/as_rel")
    # dest_path = dest_dir / latest.replace(".bz2", "")   # strip the .bz2 suffix
    dest_path = dest_dir / 'as-rel2.txt'
    download_url = BASE_URL + latest
    print(f"Downloading from  : {download_url}")
    print(f"Saving decompressed file to: {dest_path}")

    decompressor = bz2.BZ2Decompressor()
    chunk_size   = 1 << 20  # 1 MiB

    with requests.get(download_url, stream=True) as r, open(dest_path, "wb") as out_file:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        with tqdm(total=total, unit="B", unit_scale=True, desc="Downloading") as pbar:
            for chunk in r.iter_content(chunk_size):
                out_file.write(decompressor.decompress(chunk))
                pbar.update(len(chunk))

    print("✔ Done. File size:", dest_path.stat().st_size, "bytes")
    return str(dest_path)

def create_ases_dict():
    as_rels_path = f"datasets/as_rel/as-rel2.txt" 
    as_rel_lines = []
    with open(as_rels_path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            # Drop blank lines & commented / header lines
            if not line or line.startswith("#"):
                continue

            fields = line.split("|")
            # Keep only the rows that have exactly 4 pipe‑separated fields
            if len(fields) == 4:
                as_rel_lines.append(line)

    # Quick sanity‑check:
    print(f"Loaded {len(as_rel_lines):,} AS‑relationship rows.")
    print("First 5 examples:", as_rel_lines[:5])
    len(as_rel_lines)
    records = []
    for line in as_rel_lines:
        a1, a2, rel, *_ = line.split("|")
        records.append({
            "AS1": int(a1),
            "AS2": int(a2),
            "rel": int(rel)
        })

    print(f"Built {len(records):,} dictionaries")
    print(records[:5])
    return records

def build_index(edges: List[Dict[str, int]]):
    providers_of: Dict[int, Set[int]] = defaultdict(set)
    customers_of: Dict[int, Set[int]] = defaultdict(set)
    peers_of:      Dict[int, Set[int]] = defaultdict(set)

    for e in edges:
        a1, a2, r = e["AS1"], e["AS2"], e["rel"]

        if r == -1:               # provider–customer
            customers_of[a1].add(a2)   # a1’s customer
            providers_of[a2].add(a1)   # a2’s provider
        elif r == 0:              # peer–peer
            peers_of[a1].add(a2)
            peers_of[a2].add(a1)

    return providers_of, customers_of, peers_of

def get_as_rank(asn):
  return get_as_rank_data(asn)['data']['asn']['rank']

def get_siblings(asn: int) -> List[int] :
    org = fetch_org(asn)
    siblings = current_as2org(org)
    if siblings:
        if asn in siblings:
            siblings.remove(asn)
    return siblings

def return_asn_rels_dict():
    download_recent_as_rels()
    records = create_ases_dict()
    providers, customers, peers = build_index(records)

    def get_providers(asn: int) -> List[int]:
        """Return a sorted list of all upstream providers of `asn`."""
        return sorted(providers.get(asn, []))

    def get_customers(asn: int) -> List[int]:
        """Return a sorted list of all direct customers of `asn`."""
        return sorted(customers.get(asn, []))

    def get_peers(asn: int) -> List[int]:
        """Return a sorted list of all peers of `asn`."""
        return sorted(peers.get(asn, []))
        
    il_asns = [8551, 7922, 8283, 9002, 199995, 200462]
    tiers_dataset_path = 'datasets/as_tier/tiers.json'
    with open(tiers_dataset_path, "r", encoding="utf-8") as f:
        tiers_dict = json.load(f)
    tier1 = [asn for asn, tier in tiers_dict.items() if tier == "tier-1"]
    tier1_sample = random.sample(tier1, k=2)
    tier2 = [asn for asn, tier in tiers_dict.items() if tier == "tier-2"]
    tier2_sample = random.sample(tier2, k=2)
    tier3 = [asn for asn, tier in tiers_dict.items() if tier == "tier-3"]
    tier3_sample = random.sample(tier3, k=2)
    asns = list(set(il_asns + tier1_sample + tier2_sample + tier3_sample))
    asns_data = dict()

    for asn in asns:
        asn_providers = get_providers(asn)
        asn_customers = get_customers(asn)
        asn_peers = get_peers(asn)
        asn_siblings = get_siblings(asn)
        asns_data[asn] = {
            "providers": asn_providers,
            "customers": asn_customers,
            "peers": asn_peers,
            "siblings": asn_siblings,
        }

    return asns_data

def assign_types(as_rel_dict: Dict[str, Dict[str, list]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}

    for origin_asn, rels in as_rel_dict.items():
        per_origin: Dict[str, Dict[str, Dict[str, str]]] = {}
        for rel_name in ("providers", "customers", "peers", "siblings"):
            rel_asns = rels.get(rel_name, []) or []
            mapped: Dict[str, Dict[str, str]] = {}
            for asn in rel_asns:
                # Normalize to int for the function call, keep str as key
                asn_int = int(asn)
                as_type = pdb_as_type_info_type(asn_int)
                if isinstance(as_type, list):  # Check if as_type is a list
                    as_type = as_type[0] if as_type else None # Take the first element if the list is not empty
                if as_type:
                    as_class = map_as_class(as_type)
                    mapped[str(asn)] = {
                        "as_type": as_type,
                        "as_class": as_class,
                    }
                else:
                  as_class = " "
                  mapped[str(asn)] = {
                        "as_type": " ",
                        "as_class": as_class,
                    }
            per_origin[rel_name] = mapped
        result[str(origin_asn)] = per_origin

    return result

def _iter_all_relationships(red: Dict[str, Dict[str, Dict[str, dict]]]) -> Iterable[Relation]:
    for origin, rels in red.items():
        # origin's providers => origin is customer of each provider
        for p in rels.get("providers", {}) or {}:
            yield (str(origin), str(p), "customer-provider")

        # origin's customers => each customer is customer of origin (provider)
        for c in rels.get("customers", {}) or {}:
            yield (str(c), str(origin), "customer-provider")

        # peers
        for peer in rels.get("peers", {}) or {}:
            a, b = str(origin), str(peer)
            # canonicalize to avoid duplicates a-b and b-a
            if a < b:
                yield (a, b, "peers")
            else:
                yield (b, a, "peers")

        # siblings
        for sib in rels.get("siblings", {}) or {}:
            a, b = str(origin), str(sib)
            if a < b:
                yield (a, b, "siblings")
            else:
                yield (b, a, "siblings")

def sample_relationships(
    red: Dict[str, Dict[str, Dict[str, dict]]],
    k: int = 50,
    seed: int | None = None,
) -> List[Dict[str, str]]:
    all_rels: List[Relation] = []

    # We'll still iterate the same way, but produce labels via REL_MAP.
    for origin, rels in red.items():
        # origin's providers => origin is customer of each provider -> REL -1 (Provider to Customer) from provider -> origin
        for p in rels.get("providers", {}) or {}:
            rel_label = REL_MAP[-1]
            # provider -> customer
            all_rels.append((str(p), str(origin), rel_label))

        # origin's customers => origin is provider of each customer -> REL -1 from origin -> customer
        for c in rels.get("customers", {}) or {}:
            rel_label = REL_MAP[-1]
            all_rels.append((str(origin), str(c), rel_label))

        # peers => 0
        for peer in rels.get("peers", {}) or {}:
            a, b = str(origin), str(peer)
            if a < b:
                all_rels.append((a, b, REL_MAP[0]))
            else:
                all_rels.append((b, a, REL_MAP[0]))

        # siblings => 'S'
        for sib in rels.get("siblings", {}) or {}:
            a, b = str(origin), str(sib)
            if a < b:
                all_rels.append((a, b, REL_MAP['S']))
            else:
                all_rels.append((b, a, REL_MAP['S']))

    # Deduplicate
    seen = set()
    deduped: List[Relation] = []
    for r in all_rels:
        if r not in seen:
            seen.add(r)
            deduped.append(r)

    if seed is not None:
        random.seed(seed)
    if len(deduped) <= k:
        chosen = deduped
    else:
        chosen = random.sample(deduped, k)

    return [{"AS1": a, "AS2": b, "Rel": rel} for a, b, rel in chosen]

def build_as_relationship_qas(rels_dataset):
    # Here rel_data['Rel'] already stores the descriptive string like 'Provider to Customer', 'Peers', 'Siblings'.
    # We simply use that string directly rather than looking it up in REL_MAP again.
    as_rels_query = lambda asn1, asn2: (
        f"What is the type of relationship between the following ASes: AS{asn1}, AS{asn2}?"
    )
    return [
        {
            "question": as_rels_query(rel_data['AS1'], rel_data['AS2']),
            "answer": (
                f"The relationship between AS{rel_data['AS1']} and AS{rel_data['AS2']} is: {rel_data['Rel']}"
            ),
        }
        for rel_data in rels_dataset
    ]

def prepare_as_rels_q_a_dataset(SAMPLES):
    asn_rels_dict = return_asn_rels_dict()
    res = assign_types(asn_rels_dict)
    rows = sample_relationships(res, k=SAMPLES, seed=42)
    as_q_a_dataset = build_as_relationship_qas(rows)
    output_file = Path("datasets/as_rel/as_rels_qas.json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(as_q_a_dataset, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(as_q_a_dataset)} items to {output_file}")
    return as_q_a_dataset

# X AS Rels
def _top_related(
    asn: int,
    top_n: int,
    rel_type: str,
    direction: str,
    ip_version: int | None = None,
    rank_source: str = "caida.asrank",
) -> pd.DataFrame:
    """Generic helper – do not call directly.

    *direction* ∈ {"out", "in", "both"}:
        • "out"  → (src)-[rel]->(dst)   – *dst* are returned.
        • "in"   → (src)<-[rel]-(dst)
        • "both" → (src)-[rel]-(dst)
    """

    # Relationship pattern depending on direction ---------------------------
    if direction == "out":
        pattern = f"(:AS {{asn:$asn}})-[r:{rel_type}]->(other:AS)"
    elif direction == "in":
        pattern = f"(:AS {{asn:$asn}})<-[r:{rel_type}]-(other:AS)"
    else:  # both
        pattern = f"(:AS {{asn:$asn}})-[r:{rel_type}]-(other:AS)"

    ip_filter = "" if ip_version is None else "AND r.af = $ip_version"

    query = f"""
    MATCH {pattern}
    WHERE other.asn <> $asn {ip_filter}
    OPTIONAL MATCH (other)-[rank_rel:RANK {{reference_name:$rank_source}}]->(:Ranking)
    OPTIONAL MATCH (other)-[:NAME]->(n:Name)
    WITH other,
         toInteger(rank_rel.rank) AS as_rank,
         head([name IN collect(n.name) WHERE name IS NOT NULL]) AS as_name
    RETURN DISTINCT other.asn  AS asn,
                    coalesce(as_name,'(no name)') AS name,
                    as_rank
    ORDER BY (as_rank IS NULL) ASC, as_rank ASC
    LIMIT $top_n
    """

    df = db.execute_query(
        query,
        asn=int(asn),
        top_n=int(top_n),
        ip_version=int(ip_version) if ip_version is not None else None,
        rank_source=rank_source,
        database_=DB,
        result_transformer_=neo4j.Result.to_df,
    )

    return df

def _top_related_hege(
    asn: int,
    top_n: int,
    rel_type: str,
    direction: str,
    ip_version: int | None = None,
) -> pd.DataFrame:
    """Generic helper ordered by relationship hegemony (higher first)."""

    if direction == "out":
        pattern = f"(:AS {{asn:$asn}})-[r:{rel_type}]->(other:AS)"
    elif direction == "in":
        pattern = f"(:AS {{asn:$asn}})<-[r:{rel_type}]-(other:AS)"
    else:
        pattern = f"(:AS {{asn:$asn}})-[r:{rel_type}]-(other:AS)"

    ip_filter = "" if ip_version is None else "AND r.af = $ip_version"

    query = f"""
    MATCH {pattern}
    WHERE other.asn <> $asn {ip_filter}
    OPTIONAL MATCH (other)-[:NAME]->(n:Name)
    WITH other,
         max(r.hege) AS hege,
         head([name IN collect(n.name) WHERE name IS NOT NULL]) AS as_name
    RETURN DISTINCT other.asn AS asn,
                    coalesce(as_name,'(no name)') AS name,
                    hege
    ORDER BY hege DESC
    LIMIT $top_n
    """

    df = db.execute_query(
        query,
        asn=int(asn),
        top_n=int(top_n),
        ip_version=int(ip_version) if ip_version is not None else None,
        database_=DB,
        result_transformer_=neo4j.Result.to_df,
    )
    return df

def top_providers(asn: int | str, top_n: int = 5, *, ip_version: int | None = None) -> pd.DataFrame:
    """Return the *top_n* upstream (provider) ASes for **asn** (lower CAIDA AS‑Rank preferred)."""
    return _top_related(int(asn), top_n, "DEPENDS_ON", "out", ip_version)


def top_customers(asn: int | str, top_n: int = 5, *, ip_version: int | None = None) -> pd.DataFrame:
    """Return the *top_n* downstream (customer) ASes for **asn**."""
    return _top_related(int(asn), top_n, "DEPENDS_ON", "in", ip_version)


def top_peers(asn: int | str, top_n: int = 5, *, ip_version: int | None = None) -> pd.DataFrame:
    """Return the *top_n* lateral peers for **asn**."""
    return _top_related(int(asn), top_n, "PEERS_WITH", "both", ip_version)


def top_siblings(asn: int | str, top_n: int = 5) -> pd.DataFrame:
    """Return the *top_n* sibling ASes for **asn**."""
    return _top_related(int(asn), top_n, "SIBLING_OF", "both", ip_version=None)

def top_providers_hege(asn: int | str, top_n: int = 5, *, ip_version: int | None = None) -> pd.DataFrame:
    """Top *providers* ordered by hegemony (higher first)."""
    return _top_related_hege(int(asn), top_n, "DEPENDS_ON", "out", ip_version)


def top_customers_hege(asn: int | str, top_n: int = 5, *, ip_version: int | None = None) -> pd.DataFrame:
    """Top *customers* ordered by hegemony."""
    return _top_related_hege(int(asn), top_n, "DEPENDS_ON", "in", ip_version)


def top_peers_hege(asn: int | str, top_n: int = 5, *, ip_version: int | None = None) -> pd.DataFrame:
    """Top *peers* ordered by hegemony."""
    return _top_related_hege(int(asn), top_n, "PEERS_WITH", "both", ip_version)


def top_siblings_hege(asn: int | str, top_n: int = 5) -> pd.DataFrame:
    """Top *siblings* ordered by hegemony."""
    return _top_related_hege(int(asn), top_n, "SIBLING_OF", "both", ip_version=None)

def build_top_x_rels_qas(top_x_rels_dataset):
    query = lambda asn, n, relationship: (
        f"Who are the top {n} {relationship}s of AS{asn}?"
    )
    return [
        {
            "question": query(top_as_x_data['asn'], top_as_x_data['n'], top_as_x_data['relationship']),
            "answer": (
                f"The top {top_as_x_data['n']} {top_as_x_data['relationship']}s of AS{top_as_x_data['asn']} are: "
                + ", ".join([str(x) for x in top_as_x_data['asns']])
            ),
        }
        for top_as_x_data in top_x_rels_dataset if 'asn' in top_as_x_data and 'n' in top_as_x_data and 'relationship' in top_as_x_data and 'asns' in top_as_x_data
    ]

def _caida_rel_set(rel, asn, CAIDA_REL_MAP) -> set[int]:
    """Return the CAIDA set of ASNs holding *rel* to *asn*."""
    return set(get_siblings(asn)) if rel == "sibling" else set(CAIDA_REL_MAP[rel][asn])

def top_x_relationships(asns, relationship: str, metric: str):
    records = create_ases_dict()
    providers, customers, peers = build_index(records)
    CAIDA_REL_MAP = {
        "provider": providers,
        "customer": customers,
        "peer":     peers,
    }
    REL_FUNCS = {
        "asrank": {
            "provider": top_providers,
            "customer": top_customers,
            "peer":     top_peers,
            "sibling":  top_siblings,
        },
        "hegemony": {
            "provider": top_providers_hege,
            "customer": top_customers_hege,
            "peer":     top_peers_hege,
            "sibling":  top_siblings_hege,
        },
    }
    relationship = relationship.lower()
    metric = metric.lower()

    if relationship not in ("provider", "customer", "peer", "sibling"):
        raise ValueError("relationship must be provider / customer / peer / sibling")
    if metric not in ("asrank", "hegemony"):
        raise ValueError("metric must be asrank / hegemony")

    top_fn = REL_FUNCS[metric][relationship]
    results = []

    for asn in asns:
        print(asn)
        n_target = random.randint(2,10)

        # authoritative CAIDA list for this relationship
        caida_set = _caida_rel_set(relationship, asn, CAIDA_REL_MAP)
        print(f"CAIDA: {caida_set}")
        # gather candidates from the chosen metric (v4+v6)
        candidates = set()
        if relationship in ("provider", "customer", "peer"):
            for v in [4,6]:
                df = top_fn(asn, n_target, ip_version=v)
                candidates.update(df["asn"].tolist())
        else: # sibling
             df = top_fn(asn, n_target)
             candidates.update(df["asn"].tolist())
        print(f"IYP: {candidates}")
        # keep only those that CAIDA also labels with the same relationship
        candidates &= caida_set
        if not candidates:
            continue                    # skip ASNs with no valid matches

        n_final = min(len(candidates), n_target)
        results.append(
            {
                "asn": asn,
                "n": n_final,
                "relationship": relationship,
                "asns": sorted(candidates)[:n_final],
            }
        )
    print(results)
    return build_top_x_rels_qas(results)

def prepare_top_x_rels_datasets():
    il_asns = [6810, 8551, 7922, 8283, 9002, 199995, 200462]
    tiers_dataset_path = 'datasets/as_tier/tiers.json'
    with open(tiers_dataset_path, "r", encoding="utf-8") as f:
        tiers_dict = json.load(f)
    asns = list(tiers_dict.keys())
    asns = [int(x) for x in asns]
    asns = asns + il_asns

    top_providers_asrank = top_x_relationships(asns, 'provider', 'asrank')
    top_providers_asrank = top_providers_asrank[:50]

    output_file = Path("datasets/top_x_rels/top_providers_as_rank_qas.json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(top_providers_asrank, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(top_providers_asrank)} items to {output_file}")

    top_providers_hege = top_x_relationships(asns, 'provider', 'hegemony')
    top_providers_hege = top_providers_hege[:50]

    output_file = Path("datasets/top_x_rels/top_providers_hege_qas.json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(top_providers_hege, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(top_providers_hege)} items to {output_file}")

    top_customers_asrank = top_x_relationships(asns, "customer", "asrank")
    top_customers_asrank = top_customers_asrank[:50]

    output_file = Path("datasets/top_x_rels/top_customers_as_rank_qas.json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(top_customers_asrank, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(top_customers_asrank)} items to {output_file}")

    top_customers_hege = top_x_relationships(asns, "customer", "hegemony")
    top_customers_hege = top_customers_hege[:50]

    output_file = Path("datasets/top_x_rels/top_customers_hege_qas.json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(top_customers_hege, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(top_customers_hege)} items to {output_file}")

    top_peers_asrank = top_x_relationships(asns, "peer", "asrank")
    top_peers_asrank = top_peers_asrank[:50]

    output_file = Path("datasets/top_x_rels/top_peers_as_rank_qas.json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(top_peers_asrank, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(top_peers_asrank)} items to {output_file}")

    top_peers_hege = top_x_relationships(asns, "peer", "hegemony")
    top_peers_hege = top_peers_hege[:50]

    output_file = Path("datasets/top_x_rels/top_peers_hege_qas.json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(top_peers_hege, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(top_peers_hege)} items to {output_file}")

    top_siblings_asrank = top_x_relationships(asns, "sibling", "asrank")
    top_siblings_asrank = top_siblings_asrank[:50]

    output_file = Path("datasets/top_x_rels/top_siblings_as_rank_qas.json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(top_siblings_asrank, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(top_siblings_asrank)} items to {output_file}")

    top_siblings_hege = top_x_relationships(asns, "sibling", "hegemony")
    top_siblings_hege = top_siblings_hege[:50]

    output_file = Path("datasets/top_x_rels/top_siblings_hege_qas.json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(top_siblings_hege, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(top_siblings_hege)} items to {output_file}")
