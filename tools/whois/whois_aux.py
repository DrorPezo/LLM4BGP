from __future__ import annotations
import socket
import re
from typing import List, Dict, Optional, Iterable
from collections import defaultdict

# Regex helpers
IP_RE  = r"(?:\d{1,3}(?:\.\d{1,3}){3}|[0-9a-fA-F:]+)"  # v4 or v6
ASN_RE = r"AS(\d+)"
ACT_RE = r"(?:action\s+([^;]+);\s*)?"  # optional action‑clause

IMPORT_RX = re.compile(
    rf"^(from)\s+{ASN_RE}(?:\s+({IP_RE}))?\s+{ACT_RE}accept\s+(.+)$",
    re.I,
)
EXPORT_RX = re.compile(
    rf"^(to)\s+{ASN_RE}(?:\s+({IP_RE}))?\s+{ACT_RE}announce\s+(.+)$",
    re.I,
)

def get_full_as_irr(asn: int, host: str = "whois.radb.net") -> str:
    query = f"-r -T aut-num AS{asn}\n"  # -r no recursion, -T filter by type
    with socket.create_connection((host, 43), timeout=10) as sock:
        sock.sendall(query.encode())
        buf = bytearray()
        while chunk := sock.recv(16384):
            buf.extend(chunk)
    return buf.decode(errors="replace")

def split_rpsl(rpsl: str) -> List[tuple[str, str]]:
    rows: List[tuple[str, str]] = []
    current: Optional[str] = None

    for raw in rpsl.splitlines():
        if not raw.strip():
            continue  # skip blank lines entirely

        if raw[:1].isspace():  # continuation line
            if current:
                rows.append((current, raw.strip()))
            continue

        if ":" not in raw:
            continue  # ignore garbage

        key, val = raw.split(":", 1)
        current = key.strip().lower()
        rows.append((current, val.strip()))

    return rows

def parse_policy(rows: Iterable[tuple[str, str]]) -> List[Dict]:
    table: List[Dict] = []

    for attr, line in rows:
        if attr not in ("import", "export"):
            continue

        rx = IMPORT_RX if attr == "import" else EXPORT_RX
        m  = rx.match(line)

        if not m:
            # Store unparsed line for completeness
            table.append({
                "attr": attr,
                "direction": None,
                "peer_asn": None,
                "neighbor_ip": None,
                "action": None,
                "policy": line,
            })
            continue

        direction, asn_str, ip, action, policy = m.groups()
        table.append({
            "attr": attr,
            "direction": direction.lower(),  # 'from' / 'to'
            "peer_asn": int(asn_str),
            "neighbor_ip": ip,               # may be None
            "action": action,               # may be None
            "policy": policy.strip(),
        })

    return table

def collect_remarks(rows: Iterable[tuple[str, str]]) -> List[str]:
    paras: List[str] = []
    bucket: List[str] = []

    for attr, txt in rows:
        if attr != "remarks":
            continue
        if not txt:
            if bucket:
                paras.append(" ".join(bucket))
                bucket = []
        else:
            bucket.append(txt)
    if bucket:
        paras.append(" ".join(bucket))
    return paras

def get_structured_policies(asn: int, host: str = "whois.radb.net") -> Dict[str, list]:
    raw  = get_full_as_irr(asn, host)
    rows = split_rpsl(raw)
    return {
        "policies": parse_policy(rows),
        "remarks": collect_remarks(rows),
    }

data = get_structured_policies(3356)   # Level-3 / Lumen

def get_as_imports_exports(asn):
    # Initialize imports and exports dictionaries using defaultdict
    # This automatically creates a list for a key if it doesn't exist when you try to access it
    imports = defaultdict(list)
    exports = defaultdict(list)

    for policy in data['policies']:
      if policy['attr'] == 'import':
        # Append directly to the list associated with the peer_asn key.
        # defaultdict ensures a list exists even if it's the first time encountering this peer_asn.
        imports[policy['peer_asn']].append(policy)
      else:
        # Same logic for exports
        exports[policy['peer_asn']].append(policy)
    return imports, exports

def as_imports_with_other_as(asn1, asn2):
    imports, exports = get_as_imports_exports(asn1)
    return imports[asn2]

def as_exports_with_other_as(asn1, asn2):
    imports, exports = get_as_imports_exports(asn1)
    return exports[asn2]
    