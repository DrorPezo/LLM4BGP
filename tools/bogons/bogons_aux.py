import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from collections import defaultdict
import ipaddress

# Check if an IP address or a prefix is bogon
def is_bogon(prefix):
    # List of well-known bogon prefixes
    bogon_prefixes = [
        "0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
        "172.16.0.0/12", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15",
        "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4"
    ]

    # Fetch bogon prefixes from Internet Yellow Pages (Team Cymru)
    bogons = fetch_bogons()
    bogon_prefixes.extend(bogons)

    prefix_network = ipaddress.ip_network(prefix, strict=False)

    for bogon in bogon_prefixes:
        if prefix_network.overlaps(ipaddress.ip_network(bogon)):
            return True  # The prefix is a bogon

    return False  # The prefix is not a bogon


def fetch_bogons():
    url = "https://www.team-cymru.org/Services/Bogons/fullbogons-ipv4.txt"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return [line.strip() for line in response.text.split('\n') if line and not line.startswith('#')]
    except requests.RequestException:
        pass  # If the request fails, we return an empty list
    return []
