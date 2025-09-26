import requests

# Parse text to dictionary
def parse_to_dict(text):
    result = {}
    for line in text.strip().splitlines():
        if not line.strip():
            continue  # Skip any empty lines
        # Split at the first colon only
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        # If the key already exists, convert to list or append to existing list
        if key in result:
            if isinstance(result[key], list):
                result[key].append(value)
            else:
                result[key] = [result[key], value]
        else:
            result[key] = value
    return result

# Fetch AS prefixes data
def fetch_asn_data(asn: str, query_type: str = "prefix"):
    """Fetch ASN-related data from IRRExplorer"""
    # Ensure the ASN starts with "AS"
    if not asn.upper().startswith("AS"):
        asn = "AS" + asn

    base_url = "https://irrexplorer.nlnog.net/api"
    if query_type.lower() == "prefix":
        url = f"{base_url}/prefixes/asn/{asn}"
    elif query_type.lower() == "as-set":
        url = f"{base_url}/sets/member-of/as-set/{asn}"
    elif query_type.lower() == "route-set":
        url = f"{base_url}/sets/member-of/route-set/{asn}"
    else:
        raise ValueError("Invalid query type. Choose 'prefix', 'as-set', or 'route-set'.")

    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

# Fetch data for an IP address
def fetch_ip_data(ip_input: str):
    """Fetch IP prefix or IP address related data from IRRExplorer."""
    base_url = "https://irrexplorer.nlnog.net/api"
    url = f"{base_url}/prefixes/prefix/{ip_input}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Return the number of originated prefixes of an ASes according IRRs data
def num_of_as_originated_prefixes(asn):
  asn_prefixes_data = fetch_asn_data(str(asn), query_type="prefix")
  return len(asn_prefixes_data['directOrigin'])

# Return all the originated prefixes of an ASes according IRRs data
def as_originated_prefixes(asn):
  asn_prefixes_data = fetch_asn_data(str(asn), query_type="prefix")
  prefixes = []
  for prefix in asn_prefixes_data['directOrigin']:
    prefixes.append(prefix['prefix'])
  return prefixes

# Return all the rpki invalid prefixes of an ASes according IRRs data
def invalid_prefixes_of_as(asn):
  asn_prefixes_data = fetch_asn_data(str(asn), query_type="prefix")
  invalid_prefixes = []
  for prefix in asn_prefixes_data['directOrigin']:
    if prefix['rpkiRoutes']:
      if prefix['rpkiRoutes'][0]['rpkiStatus'] != 'VALID':
        invalid_prefixes.append(prefix['prefix'])
  return invalid_prefixes

# Return all the suspicious prefixes of an AS according IRR data
def suspicious_prefixes_of_as(asn):
  asn_prefixes_data = fetch_asn_data(str(asn), query_type="prefix")
  suspicious_prefixes = []
  for prefix in asn_prefixes_data['directOrigin']:
    if prefix['categoryOverall'] != 'success':
      prefix_dict = {}
      prefix_dict['prefix'] = prefix['prefix']
      prefix_dict['category'] = prefix['categoryOverall']
      prefix_dict['bgpOrigins'] = prefix['bgpOrigins']
      prefix_dict['irrRoutes']= prefix['irrRoutes']
      prefix_dict['category_text'] = prefix['messages'][0]['category']
      prefix_dict['text'] = prefix['messages'][0]['text']
      prefix_dict['rpkiRoutes'] = prefix['rpkiRoutes']
      prefix_dict['goodnessOverall'] = prefix['goodnessOverall']
      suspicious_prefixes.append(prefix_dict)
  return suspicious_prefixes

# Return all the overlapped prefixes of an AS according IRR data
def overlaps_prefixes_of_as(asn):
    prefixes = fetch_asn_data(str(asn), query_type="prefix")
    overlaps = prefixes['overlaps']
    overlap_prefixes = []
    for prefix in overlaps:
        overlap_prefixes.append(prefix['prefix'])
    return overlap_prefixes

# ip2asn
def ip2asn(ip):
    ip_data = fetch_ip_data(ip)
    irr_routes_data = ip_data[0]['irrRoutes']
    asns = []
    for irr in irr_routes_data:
      asns.append(irr_routes_data[irr][0]['asn'])
    return asns 

# IP IRR Status
def get_ip_irr_data(ip): 
    ip_data = fetch_ip_data(ip)
    irr_routes_data = ip_data[0]['irrRoutes']
    return irr_routes_data

# IP RPKI data
def get_ip_rpki_data(ip):
    ip_data = fetch_ip_data(ip)
    irr_routes_data = ip_data[0]['rpkiRoutes']
    return irr_routes_data
# Get IP IRR 
def get_ip_rir(ip):
  ip_data = fetch_ip_data(ip)
  irr_routes_data = ip_data[0]['rir']
  return irr_routes_data

# IP RPKI status
def ip_rpki_status(ip):
    ip_data = fetch_ip_data(ip)
    irr_routes_data = ip_data[0]['irrRoutes']
    rpki_statuses = []
    for irr in irr_routes_data:
      rpki_statuses.append(irr_routes_data[irr][0]['rpkiStatus'])
    return rpki_statuses

# IP IRR last modified
def get_ip_rpki_last_modified(ip):
    ip_data = fetch_ip_data(ip)
    irr_routes_data = ip_data[0]['irrRoutes']
    last_modified = []
    for irr in irr_routes_data:
      rpsl = parse_to_dict(irr_routes_data[irr][0]['rpslText'])
      last_modified.append(rpsl['last-modified'])
    return last_modified

# IP IRR max length
def ip_rpki_max_length(ip):
    ip_data = fetch_ip_data(ip)
    irr_routes_data = ip_data[0]['irrRoutes']
    max_lengths = []
    for irr in irr_routes_data:
      max_lengths.append(irr_routes_data[irr][0]['rpkiMaxLength'])
    return max_lengths

def get_irr_routes_data_for_an_ip(ip):
    ip_data = fetch_ip_data(ip)
    irr_routes_data = ip_data[0]['irrRoutes']
    routes = []
    for route in irr_routes_data:
        print(route)
        routes.append(route)
        # for key,value in irr_routes_data[route][0].items():
        #     print(f"{key}: {value}")
    return routes

def get_irr_ip_risk_status(ip):
    ip_data = fetch_ip_data(ip)
    risk = ip_data[0]['categoryOverall']
    return risk  

def get_irr_ip_origin_ases(ip):
    ip_data = fetch_ip_data(ip)
    asns = ip_data[0]['bgpOrigins']
    return asns

def get_irr_ip_messages(ip):
    ip_data = fetch_ip_data(ip)
    messages = ip_data[0]['messages'][0]
    return messages

def fetch_as_set_data(as_set: str):
    """
    Fetch as-set data from IRRExplorer.

    Parameters:
      as_set (str): The as-set string, for example "AS-BSVE:AS-LINX-PEERS-2".

    Returns:
      dict: The JSON response from the API.
    """
    base_url = "https://irrexplorer.nlnog.net/api"
    url = f"{base_url}/sets/expand/{as_set}"
    response = requests.get(url)
    response.raise_for_status()  # Raises an error for HTTP status codes 4XX/5XX
    return response.json()

def get_as_set_path(as_set):
    as_set_data = fetch_as_set_data(as_set)
    return as_set_data[0]['path']

def get_as_set_members(as_set):
    as_set_data = fetch_as_set_data(as_set)
    return as_set_data[0]['members']

def fetch_route_set_data(route_set: str):
    base_url = "https://irrexplorer.nlnog.net/api"
    url = f"{base_url}/sets/expand/{route_set}"
    response = requests.get(url)
    response.raise_for_status()  # Raises an error for HTTP status codes 4XX/5XX
    data =  response.json()
    return data[0]

def get_route_set_path(route_set):
    route_set_data = fetch_route_set_data(route_set)
    return route_set_data[0]['path']

def get_route_set_members(route_set):
    route_set_data = fetch_route_set_data(route_set)
    return route_set_data[0]['members']

