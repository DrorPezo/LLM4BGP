import json
import pybgpstream
from collections import defaultdict
from ipaddress import ip_network
import pickle


# Moas Analysis
# Return all MOAS prefixes and their ASNs in a time interval
def return_moas_prefixes_and_asns():
    stream = pybgpstream.BGPStream(
        from_time="2024-08-01 07:50:00",
        until_time="2024-08-01 08:10:00",
        collectors=["rrc00"],
        record_type="ribs",
    )

    prefix_origin = defaultdict(set)
    for rec in stream.records():
        for elem in rec:
            pfx = elem.fields["prefix"]
            ases = elem.fields["as-path"].split(" ")
            if len(ases) > 0:
                origin = ases[-1]
                prefix_origin[pfx].add(origin)

    moas_prefixes = []
    for pfx in prefix_origin:
        if len(prefix_origin[pfx]) > 1:
            print((pfx, ",".join(prefix_origin[pfx])))
            moas_prefixes.append([pfx, prefix_origin[pfx]])
    return moas_prefixes

def moas_prefixes_for_asn(target_asn):
    # Create a BGPStream instance.
    # You can adjust the time interval, collector(s), or record_type as needed.
    stream = pybgpstream.BGPStream(
        from_time="2015-08-01 07:50:00",
        until_time="2015-08-01 08:10:00",
        collectors=["rrc00"],
        record_type="ribs",
    )

    # Dictionary to hold prefix -> set of origin ASNs.
    prefix_origin = defaultdict(set)

    # Iterate over all records and their elements.
    for rec in stream.records():
        for elem in rec:
            pfx = elem.fields.get("prefix")
            if not pfx:
                continue

            # Split the AS-path string into individual ASNs.
            ases = elem.fields.get("as-path", "").split(" ")
            if not ases or ases == [""]:
                continue

            # The origin ASN is the last element in the AS-path.
            origin = ases[-1]
            prefix_origin[pfx].add(origin)
    moas = []
    # Print prefixes that are MOAS (announced by >1 origin)
    # and that include the target ASN.
    print(f"MOAS prefixes that include ASN {target_asn}:")
    for pfx, origins in prefix_origin.items():
        if len(origins) > 1 and target_asn in origins:
            print(f"{pfx} -> Origins: {', '.join(origins)}")
            moas.append(pfx)
    return moas
    
# Check if a prefix is a MOAS in a time interval
def check_if_prefix_is_moas_bgp_stream(prefix, from_time, until_time):
    collectors = ["rrc00"]
    moas_prefixes = return_moas_prefixes_and_asns()
    asns = moas_prefixes[prefix]
    if len(asns) > 1:
        return (True, asns)
    else:
        return (False, asns)

# RPKI
def get_bgp_stream_rpki_data():
    stream = BGPStream(project="routeviews-stream", filter="router amsix")
    rpki_data = []
    counter = 0
    for record in stream.records():
        for elem in record:
            prefix = ip_network(elem.fields['prefix'])
            if elem.type == "A":
                # Lookup RPKI state based on announced route.
                request = requests.get(f"https://api.routeviews.org/rpki?prefix={prefix}", verify=False)
                response = request.json()
                # Skip all None responses
                if response[str(prefix)] is not None:
                    data = {
                        "prefix": str(prefix),
                        "rpki": response[str(prefix)],
                        "timestamp": response[str(prefix)]['timestamp']
                    }
                    # Output json to stdout
                    rpki_data.append(json.dumps(data))

# More aux functions
# Identify the top 10 AS paths with the highest number of transits of IPv4 prefixes originated from ASN xxxx
def get_top_x_transit_asns(prefix, asn, x, from_time, until_time, collectors):
    stream = pybgpstream.BGPStream(
        from_time=from_time,
        until_time=until_time,
        collectors=collectors,
        record_type="updates",
        filter='''prefix''' + prefix + '''and ipversion 4 and path \"_''' + asn + '''_\" '''
    )

    as_paths = []

    for rec in stream.records():
        for elem in rec:
            if elem.type == "A":
                as_paths.append(elem.fields["as-path"])

    top_x_paths = Counter(as_paths).most_common(x)

    # Calculate number of transits
    transits = []
    for path in top_x_paths:
        transits.append(len(path[0].split()) - 2)

    print(f"Top x AS paths with the highest number of transits for IPv4 prefix ayde-11 originated from AS 6810: {top_10_paths}")
    print(f"Number of transits: {transits}")
    return transits

# Get all BGP Communities detected in time interval
def get_bgp_communities_info(from_time, until_time, collectors, filter=None):
    stream = pybgpstream.BGPStream(
      # Consider this time interval:
      # Sat, 01 Aug 2015 7:50:00 GMT -  08:10:00 GMT
      from_time=from_time,
      until_time=until_time,
      collectors=collectors,
      record_type="ribs",
    )
    if filter:
      stream.filter=filter
    # <community, prefix > dictionary
    community_prefix = defaultdict(set)

    # Get next record
    for rec in stream.records():
        for elem in rec:
            # Get the prefix
            pfx = elem.fields['prefix']
            # Get the associated communities
            communities = elem.fields['communities']
            # for each community save the set of prefixes
            # that are affected
            for c in communities:
                community_prefix[c].add(pfx)
    return community_prefix

def load_graph_from_pickle(pickle_filename):
    """Load a NetworkX graph from a pickle file"""
    with open(pickle_filename, 'rb') as f:
        graph = pickle.load(f)
    return graph
