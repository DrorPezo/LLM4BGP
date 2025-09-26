from langchain_core.tools import tool
from pandas import DataFrame
from tools.iyp.iyp_aux import *

# Tool 1 - Tool for fetching all the neighbors ASNs of a given ASN
# @tool(return_direct=True)
@tool
def connected_asns(asn: int):
    """
    For a given ASN, fetch all the ASNs which connected to an input ASN.
    Input: ASN (int)
    Output: ASNs list (list)
    """
    asns = fetch_connected_asns(asn)
    asns_list = asns["asn"].astype(int).tolist()
    return asns_list

# Tool 2 - Given ASN, return a dictionary with the countries of its neighborgs ASes
# @tool(return_direct=True)
@tool
def connected_asns_by_country(asn: int):
    """
    For a given ASN, fetch all the ASNs which connected to an input ASN, and return a dictionary with the AS neighbors' countries
    Input: ASN (int)
    Output: Neighborgs countries histogram (dict)
    """
    connected_ases = fetch_connected_asns(asn)
    count = connected_ases.groupby("cc")["asn"].nunique().sort_values(ascending=False)
    return count.to_dict()

#Tool 3 - Tool for fetching upstreams sorted by their hegemony
# @tool(return_direct=True)
@tool
def top_hege_upstreams(asn, x, ip_version):
    """return the top x upstreams with the highest hegemony connected to ASN asn.
    - asn (int or str): The autonomous system number.
    - x (int or str): The number of top results to return. Default is 5
    - ip_version (int): The IP version to filter by (4 for IPv4, 6 for IPv6). Default is 4.
    Returns:
    - pandas.DataFrame: DataFrame containing the results."""
    top_upstreams = fetch_top_hege_upstreams(asn, x, ip_version)
    print(top_upstreams)
    return top_upstreams["asn"].astype(int).tolist()

#Tool 4 - Tool for fetching upstreams sorted by their AS rank
# @tool(return_direct=True) 
@tool
def top_as_rank_upstreams(asn, x, ip_version):
    """return the top x upstreams with the highest as rank connected to ASN asn.
    - asn (int or str): The autonomous system number.
    - x (int or str): The number of top results to return. Default is 5
    - ip_version (int): The IP version to filter by (4 for IPv4, 6 for IPv6). Default is 4.
    Returns:
    - pandas.DataFrame: DataFrame containing the results."""
    top_upstreams = fetch_top_as_rank_upstreams(asn, x, ip_version)
    print(top_upstreams)
    return top_upstreams["asn"].astype(int).tolist()

# Tool 5 - Fetch upstream ASNs for a given ASN
# @tool(return_direct=True)
@tool
def upstream_asns(asn: int) -> DataFrame:
    """
    For a given ASN, return its upstreams ASNs.
    - asn (int or str): The autonomous system number.
    """
    upstreams = fetch_upstreams_for_asn(asn)
    return upstreams['asn'].astype(int).tolist()

# Tool 6 - Check if ASN is an upstream for another ASN
# @tool(return_direct=True)
@tool
def is_upstream(asn1, asn2):
    """
    For two asns given in a format of a list or a string: "asn1, asn2",check if asn1 is an upstream of an asn2.
    - asn1 (int or str): ASN1 autonomous system number.
    - asn2 (int or str): ASN2 autonomous system number.
    """
    return is_upstream_of(asn1, asn2)

# Tool 7 - Fetch downstream asns
# @tool(return_direct=True)
@tool
def downstream_asns(asn):
    """
    For a given ASN, return DataFrame contains all the data about its downstreams ASNs.
    - asn (int or str): The autonomous system number.
    """
    downstream_asns = fetch_downstreams_for_asn(asn)
    print(DataFrame(downstream_asns))
    return downstream_asns['asn'].astype(int).tolist()

# Tool 8 - Fetch top hegemony downstreams
# @tool(return_direct=True)
@tool
def top_hege_downstreams(asn, x, ip_version):
    """return the top x downstreams with the highest hegemony connected to ASN asn.
    - asn (int or str): The autonomous system number.
    - x (int or str): The number of top results to return. Default is 5
    - ip_version (int): The IP version to filter by (4 for IPv4, 6 for IPv6). Default is 4.
    Returns:
    - pandas.DataFrame: DataFrame containing the results."""
    top_hege_downstreams = fetch_top_hege_downstreams(asn, x, ip_version)
    print(top_hege_downstreams)
    return top_hege_downstreams["asn"].astype(int).tolist()

# Tool 9 - Fetch downstream asns with top as rank
# @tool(return_direct=True)
@tool
def top_as_rank_downstreams(asn, x, ip_version):
    """return the top x downstreams with the highest hegemony connected to ASN asn.
    - asn (int or str): The autonomous system number.
    - x (int or str): The number of top results to return. Default is 5
    - ip_version (int): The IP version to filter by (4 for IPv4, 6 for IPv6). Default is 4.
    Returns:
    - pandas.DataFrame: DataFrame containing the results."""
    top_as_rank_downstreams = fetch_top_as_rank_downstreams(asn, x, ip_version)
    print(top_as_rank_downstreams)
    return top_as_rank_downstreams["asn"].astype(int).tolist()

# Tool 10 - Check if ASN is a downstream for another ASN
# @tool(return_direct=True)
@tool
def is_downstream(asn1, asn2):
    """
    For two asns given in a format of a list or a string: "asn1, asn2",check if asn1 is a downstream of an asn2.
    - asn1 (int or str): ASN1 autonomous system number.
    - asn2 (int or str): ASN2 autonomous system number.
    """
    return bool(is_downstream_of(asn1, asn2))

# Tool 11 - Fetch peers asns
# @tool(return_direct=True)
@tool
def peers(asn):
    """
    For a given ASN, return DataFrame contains all the data about its downstreams ASNs.
    - asn (int or str): The autonomous system number.
    """
    peers_asns = fetch_peers_for_asn(asn)
    return peers_asns['asn'].astype(int).tolist()

# Tool 12 - Check if a given ASN is a peer of other ASN
# @tool(return_direct=True)
@tool
def is_peer(asn1, asn2):
    """
    For two asns asn1 and asn2, check if they are peers.
    - asn1 (int or str): ASN1 autonomous system number.
    - asn2 (int or str): ASN2 autonomous system number.
    """
    return bool(is_peer_of(asn1, asn2))

# Tool 13 - Fetch siblings asns
# @tool(return_direct=True)
@tool
def siblings(asn):
    """For a given ASN, return DataFrame contains all the data about its 
    siblings ASNs."""
    siblings = fetch_siblings_for_asn(asn)
    print(siblings)
    return siblings['asn'].astype(int).tolist()

# Tool 14 - Check if two ASNs are siblings
# @tool(return_direct=True)
@tool
def are_siblings(asn1, asn2):
    """
    For two asns ,check if they are siblings.
    - asn1 (int or str): ASN1 autonomous system number.
    - asn2 (int or str): ASN2 autonomous system number.
    """
    return bool(is_sibling_of(asn1, asn2))

# Tool 15 - Check if two ASNs are connected in any way
# @tool(return_direct=True)
@tool
def are_connected(asn1, asn2) -> bool:
    """
    Check if two ASNs asn1 and asn2 are connected to each other.
    - asn1 (int or str): ASN1 autonomous system number.
    - asn2 (int or str): ASN2 autonomous system number.
    """
    return are_asns_connected(asn1, asn2)

# Tool 16 - Check for registered ROA for an ASN
# @tool(return_direct=True)
@tool
def check_registered_roas_for_asn(asn):
    """
    Return all the prefixes which are registered to a given ASN via route origin authorization (ROA).
    - asn (int or str): ASN autonomous system number.
    """
    registered_prefixes_for_asn = registered_roa_for_asn(asn)
    return registered_prefixes_for_asn['prefix'].astype(str).tolist()

# Tool 17 - get the popular domains hosted by a given ASN
# @tool(return_direct=True)
@tool
def get_popular_domains_hosted_by_asn(asn):
    '''
    get the popular domains hosted by a given ASN
    - asn (int or str): ASN autonomous system number.
    '''
    popular_domains = popular_domains_hosted_by_asn(asn)
    return popular_domains['hostName'].astype(str).tolist()

# Tool 18 - get the popular hostnames hosted by a given ASN
# @tool(return_direct=True)
@tool
def get_popular_hostnames_hosted_by_asn(asn):
    '''get the popular hostnames hosted by a given ASN'''
    popular_host_names = popular_hostnames_hosted_by_asn(asn)
    return popular_host_names['hostName'].astype(str).tolist()

# Tool 19 - get the authoritative name servers hosted by a given ASN
# @tool(return_direct=True)
@tool
def get_authoritative_ns_hosted_by_asn(asn):
    '''
    get the authoritative name servers hosted by a given ASN
    - asn (int or str): ASN autonomous system number.
    '''
    authoritative_ns = authoritative_ns_hosted_by_asn(asn)
    return authoritative_ns['nameserver'].astype(str).tolist()

# Tool 20 - get the IXPs hosted by a given ASN
# @tool(return_direct=True)
@tool
def get_ixps_for_asn(asn):
    '''
    get the IXPs hosted by a given ASN
    - asn (int or str): ASN autonomous system number.
    '''
    ixps = ixps_for_asn(asn)
    return ixps['name'].astype(str).tolist()

# Tool 21 - get the co-located ASNs for a given ASN
# @tool(return_direct=True)
@tool
def get_co_located_asns_for_asn(asn):
    '''
    get the co-located ASNs for a given ASN
    - asn (int or str): ASN autonomous system number.
    '''
    asns = co_located_asns_for_asn(asn)
    return asns['asn'].astype(str).tolist()

@tool
def top_hege_peers(asn, x, ip_version):
    """return the top x peers with the highest hegemony connected to ASN asn.
    - asn (int or str): The autonomous system number.
    - x (int or str): The number of top results to return. Default is 5
    - ip_version (int): The IP version to filter by (4 for IPv4, 6 for IPv6). Default is 4.
    Returns:
    - pandas.DataFrame: DataFrame containing the results."""
    top_hege_peers = fetch_top_hege_peers(asn, x, ip_version)
    print(top_hege_peers)
    return top_hege_peers["asn"].astype(int).tolist()

@tool
def top_as_rank_peers(asn, x, ip_version):
    """return the top x peers with the highest hegemony connected to ASN asn.
    - asn (int or str): The autonomous system number.
    - x (int or str): The number of top results to return. Default is 5
    - ip_version (int): The IP version to filter by (4 for IPv4, 6 for IPv6). Default is 4.
    Returns:
    - pandas.DataFrame: DataFrame containing the results."""
    top_as_rank_peers = fetch_top_as_rank_peers(asn, x, ip_version)
    print(top_as_rank_peers)
    return top_as_rank_peers["asn"].astype(int).tolist()

@tool
def top_hege_siblings(asn, x, ip_version):
    """return the top x siblings with the highest hegemony connected to ASN asn.
    - asn (int or str): The autonomous system number.
    - x (int or str): The number of top results to return. Default is 5
    - ip_version (int): The IP version to filter by (4 for IPv4, 6 for IPv6). Default is 4.
    Returns:
    - pandas.DataFrame: DataFrame containing the results."""
    top_hege_siblings = fetch_top_hege_siblings(asn, x, ip_version)
    print(top_hege_siblings)
    return top_hege_siblings["asn"].astype(int).tolist()

@tool
def top_as_rank_siblings(asn, x, ip_version):
    """return the top x siblings with the highest hegemony connected to ASN asn.
    - asn (int or str): The autonomous system number.
    - x (int or str): The number of top results to return. Default is 5
    - ip_version (int): The IP version to filter by (4 for IPv4, 6 for IPv6). Default is 4.
    Returns:
    - pandas.DataFrame: DataFrame containing the results."""
    top_as_rank_siblings = fetch_top_as_rank_siblings(asn, x, ip_version)
    print(top_as_rank_siblings)
    return top_as_rank_siblings["asn"].astype(int).tolist()

# # Tool 22 - get the originated prefixes statistics for an ASN
# @tool(return_direct=True)
# def show_originated_prefixes_statistics_for_asn(asn):
#     '''
#     get the originated prefixes statistics for an ASN
#     - asn (int or str): ASN autonomous system number.
#     '''
#     return originated_prefixes_statistics_for_asn(asn)

iyp_tools = [connected_asns, connected_asns_by_country, top_hege_upstreams,
        top_as_rank_upstreams, upstream_asns, is_upstream, downstream_asns,
         is_downstream, top_hege_downstreams, top_as_rank_downstreams, peers, is_peer, are_connected,  
        check_registered_roas_for_asn, get_popular_domains_hosted_by_asn, get_popular_hostnames_hosted_by_asn,
        get_authoritative_ns_hosted_by_asn, get_ixps_for_asn, get_co_located_asns_for_asn]

