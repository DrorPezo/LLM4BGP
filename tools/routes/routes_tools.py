from langchain_core.tools import tool
from tools.routes.routes_aux import *

# Tool 1 - Find the shortest path between two ASes
# @tool(return_direct=True)
@tool
def find_shortest_path(asn1, asn2):
    """
    Given two ASNs of ASes, return the shortest path between them.
    Input: ASN1 (int), ASN2(int)
    Output: List of ASNs which describe the ASes in the shortest path between the ASes (list)
    """
    return shortest_path(str(asn1), str(asn2))

# Tool 2 - Given a path between ASes, check if it's the shortest path between them
# @tool(return_direct=True)
@tool
def is_the_shortest_path(asn1, asn2, path):
    """
    Given a path between ASes, check if it's the shortest path between them
    Input: ASN1 (int), ASN2(int)
    Output: List of ASNs which describe the ASes in the shortest path between the ASes (list of strings)
    """
    return is_shortest_path(str(asn1), str(asn2), path)

# Tool 3 - Given two ASes and a path length, return all the paths between the ASes with the given length
@tool
def paths_with_length(asn1, asn2, length):
    """
    Given two ASes, find all the paths of a given length
    Input: ASN1 (int), ASN2(int)
    Output: List of ASNs which describe the ASes in the shortest path between the ASes (list of strings)
    """
    return paths_with_exact_length(str(asn1), str(asn2), int(length))

# Tool 4 - Given a ASN, return the moas prefixes of the AS
@tool
def asn_moas_prefixes(asn):
    """
    Given ASN return all the moas prefixes of the AS
    Input: ASN (string)
    Output: List of MOAS prefixes (list)
    """
    return moas_prefixes_for_asn(int(asn))

# Tool 5 - Given a prefix, check if it's moas
@tool
def check_if_prefix_is_moas_bgpstream(prefix):
    """
    Given a prefix, check if it's a moas prefix
    Input: Prefix (string)
    Output: True / False
    """
    return check_if_prefix_is_moas_bgp_stream(prefix)

# Tool 6 - Get as degree
def get_as_degree_from_as_graph(asn):
    '''
    Returns the degree of an ASN from BGPStream indirected AS graph.
    Input: asn (int | str)
    Output: degree (int)
    '''
    return as_degree_from_as_graph(asn)

# Tool 7 - Get Random routes
@tool
def get_random_routes_with_asns(asn1, asn2):
    '''
    Returns a random list of m routes where asn1 and asn2 are adjacent
    Input: asn1 (int | str), asn2 (int | str), m (int | str)
    Output: list
    '''
    return choose_random_routes(asn1, asn2, 30)

# bgpstream tools list
bgp_stream_tools = [find_shortest_path, is_the_shortest_path, paths_with_length, asn_moas_prefixes, check_if_prefix_is_moas_bgpstream]


