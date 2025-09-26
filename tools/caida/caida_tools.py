from langchain_core.tools import tool
from tools.caida.as_rank_aux import *
from tools.caida.as2org_aux import *
from tools.caida.tor_aux import *

# Tools for fetching AS rank data for a given ASN

# Tool 1 - Returns the CAIDA AS rank for the given ASN by calling to CAIDA AS Rank API
# @tool(return_direct=True)
@tool
def as_rank(asn):
    '''
    Returns the CAIDA AS rank for the given ASN by calling to CAIDA AS Rank API.
    Input: ASN - int | str
    Output: AS Rank - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    elif data['data']:
        if data['data']['asn']:
            if data['data']['asn']['rank']:
                return int(data['data']['asn']['rank']) 
    return -1

# Tool 2 - Returns the numbers of ASes that are observed to be in a given ASN customers cone
# @tool(return_direct=True)
@tool
def as_cone_size(asn):
    '''
    Returns the numbers of ASes that are observed to be in a given ASN customers cone.
    Input: asn - int | str
    Output: as cone size - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        return int(data['data']['asn']['cone']['numberAsns'])

# Tool 3 - return the number of announced prefixes for a given ASN
# @tool(return_direct=True)
@tool
def num_of_announced_prefixes(asn):
    '''
    Returns number of prefixes which observed to be in the customer cone of a given ASN.
    Input: asn - int | str
    Output: number of announced prefixes - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        return int(data['data']['asn']['cone']['numberPrefixes'])

# Tool 4 - returns number of announced IP addreses for the given ASN
# @tool(return_direct=True)
@tool
def num_of_announced_addresses(asn):
    '''
    Returns number of announced IP addreses for the given ASN.
    Input: asn - int | str
    Output: number of announced IP addresses - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        return int(data['data']['asn']['cone']['numberAddresses'])

# Tool 5 - return the organization of the AS
# @tool(return_direct=True)
@tool
def caida_as_rank_as2org(asn):
    '''
    Returns organization which originates the ASN.
    Input: asn - int | str
    Output: Organization name - str
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return '-1'
    else:
        return str(data['data']['asn']['organization']['orgName'])

# Tool 6 - Returns the name of a AS given its ASN from CAIDA AS rank API
# @tool(return_direct=True)
@tool
def as_rank_as_name(asn):
    '''
    Returns the name of a AS given its ASN from CAIDA AS rank API.
    Input: asn - int | str
    Output: AS name - str
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return '-1'
    else:
        return str(data['data']['asn']['asnName'])

# Tool 7 - Returns True / False weather the AS is reachable
# @tool(return_direct=True)
@tool
def is_seen(asn):
    '''
    Returns True / False weather the AS is reachable.
    Input: asn - int | str
    Output: 1 for seen, 0 for unseen - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        if str(data['data']['asn']['seen']) == 'True':
          return 1
        else:
          return 0

# Tool 8 - Returns True / False if the AS is a clique member
# @tool(return_direct=True)
@tool
def is_clique_member(asn):
    '''
    Returns True / False if the AS is a clique member.
    Input: asn - int | str
    Output: 1 if the AS is a clique member, 0 if not - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        if str(data['data']['asn']['cliqueMember']) == 'True':
          return 1
        else:
          return 0

# Tool 9 - Returns the country which the AS is located in
# @tool(return_direct=True)
@tool
def as2country(asn):
    '''
    Returns the country which the AS is located in.
    Input: asn - int | str
    Output: country's name - str
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return '-1'
    else:
        return str(data['data']['asn']['country']['name'])

# Tool 10 - Returns the peers number of an AS given its ASN
# @tool(return_direct=True)
@tool
def as_peers_num(asn):
    '''
    Returns the peers number of an AS given its ASN.
    Input: asn - int | str
    Output: number of AS peers - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        return int(data['data']['asn']['asnDegree']['peer'])

# Tool 11 - Returns the providers number of an AS given its ASN
# @tool(return_direct=True)
@tool
def as_providers_num(asn):
    '''
    Returns the providers number of an AS given its ASN.
    Input: asn - int | str
    Output: number of AS providers - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        return int(data['data']['asn']['asnDegree']['provider'])

# Tool 12 - Returns number of ASes which the given ASN gives them transit service
# @tool(return_direct=True)
@tool
def as_transit_num(asn):
    '''
    Returns number of ASes which the given ASN gives them transit service.
    Input: asn - int | str
    Output: number of ASes which the given ASN gives them transit service - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        return int(data['data']['asn']['asnDegree']['transit'])

# Tool 13 - Returns the customers number of an AS given its ASN
# @tool(return_direct=True)
@tool
def as_customers_num(asn):
    '''
    Returns the customers number of an AS given its ASN.
    Input: asn - int | str
    Output: number of AS customers - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        return int(data['data']['asn']['asnDegree']['customer'])

# Tool 14 - Returns the degree of an AS given its ASN
# @tool(return_direct=True)
@tool
def as_degree(asn):
    '''
    Returns the degree of an AS given its ASN.
    Input: asn - int | str
    Output: AS degree - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        return int(data['data']['asn']['asnDegree']['total'])

# Tool 15 - Returns the number of siblings of AS given its ASN
# @tool(return_direct=True)
@tool
def as_siblings_num(asn):
    '''
    Returns the number of siblings of AS given its ASN.
    Input: asn - int | str
    Output: number of siblings - int
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        return int(data['data']['asn']['asnDegree']['sibling'])

# Tool 16 - Returns the location of AS given its ASN
# @tool(return_direct=True)
@tool
def asn2loc(asn):
    '''
    Returns the location of AS given its ASN.
    Input: asn - int | str
    Output: longtitude and latitute - dict
    '''
    asn = str(asn)
    data = get_as_rank_data(extract_numbers(asn))
    if data == -1:
        return -1
    else:
        return {
          "longitude": float(data['data']['asn']['longitude']),
          "latitude": float(data['data']['asn']['latitude'])
        }

# Tool 17 - Find ASN of largest AS of given org name
# @tool(return_direct=True)
@tool
def find_org_largest_asn(org: str) -> int:
    """
    Recieve an organization name and return the ASN of the largest ASN owned by the organization
    Input: organization name (stirng)
    Output: ASN of the largest organization that the organization owns (int)
    """
    return findLargestASN(org)

# Tool 18 - Find the organization for a target AS
# @tool(return_direct=True)
@tool
def as2org(asn:int) -> str:
    """
    Receive ASN and return the organization name for the AS with the given ASN
    Input: ASN (int)
    Output: orhanization name (string)
    If you recieve from the tool -1 then the organization name is not corrent or doesn't exist
    """
    return fetch_org(str(asn))

# Tool 19 - Find number of ASNs in an organization
# @tool(return_direct=True)
@tool
def org_as_count(org: str) -> int:
    """
    Given organization name, return the number of the ASes owned by the organization
    Input: organization name (stirng)
    Output: Number of ASes which the organization owns (int)
    """
    return len(current_as2org(org))

# Tool 20 - Fetch a list of ASNs owned by an organization
# @tool(return_direct=True)
@tool
def list_of_current_org_asns(org: str) -> list:
    """
    Given organization name, reutrn the list of ASes owned by the organization
    Input: Organization name (stirng)
    Output: ASNs of the ASes owned by the organization (list of int)
    """
    return current_as2org(org)

# Tool 21 - Return the biggest organization in a given country
# @tool(return_direct=True)
@tool
def largest_org_in_a_country(country):
    """
    Given country name, reutrn the organization with the biggest number of ASes owned by the organization
    Input: Country name (string)
    Output: Organization's name (string)
    """
    return largets_org_in_a_country(country)

# Tool 22 - Return the biggest AS in a given country
# @tool(return_direct=True)
@tool
def largest_as_in_a_country(country):
    """
    Given country name, reutrn the AS with the largest customers cone
    Input: Country name (string)
    Output: ASN (int)
    """
    largest_as_in_a_country(country)

# Tool 23 - Returns the number of organizations in a country
# @tool(return_direct=True)
@tool
def num_of_orgs_in_country(country):
    """
    Given country name, reutrn the number of organizations with at least one ASN registered in the country
    Input: Country name (string)
    Output: Number of organizations (int)
    """
    orgs_num, orgs = num_of_orgs_in_a_country(country)
    return orgs_num

# Tool 24 - Returns the number of ASes in a country
# @tool(return_direct=True)
@tool
def num_of_ases_in_country(country):
    """
    Given country name, reutrn the number of ASes registered in the country
    Input: Country name (string)
    Output: Number of ASes (int)
    """
    asns_num, asns = num_of_as_in_a_country(country)
    return asns_num

# Tool 25 - Returns the ToR between two ASes according CAIDA dataset
# @tool(return_direct=True)
@tool
def get_caida_tor(asn1, asn2):
    """
    Given two ASNs, reutrn the number of relationship between the ASes represented by the ASNs according CAIDA dataset
    Input: ASN1 (int), ASN2(int)
    Output: AS relationship dictionary (dict)
    """
    rels = get_tor(asn1, asn2)
    return rels

# Tool 26 - get siblings for a given ASN
@tool
def get_siblings(asn):
    """
    Given ASN, reutrn a list of its siblings according CAIDA dataset
    Input: ASN (int)
    Output: AS siblings dictionary (dict)
    """
    org = as2org(asn)
    sibs = current_as2org(org)
    return {'ASN': asn,
            'Siblings': sibs
           }

# as2org tools list
as2org_tools = [find_org_largest_asn, 
                as2org, 
                org_as_count, 
                list_of_current_org_asns, 
                largest_org_in_a_country, 
                largest_as_in_a_country,
                num_of_orgs_in_country,
                num_of_ases_in_country
                ]
    
# as rank tools list
caida_as_rank_tools = [
    as_rank,
    as_cone_size,
    num_of_announced_prefixes,
    num_of_announced_addresses,
    caida_as_rank_as2org,
    as_rank_as_name,
    is_seen,
    is_clique_member,
    as2country,
    as_peers_num,
    as_providers_num,
    as_transit_num,
    as_customers_num,
    as_degree,
    as_siblings_num,
    asn2loc
]

tor_tools = [
    get_caida_tor,
    get_siblings
]
