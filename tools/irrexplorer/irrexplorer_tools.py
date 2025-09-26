from langchain_core.tools import tool
from tools.irrexplorer.irrexplorer_aux import *

# Tool 1 - return number of originated prefixes of ASN according IRR data
@tool
def num_of_originated_prefixes(asn):
    '''Given ASN, return the number of originated prefixes accordint IRR entry''' 
    return num_of_as_originated_prefixes(asn)

# Tool 2 - fetch all originated prefixes of ASN according IRR data
@tool
def originated_prefixes(asn):
    '''Given ASN, return all its originated prefixes'''
    return as_originated_prefixes(asn)

# Tool 3 - fetch all RPKI invalid prefixes according IRR data for AS
@tool
def rpki_invalid_as_prefixes(asn):
    '''Given ASN, return all its invalid rpki prefixes'''
    return invalid_prefixes_of_as(asn)

# Tool 4 - return all suspicious IP prefixes of an AS according IRR data
@tool 
def suspicious_prefixes(asn):
    '''Given ASN, return all suspicious IP prefixes of an AS according IRR data'''
    return suspicious_prefixes_of_as(asn)

# Tool 5 - return all the overlapped prefixes of an AS according IRR data
@tool
def overlapped_prefixes(asn):
    '''Given ASN, return all the overlapped prefixes of an AS according IRR data'''
    return overlaps_prefixes_of_as(asn)

# Tool 6 - IP2ASN according IRR data
@tool
def ip2asn_irr(ip):
    '''Perform ip->ASN mapping according IRR data'''
    return ip2asn(ip)

# Tool 7 - get IP IRR data
@tool
def ip_irr_data(ip):
    '''Return IRR data for IP prefix or address'''
    return get_ip_irr_data(ip)

# Tool 8 - get IP RPKI data
@tool
def ip_rpki_data(ip):
    '''Return RPKI data for IP prefix or address'''
    return get_ip_rpki_data(ip)

# Tool 9 - get RIRs for IP prefix or address
@tool
def ip_rir(ip):
    '''Return RIRs for IP prefix or address'''
    return get_ip_rir(ip)

# Tool 10 - get RPKI status for IP prefix or address
@tool
def rpki_status(ip):
    '''get RPKI status for IP prefix or address'''
    return ip_rpki_status(ip)

# Tool 11 - get RPKI last modified status for IP prefix or address
@tool
def ip_rpki_last_modified(ip):
    '''get RPKI last modified status for IP prefix or address'''
    return get_ip_rpki_last_modified(ip)

# Tool 12 - get IRR max length
@tool
def rpki_max_length(ip):
    '''Get IRR max length for an IP address or prefix'''
    return ip_rpki_max_length(ip)

# Tool 13 - get IRR routes data for an IP prefix or address
@tool
def irr_routes_data_for_an_ip(ip):
    '''get IRR routes data for an IP prefix or address'''
    return get_irr_routes_data_for_an_ip(ip)

# Tool 14 - Get IRR IP address category overall status
@tool
def get_category_overall(ip):
    '''Get category overall status (risk) for an IP address''' 
    return get_irr_ip_risk_status(ip)

# Tool 15 - Get origin ASes for an IP address from IRR
@tool
def irr_ip_origin_ases(ip):
    '''Get origin ASes for an IP address from IRR'''
    return get_irr_ip_origin_ases(ip)

# Tool 16 - Get IRR messages and alerts for an IP address
@tool
def irr_ip_messages(ip):
    '''Get IRR messages and alerts for an IP address'''
    return get_irr_ip_messages(ip)

# Tool 17 - get as path for an as-set
@tool
def as_path_as_set(as_set):
    '''get as path for an as-set''' 
    return get_as_set_path(as_set)

# Tool 18 - get as-set members
@tool
def as_set_members(as_set):
    '''get as-set members''' 
    return get_as_set_members(as_set)

# Tool 19 - get route set path
@tool
def route_set_path(as_set):
    '''get route set path''' 
    return get_route_set_path(as_set)

# Tool 20 - get route set members
@tool
def route_set_members(route_set):
    '''get route set members''' 
    return get_route_set_members(route_set)

# IRRExplorer tools list
irrexplorer_tools = [num_of_originated_prefixes, originated_prefixes, 
                    rpki_invalid_as_prefixes, suspicious_prefixes, overlapped_prefixes, ip2asn_irr,
                    ip_irr_data, ip_rpki_data, ip_rir, rpki_status, ip_rpki_last_modified, rpki_max_length,
                    irr_routes_data_for_an_ip, get_category_overall, irr_ip_origin_ases, irr_ip_messages,
                    as_path_as_set, as_set_members, route_set_path, route_set_members]
