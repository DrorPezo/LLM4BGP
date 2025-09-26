from langchain_core.tools import tool
from tools.peeringdb.peeringdb_aux import *

# Tools for fetching peeringdb data

@tool
def get_as_deployed_inf_data(asn):
    '''
    Returns a list with infrastructures where the AS is deployed.
    Input: asn (int)
    Output: list of dictionaries which describe each deployement (dict)
    '''
    dep_infs = get_netfac_data(int(asn))
    return dep_infs.to_dict(orient="records")

@tool
def get_as_deployed_inf_data_in_country(asn, country_code):
    '''
    Returns a list with infrastructures where the AS is deployed in a certain country.
    Input: asn (int), country_code (str)
    Output: list of dictionaries which describe each deployement (dict)
    '''
    dep_infs = get_netfac_data(int(asn))
    country_deployed_infs = dep_infs.loc[dep_infs["country"].eq(country_code)]
    return country_deployed_infs.to_dict(orient="records")

@tool 
def get_asn_poc(asn):
    '''
    Returns contact data for ASN maintanance.
    Input: asn (int)
    Output: contact data for ASN (dict)
    '''
    data = get_poc_data_for_asn(asn)
    return data.to_dict(orient="records")

@tool
def pdb_org_location(org_name):
    '''
    Given org_name, returns information about its geolocation.
    Input: organization name (str)
    Output: information about its geolocation (dict)
    '''
    return get_org_location_data('Equinix, Inc.').to_dict(orient="records")

@tool
def pdb_get_org_sm_data(org_name):
    '''
    Get web and social media data for a given organization.
    Input: Organizatin name (str)
    Output: List of dictionaries containing web and social media data for the organization.
    '''
    return pdb_get_org_web_data(org_name)

@tool
def pdb_get_org_notes(org_name):
    '''
    Get important peeringdb notes for a given organization.
    Input: organization name (str)
    Output: notes (str)
    '''
    org_data = get_org_data(org_name)
    return org_data[["notes"]].to_dict(orient='records')[0]['notes']

@tool
def pdb_get_org_suite(org_name):
    '''
    Get important peeringdb suite data for a given organization.
    Input: organization name (str)
    Output: suite data(str)
    '''
    org_data = get_org_data(org_name)
    return org_data[["suite"]].to_dict(orient='records')[0]['suite']

@tool
def pdb_get_org_aka(org_name):
    '''
    Get important peeringdb aka information for a given organization.
    Input: organization name (str)
    Output: aka information (str)
    '''
    org_data = get_org_data(org_name)
    return org_data[["aka"]].to_dict(orient='records')[0]['aka']

@tool
def get_carrier_sm_data(org_name):
    '''
    Get carrier social media data
    Input: organization name (str)
    Output: social media links (list)
    '''
    carrier_data = get_carrier_data(org_name)
    if carrier_data['social_media']:
        return carrier_data['social_media'].tolist()
    else:
        return []

@tool
def get_carrier_aka_data(org_name):
    '''
    Get carrier aka data
    Input: organization name (str)
    Output: aka data (list)
    '''
    carrier_data = get_carrier_data(org_name)
    if carrier_data['aka']:
        return carrier_data['aka'].tolist()
    else:
        return []

@tool
def get_carrier_fac_count(org_name):
    '''
    Get carrier facilities count
    Input: organization name (str)
    Output: facilities count (int)
    '''
    carrier_data = get_carrier_data(org_name)
    return carrier_data['fac_count'].tolist()[0]

@tool
def get_carrier_notes(org_name):
    '''
    Get carrier notes
    Input: organization name (str)
    Output: facilities note (dict)
    '''
    carrier_data = get_carrier_data(org_name)
    data = carrier_data['notes'].to_dict()
    return data

@tool
def get_fec_location_for_org(org_name):
    '''
    Return a dictionary with information about facilities locations of a given organization.
    Input: organization name (str)
    Output: dictionary with information about facilities locations of a given organization
    '''
    return get_fac_location(org_name).to_dict(orient='records')

@tool
def org2ases_pdb(org_name):
    '''
    Returns a list of ASNs for a given organization name.
    Input: Organization name (str)
    Output: List of ASNs (int)
    '''
    org_id = get_org_id(org_name)
    if org_id > -1:
      data = get_asn_basic_data_via_org_id(org_id)
      return data['asn'].tolist()
    else:
      return []

@tool
def get_asn_aka(asn):
    '''
    Returns aka data for an ASN.
    Input: ASN (int)
    Output: List of aka (str)
    '''
    asn_data = get_asn_basic_data(asn)
    return list(asn_data["aka"])

@tool
def get_as_looking_glass(asn):
    '''
    Returns looking glass link for an ASN
    Input: asn (int)
    Output: looking glass link (str)
    '''
    lg = get_asn_lg_rs_data(asn)
    return lg.loc[lg["asn"].eq(asn), "looking_glass"].iat[0]

@tool
def get_as_route_server(asn):
    '''
    Returns route server link for an ASN
    Input: asn (int)
    Output: route server link (str)
    '''
    rs = get_asn_lg_rs_data(asn)
    return rs.loc[rs["asn"].eq(asn), "looking_glass"].iat[0]

@tool
def get_net_dac_data(asn):
    '''
    Returns network facilities data for ASN
    Input: asn (int)
    Output: network facilities data (dict)
    '''
    net_fac_data = pdb['net'][["name", "asn", "fac_count", "info_unicast", "ix_count", "org_id"]]
    return net_fac_data.loc[net_fac_data["asn"].eq(asn)].to_dict('records')

@tool
def get_net_policy_data(asn):
    '''
    Returns network policy data for ASN
    Input: asn (int)
    Output: network policy data (dict)
    '''
    net_policy_data = pdb['net'][["name", "asn", "rir_status", "policy_ratio", "policy_general", "policy_locations", "policy_url"]]
    return net_policy_data.loc[net_policy_data["asn"].eq(asn)].to_dict('records')

@tool
def pdb_as_type_info_type(asn):
    '''
    Returns AS type according info_type field
    Input: asn (int)
    Output: AS possible types (list)
    '''
    as_type = get_as_type_data(asn)
    return list(as_type['info_type'])

@tool
def pdb_as_type_info_types(asn):
    '''
    Returns AS type according info_types field
    Input: asn (int)
    Output: AS possible types (list)
    '''
    as_type = get_as_type_data(asn)
    return list(as_type['info_types'])

@tool
def org_asn_types_info_types(org_name):
    '''
    Returns organization type according info_types field
    Input: organization name (str)
    Output: organization possible types (dict)
    '''
    data = org_type(org_name)
    return data[['info_types']].to_dict(orient="records")

@tool
def org_asn_types_info_type(org_name):
    '''
    Returns organization type according info_type field
    Input: organization name (str)
    Output: organization possible types (dict)
    '''
    data = org_type(org_name)
    return data[['info_type']].to_dict(orient="records")

@tool
def get_net_traffic_data_for_asn(asn):
    '''
    Returns network traffic data for an ASN
    Input: ASN (int)
    Output: network traffic data (dict)
    '''
    return get_net_traffic_info_data(asn)[['info_ratio', 'info_traffic', 'info_multicast', 'info_scope', "info_prefixes4", "info_prefixes6"]].to_dict(orient='records')

@tool
def get_net_traffic_aka_data_for_asn(asn):
    '''
    Returns network traffic aka data for an ASN
    Input: ASN (int)
    Output: network traffic aka data (dict)
    '''
    return get_net_traffic_info_data(asn)[['aka']].to_dict(orient='records')

@tool
def get_traffic_data_for_org(org_name):
    '''
    Returns traffic data for an organization
    Input: organization name (str)
    Output: network traffic data (dict)
    '''
    org_id = get_org_id(org_name)
    net_info_data = pdb['net'][["name", "asn", "info_ratio", "info_traffic", "info_multicast", "info_scope", "org_id", "aka", "info_prefixes4", "info_prefixes6"]]
    return net_info_data.loc[net_info_data["org_id"].eq(org_id)].to_dict(orient='records')

@tool
def pdb_get_as_as_set(asn):
    '''
    Returns AS_SET for ASN
    Input: ASN (int)
    Output: AS SET (str)
    '''
    net_additional_notes = pdb['net'][["name", "asn", "notes", "irr_as_set"]]
    return net_additional_notes.loc[net_additional_notes["asn"].eq(asn)][['irr_as_set']].values

@tool
def get_net_notes(asn):
    '''
    Returns notes for ASN
    Input: ASN (int)
    Output: notes (str)
    '''
    net_pdb_as_set = pdb['net'][["name", "asn", "notes", "org_id"]]
    return net_pdb_as_set.loc[net_pdb_as_set["asn"].eq(asn)][['notes']].values.tolist()[0]

@tool
def get_org_notes(org_name):
    '''
    Returns notes for all ASNs in a given organization
    Input: organization name (str)
    Output: notes (dict)
    '''
    org_id = get_org_id(org_name)
    net_pdb_as_set = pdb['net'][["name", "asn", "notes", "org_id"]]
    return net_pdb_as_set.loc[net_pdb_as_set["org_id"].eq(org_id)][['notes']].to_dict(orient='records')

@tool
def get_net_web_data(asn):
    '''
    Returns web data for ASN
    Input: ASN (int)
    Output: web data (dict)
    '''
    net_pdb_web_data = pdb['net'][["name", "asn", "website", "social_media"]]
    return net_pdb_web_data.loc[net_pdb_web_data["asn"].eq(asn)].to_dict(orient='records')

@tool
def get_ixfac_data_for_asn(asn):
    '''
    Returns ixfac data for ASN
    Input: ASN (int)
    Output: ixfac data (dict)
    '''
    fac_ids = get_asn_fac_id(asn)
    df = pd.DataFrame()
    for fac_id in fac_ids:
        all_fac_data = [get_ixfac_data(fac_id) for fac_id in fac_ids]
        df = pd.concat(all_fac_data, ignore_index=True)
    return df[['city', 'name', 'country']].to_dict('records')

@tool
def get_carrierfac_data_for_asn(asn):
    '''
    Returns carrierfac data for ASN
    Input: ASN (int)
    Output: carrierfac data (dict)
    '''
    fac_ids = get_asn_fac_id(asn)
    df = pd.DataFrame()
    for fac_id in fac_ids:
        all_fac_data = [get_carrierfac_data(fac_id) for fac_id in fac_ids]
        df = pd.concat(all_fac_data, ignore_index=True)
    return df.to_dict('records')

@tool
def get_netixlan_data(asn):
    '''
    Returns netixlan data for ASN
    Input: ASN (int)
    Output: netixlan data (dict)
    '''
    netixlan = pdb['netixlan'][["ix_id", "operational", "asn", "name", "speed", "notes", "ipaddr4", "status", "is_rs_peer", "bfd_support"]]
    return netixlan.loc[netixlan["asn"].eq(asn)].to_dict('records')

@tool
def get_ix_data_by_org_name(org_name):
    '''
    Returns ix data for organization
    Input: organization name (str)
    Output: netixlan data (dict)
    '''
    org_id = get_org_id(org_name)
    ix_data = get_ix_status_by_org_id(org_id)
    return ix_data[['name', 'name_long', 'status', 'net_count', 'notes', 'aka', 'fac_count', 'ixf_net_count']].to_dict(orient='records')

@tool
def get_ix_service_data_by_org_name(org_name):
    '''
    Returns ix data for organization
    Input: organization name (str)
    Output: ix service data (dict)
    '''
    org_id = get_org_id(org_name)
    ix_service_data = pdb['ix'][['org_id', 'name', 'name_long', 'service_level', 'terms', 'status', 'url_stats', 'sales_phone', 'tech_email', 'policy_phone', 'policy_email', 'sales_email', 'tech_phone', 'media', 'website', 'social_media']]
    return ix_service_data.loc[ix_service_data["org_id"].eq(org_id)].to_dict('records')

@tool
def get_ix_location(name):
    '''
    Returns ix location data for organization
    Input: ix name (str)
    Output: ix location data (dict)
    '''
    ix_location = pdb['ix'][['org_id', 'name', 'name_long', 'region_continent', 'country', 'city']]
    return ix_location.loc[ix_location["name"].eq(name)]

@tool
def get_campus_location_data(org_name):
    '''
    Returns campus location data for organization
    Input: organization name (str)
    Output: campus location data (dict)
    '''
    pdb_campus_data = pdb["campus"][["org_name", "name", "name_long", "status", "country", "state", "city", "zipcode", "website", "social_media"]]
    return pdb_campus_data.loc[pdb_campus_data["org_name"].eq(org_name)].to_dict('records')

@tool
def get_campus_notes(org_name):
    '''
    Returns campus location data for organization
    Input: organization name (str)
    Output: campus notes data (dict)
    '''
    pdb_campus_data = pdb["campus"][["org_name", "name", "name_long", "status", "country", "state", "city", "zipcode", "website", "aka", "social_media", "notes"]]
    data = pdb_campus_data.loc[pdb_campus_data["org_name"].eq(org_name)]
    return dict(data["notes"])

@tool
def get_campus_aka_data(org_name):
    '''
    Returns AKA campus data for an organization
    Input: organization name (str)
    Output: AKA campus data (dict)
    '''
    pdb_campus_data = pdb["campus"][["org_name", "name", "name_long", "status", "country", "state", "city", "zipcode", "website", "aka", "social_media", "notes"]]
    data = pdb_campus_data.loc[pdb_campus_data["org_name"].eq(org_name)]
    return dict(data["aka"])

@tool
def pdb_based_as2org(asn):
    '''
    Given ASN, return the organization's name which owns the AS.
    Input: ASN (int)
    Output: organization's name (str)
    '''
    return pdb_as2org(int(asn))

pdb_ases_tools = [get_as_deployed_inf_data, get_as_deployed_inf_data_in_country,
                    get_asn_poc, get_asn_aka, get_as_looking_glass, get_as_route_server, 
                    get_net_dac_data, get_net_policy_data, pdb_as_type_info_type, pdb_as_type_info_types,
                    get_net_traffic_data_for_asn, get_net_traffic_aka_data_for_asn, pdb_get_as_as_set, 
                    get_net_notes, get_net_web_data, get_ixfac_data_for_asn,
                    get_carrierfac_data_for_asn, get_netixlan_data, pdb_based_as2org]

pdb_orgs_tools = [pdb_org_location, pdb_get_org_sm_data, pdb_get_org_notes,
                    pdb_get_org_suite, pdb_get_org_aka, org2ases_pdb, org_asn_types_info_types, 
                    org_asn_types_info_type, get_traffic_data_for_org, get_org_notes, 
                    get_ix_data_by_org_name, get_ix_service_data_by_org_name]

pdb_carrier_tools = [get_carrier_sm_data, get_carrier_fac_count, 
                    get_carrier_notes, get_fec_location_for_org]

pdb_ix_tools = [get_ix_data_by_org_name, get_ix_service_data_by_org_name, get_ix_location]

pdb_campus_tools = [get_campus_location_data, get_campus_notes, get_campus_aka_data]
