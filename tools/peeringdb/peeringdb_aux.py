import json
import pandas as pd

peeringdb_path = 'tools/peeringdb/peeringdb_latest.json'
with open(peeringdb_path, 'r') as file:
    data_dict = json.load(file)

def field_data_normalization(contents):
   pdb = {}
   for key in contents.keys():
      if "data" in contents[str(key)].keys():
         pdb[key] = pd.json_normalize(data=contents[str(key)], record_path='data')
   
   return pdb

pdb = field_data_normalization(data_dict)

def get_netfac_data(asn):
    target_asn = str(asn)
    netfac = pdb['netfac'][["local_asn", "country", "city", "status", "name", "fac_id", "net_id"]]
    return netfac.loc[netfac["local_asn"].eq(target_asn)]

def get_net_id_for_asn(asn):
    netfac = get_netfac_data(asn)
    return list(set(netfac["net_id"]))

def get_poc_data_for_asn(asn):
    net_ids =  get_net_id_for_asn(asn)
    poc_data = pdb["poc"][["net_id", "name", "status", "visible", "email", "role", "phone", "url"]]
    poc_datas = []
    for net_id in net_ids:
        data = poc_data.loc[poc_data["net_id"].eq(net_id)]
        poc_datas.append(data)
    if poc_datas:
        return pd.concat(poc_datas)
    else:
        return []

def get_org_location_data(org_name):
    pdb_org_data = pdb["org"][["name", "country", "state", "address2", "address1", "longitude", "latitude", "zipcode"]]
    return pdb_org_data.loc[pdb_org_data["name"].eq(org_name)]

def get_org_data(org_name):
    pdb_org_data = pdb["org"][["name", "name_long", "website", "notes", "status", "social_media", "suite", "aka"]]
    return pdb_org_data.loc[pdb_org_data["name"].eq(org_name)]

def pdb_get_org_web_data(org_name):
    org_data = get_org_data(org_name)
    if org_data[["social_media"]].to_dict(orient='records'):
      return org_data[["social_media"]].to_dict(orient='records')[0]['social_media']
    else:
      return []

def get_carrier_data(org_name):
    pdb_carrier_data = pdb["carrier"][['name', 'name_long', 'org_name', 'website', 'notes', 'org_id', 'status', 'social_media', 'aka', 'fac_count']]
    return pdb_carrier_data.loc[pdb_carrier_data["org_name"].eq(org_name)]

def get_fac_data(org_name):
    fac_data = pdb["fac"][["name", "org_name", "org_id", "social_media", "aka", "property", "net_count", "suite", "status", "ix_count", "website", "tech_email", "campus_id", "sales_email", "tech_phone", "notes"]]
    return fac_data.loc[fac_data["org_name"].eq(org_name)]

def get_org_id(org_name):
    print(org_name)
    org_data = get_fac_data(org_name)
    # print(org_data['org_id'])
    if len(org_data['org_id']) == 0:
        return -1
    return list(set(org_data['org_id']))[0]

def get_fac_location(org_name):
    pdb_fac_location_data = pdb["fac"][["name", "name_long", "org_name", "org_id", "region_continent", "country", "state", "city", "latitude", "longitude", "address1", "address2", "zipcode"]]
    return pdb_fac_location_data.loc[pdb_fac_location_data["org_name"].eq(org_name)]

def get_asn_basic_data_via_org_id(org_id):
    net_basic_data = pdb['net'][["name", "asn", "aka", "org_id"]]
    return net_basic_data.loc[net_basic_data["org_id"].eq(org_id)]

def get_asn_basic_data(asn):
    net_basic_data = pdb['net'][["name", "asn", "aka", "org_id"]]
    return net_basic_data.loc[net_basic_data["asn"].eq(asn)]

def get_asn_lg_rs_data(asn):
    net_lg_rs_data = pdb['net'][["name", "asn", "looking_glass", "route_server"]]
    return net_lg_rs_data.loc[net_lg_rs_data["asn"].eq(asn)]

def get_as_type_data(asn):
    net_as_type = pdb['net'][["name", "asn",  "name_long", "info_types", "info_type", "org_id"]]
    return net_as_type.loc[net_as_type["asn"].eq(asn)][['info_type', 'info_types']]  

def pdb_as2org(asn):
    pdb["org"]["org_id"] = pdb["org"]["id"]
    pdb["org"]["name_org"] = pdb["org"]["name"]

    org_fields = pd.merge(
        pdb["org"][['org_id', 'name_org']], 
        pdb["net"], 
        on="org_id", 
        how="inner"
    )
    org_fields = org_fields[["asn", "name_org", "org_id"]]
    if org_fields.loc[org_fields['asn'] == asn]['name_org'].tolist():
      return str(org_fields.loc[org_fields['asn'] == asn]['name_org'].iat[0])
    else:
      return -1 
    
def org_type(org_name):
    org_id = get_org_id(org_name)
    net_org_type = pdb['net'][["name", "asn",  "name_long", "info_types", "info_type", "org_id"]]
    data = net_org_type.loc[net_org_type["org_id"].eq(org_id)]
    return data[['asn', 'info_types']]

def get_net_traffic_info_data(asn):
    net_info_data = pdb['net'][["name", "asn", "info_ratio", "info_traffic", "info_multicast", "info_scope", "org_id", "aka", "info_prefixes4", "info_prefixes6"]]
    return net_info_data.loc[net_info_data["asn"].eq(asn)]

def get_asn_fac_id(asn):
    target_asn = str(asn)
    netfac = pdb['netfac'][["local_asn", "country", "city", "status", "name", "fac_id", "net_id"]]
    values = list(netfac.loc[netfac["local_asn"].eq(asn)][["fac_id"]].values)
    return [a.item() for a in values] 

def get_ixfac_data(fac_id):
    ixfac = pdb['ixfac'][["ix_id", "city", "name", "country", "fac_id", "status"]]
    return ixfac.loc[ixfac["fac_id"].eq(fac_id)]

def get_carrierfac_data(fac_id):
    carrierfac = pdb['carrierfac'][["status", "name", "fac_id", "carrier_id"]]
    return carrierfac.loc[carrierfac["fac_id"].eq(fac_id)]

def get_ix_status_by_org_id(org_id):
    ix_status = pdb['ix'][['org_id', 'name', 'name_long', 'status', 'net_count', 'notes', 'aka', 'fac_count', 'ixf_net_count']]
    return ix_status.loc[ix_status["org_id"].eq(org_id)]

def get_ix_service_data(name):
    ix_service_data = pdb['ix'][['org_id', 'name', 'name_long', 'service_level', 'terms', 'status', 'url_stats', 'sales_phone', 'tech_email', 'policy_phone', 'policy_email', 'sales_email', 'tech_phone', 'media', 'website', 'social_media']]
    return ix_service_data.loc[ix_service_data["name"].eq(name)]

# netixlan = data_dict['netixlan']['data']
# net = data_dict['net']['data']
# fac = data_dict['fac']['data']

# def netixlan_record_by_asn(asn):
#    return next((record for record in netixlan if record.get("asn") == asn), None)

# def fac_record_by_asn(asn):
#    return next((record for record in net if record.get("asn") == asn), None)

# def W(org):
#    return next((record for record in net if record.get("name") == org), None)

# def fac_record_by_org(org_name):
#    return next((record for record in fac if record.get("org_name") == org_name), None)
   