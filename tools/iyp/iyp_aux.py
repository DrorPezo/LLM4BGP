from neo4j import GraphDatabase
import neo4j
import ast 
import subprocess

# Using IYP local instance
# URI = "neo4j://localhost:7687"
# Using IYP public instance
URI = "neo4j://iyp-bolt.ihr.live:7687"
AUTH = ('neo4j', 'password')
db = GraphDatabase.driver(URI, auth=AUTH)

# Get ASN rank data
def get_as_rank_data(asn):
  # Run the script and capture its output
  command = ["python3", "tools/iyp/asrank-download-asn.py", str(asn)]
  result = subprocess.check_output(command, text=True)
  # Safely evaluate the string as a Python literal
  return ast.literal_eval(result)
  
# Tool 1 - Fetch connected ASNs
def fetch_connected_asns(asn):
    query = """
    MATCH (a:AS {asn: $asn})-[:PEERS_WITH]-(peer:AS)
    OPTIONAL MATCH (peer)-[:NAME]->(n:Name)
    OPTIONAL MATCH (peer)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    RETURN c.country_code AS cc, peer.asn AS asn, head(collect(DISTINCT(n.name))) AS name"""
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query, asn=int(asn), database_="neo4j", result_transformer_=neo4j.Result.to_df)
    return pandas_df

# Tool 2 - Fetch Top x upstream ASNs
def fetch_top_hege_upstreams(asn, x=10, ip_version=4):
    query_template = """
    MATCH (a:AS {asn: $asn})-[d:DEPENDS_ON]->(b:AS)
    WHERE a.asn <> b.asn AND d.af = $ip_version
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    RETURN DISTINCT 
        b.asn AS asn, 
        COALESCE(pdbn.name, btn.name, ripen.name) AS name, 
        c.country_code AS cc, 
        100*d.hege AS hegemony_score, 
        'IPv'+d.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the provided ASN and IP version filter.
    pandas_df = db.execute_query(
        query_template,
        asn=int(asn),
        ip_version=int(ip_version),
        database_="neo4j",
        result_transformer_=neo4j.Result.to_df
    )
    
    # Add AS rank data for each row in the result
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
        asn_value = pandas_df.at[index, 'asn']
        as_data = get_as_rank_data(asn_value)
        pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    
    return pandas_df.head(x)

# Toold 3 - Fetch top as rank ustreams
def fetch_top_as_rank_upstreams(asn, x=10, ip_version=4):
    """
    Fetches the top hegemony upstreams for a given ASN, filtered by the IP version (IPv4 or IPv6).

    Parameters:
    - asn (int or str): The autonomous system number.
    - x (int): The number of top results to return.
    - ip_version (int): The IP version to filter by (4 for IPv4, 6 for IPv6). Default is 4.

    Returns:
    - pandas.DataFrame: DataFrame containing the results.
    """
    query_template = """
    MATCH (a:AS {asn: $asn})-[d:DEPENDS_ON]->(b:AS)
    WHERE a.asn <> b.asn AND d.af = $ip_version
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    RETURN DISTINCT 
        b.asn AS asn, 
        COALESCE(pdbn.name, btn.name, ripen.name) AS name, 
        c.country_code AS cc, 
        100*d.hege AS hegemony_score, 
        'IPv'+d.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the provided ASN and IP version filter.
    pandas_df = db.execute_query(
        query_template,
        asn=int(asn),
        ip_version=int(ip_version),
        database_="neo4j",
        result_transformer_=neo4j.Result.to_df
    )
    
    # Add AS rank data for each row in the result
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
        asn_value = pandas_df.at[index, 'asn']
        as_data = get_as_rank_data(asn_value)
        pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    sorted_df = pandas_df.sort_values(by="as_rank", ascending=True)
    return sorted_df.head(x)

# Tool 4 - Fetch upstreams ASNs
def fetch_upstreams_for_asn(asn):
    query_template = """
    MATCH (a:AS {asn: $asn})-[d:DEPENDS_ON]->(b:AS)
    WHERE a.asn <> b.asn
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    RETURN DISTINCT 
        b.asn AS asn, 
        COALESCE(pdbn.name, btn.name, ripen.name) AS name, 
        c.country_code AS cc, 
        100*d.hege AS hegemony_score, 
        'IPv'+d.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query_template, asn=int(asn),database_="neo4j", result_transformer_=neo4j.Result.to_df)
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
      # Call the blackbox function for each row
      asn = pandas_df.at[index, 'asn']
      as_data = get_as_rank_data(asn)
      pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    return pandas_df

# Tool 5 - Check if ASN is an upstream for another ASN
def is_upstream_of(upstream_asn, downstream_asn):
    query_template = """
    MATCH (upstream:AS {asn: $upstream_asn})-[:DEPENDS_ON]->(downstream:AS {asn: $downstream_asn})
    WHERE upstream.asn <> downstream.asn
    OPTIONAL MATCH (upstream)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (upstream)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (upstream)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (upstream)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (upstream)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT upstream.asn AS upstream_asn, COALESCE(pdbn.name, btn.name, ripen.name) AS name,
                    c.country_code AS cc, collect(DISTINCT t.label) AS tags,
                    COUNT(downstream) > 0 AS is_upstream
    """

    pandas_df = db.execute_query(query_template, upstream_asn=int(upstream_asn), downstream_asn=downstream_asn,
                                 database_="neo4j", result_transformer_=neo4j.Result.to_df)
    return bool(not pandas_df.empty and pandas_df.iloc[0]['is_upstream'])

# Tool 6 - Fetch downstream asns
def fetch_downstreams_for_asn(asn):
    query_template = """
    MATCH (a:AS {asn: $asn})<-[d:DEPENDS_ON]-(b:AS)
    WHERE a.asn <> b.asn
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (b)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT b.asn AS asn, COALESCE(pdbn.name, btn.name, ripen.name) AS name, c.country_code AS cc, 100*d.hege AS hegemony_score, collect(DISTINCT t.label) AS tags, 'IPv'+d.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query_template, asn=int(asn),database_="neo4j", result_transformer_=neo4j.Result.to_df)
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
      # Call the blackbox function for each row
      asn = pandas_df.at[index, 'asn']
      as_data = get_as_rank_data(asn)
      pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    return pandas_df

# Tool 7 - Fetch downstream asns
def fetch_top_hege_downstreams(asn, x=10, ip_version=4):
    query_template = """
    MATCH (a:AS {asn: $asn})<-[d:DEPENDS_ON]-(b:AS)
    WHERE a.asn <> b.asn AND d.af = $ip_version
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (b)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT b.asn AS asn, COALESCE(pdbn.name, btn.name, ripen.name) AS name, c.country_code AS cc, 100*d.hege AS hegemony_score, collect(DISTINCT t.label) AS tags, 'IPv'+d.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the new ASN
    pandas_df = db.execute_query(
        query_template,
        asn=int(asn),
        ip_version=int(ip_version),
        database_="neo4j",
        result_transformer_=neo4j.Result.to_df
    )
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
      # Call the blackbox function for each row
      asn = pandas_df.at[index, 'asn']
      as_data = get_as_rank_data(asn)
      pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    return pandas_df.head(x)

# Tool 8 - Fetch downstream asns with top as rank
def fetch_top_as_rank_downstreams(asn, x=10, ip_version=4):
    query_template = """
    MATCH (a:AS {asn: $asn})<-[d:DEPENDS_ON]-(b:AS)
    WHERE a.asn <> b.asn AND d.af = $ip_version
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (b)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT b.asn AS asn, COALESCE(pdbn.name, btn.name, ripen.name) AS name, c.country_code AS cc, 100*d.hege AS hegemony_score, collect(DISTINCT t.label) AS tags, 'IPv'+d.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the new ASN
    pandas_df = db.execute_query(
        query_template,
        asn=int(asn),
        ip_version=int(ip_version),
        database_="neo4j",
        result_transformer_=neo4j.Result.to_df
    )
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
      # Call the blackbox function for each row
      asn = pandas_df.at[index, 'asn']
      as_data = get_as_rank_data(asn)
      pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    sorted_df = pandas_df.sort_values(by="as_rank", ascending=True)
    return sorted_df.head(x)

# Tool 9 - Check if ASN is a downstream for another ASN
def is_downstream_of(downstream_asn, upstream_asn):
    query_template = """
    MATCH (downstream:AS {asn: $downstream_asn})-[:DEPENDS_ON]->(upstream:AS {asn: $upstream_asn})
    WHERE downstream.asn <> upstream.asn
    OPTIONAL MATCH (downstream)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (downstream)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (downstream)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (downstream)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (downstream)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT downstream.asn AS downstream_asn, COALESCE(pdbn.name, btn.name, ripen.name) AS name,
                    c.country_code AS cc, collect(DISTINCT t.label) AS tags,
                    COUNT(upstream) > 0 AS is_downstream
    """

    pandas_df = db.execute_query(query_template, downstream_asn=int(downstream_asn), upstream_asn=int(upstream_asn),
                                 database_="neo4j", result_transformer_=neo4j.Result.to_df)

    return not pandas_df.empty and pandas_df.iloc[0]['is_downstream']

# Tool 10 - Fetch peers asns
def fetch_peers_for_asn(asn):
    query_template = """
    MATCH (a:AS {asn: $asn})-[p:PEERS_WITH]-(b:AS)
    WHERE a.asn <> b.asn
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (b)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT b.asn AS asn, COALESCE(pdbn.name, btn.name, ripen.name) AS name, c.country_code AS cc, 100*p.hege AS hegemony_score, collect(DISTINCT t.label) AS tags, 'IPv'+p.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query_template, asn=int(asn),database_="neo4j", result_transformer_=neo4j.Result.to_df)
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
      # Call the blackbox function for each row
      asn = pandas_df.at[index, 'asn']
      as_data = get_as_rank_data(asn)
      pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    return pandas_df

# Tool 11 - Check if a given ASN is a peer of other ASN
def is_peer_of(asn1, asn2):
    """ Check if a given ASN is a peer of other ASN. """
    query_template = """
    MATCH (asn1:AS {asn: $asn1})-[p:PEERS_WITH]-(asn2:AS {asn: $asn2})
    WHERE asn1.asn <> asn2.asn
    OPTIONAL MATCH (asn1)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (asn1)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (asn1)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (asn1)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (asn1)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT asn1.asn AS asn1, COALESCE(pdbn.name, btn.name, ripen.name) AS name,
                    c.country_code AS cc, collect(DISTINCT t.label) AS tags,
                    COUNT(asn2) > 0 AS is_peer
    """

    pandas_df = db.execute_query(query_template, asn1=int(asn1), asn2=int(asn2),
                                 database_="neo4j", result_transformer_=neo4j.Result.to_df)

    return not pandas_df.empty and pandas_df.iloc[0]['is_peer']

# Tool 12 - Fetch siblings asns
def fetch_siblings_for_asn(asn):
    """ Fetch all sibling ASNs to an input ASN."""
    query_template = """
    MATCH (a:AS {asn: $asn})-[:SIBLING_OF]-(sibling:AS)
    OPTIONAL MATCH (sibling)-[:COUNTRY {reference_org:'NRO'}]->(c)
    OPTIONAL MATCH (sibling)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (sibling)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (sibling)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    RETURN DISTINCT sibling.asn AS asn, c.country_code AS cc, COALESCE(pdbn.name, btn.name, ripen.name) AS name
    """
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query_template, asn=int(asn),database_="neo4j", result_transformer_=neo4j.Result.to_df)
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
      # Call the blackbox function for each row
      asn = pandas_df.at[index, 'asn']
      as_data = get_as_rank_data(asn)
      pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    return pandas_df

# Tool 13 - Check if two ASNs are siblings
def is_sibling_of(asn1, asn2):
    """ Check if two asns are siblings."""
    query_template = """
    MATCH (asn1:AS {asn: $asn1})-[:SIBLING_OF]-(asn2:AS {asn: $asn2})
    WHERE asn1.asn <> asn2.asn
    OPTIONAL MATCH (asn1)-[:COUNTRY {reference_org:'NRO'}]->(c)
    OPTIONAL MATCH (asn1)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (asn1)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (asn1)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    RETURN DISTINCT asn1.asn AS asn1, COALESCE(pdbn.name, btn.name, ripen.name) AS name,
                    c.country_code AS cc, COUNT(asn2) > 0 AS is_sibling
    """

    pandas_df = db.execute_query(query_template, asn1=int(asn1), asn2=int(asn2),
                                 database_="neo4j", result_transformer_=neo4j.Result.to_df)

    return not pandas_df.empty and pandas_df.iloc[0]['is_sibling']

# Tool 14 - Check if two ASNs are connected in any way
def are_asns_connected(asn1, asn2):
    """Check if two given ASNs are connected to each other."""
    query_template = """
    MATCH (a:AS {asn: $asn1})-[r]-(b:AS {asn: $asn2})
    WHERE a.asn <> b.asn
    OPTIONAL MATCH (a)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (a)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (a)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (a)-[:COUNTRY {reference_org:'NRO'}]->(c)
    RETURN DISTINCT a.asn AS asn1, b.asn AS asn2, COALESCE(pdbn.name, btn.name, ripen.name) AS name,
                    c.country_code AS cc, TYPE(r) AS connection_type, COUNT(b) > 0 AS is_connected
    """

    pandas_df = db.execute_query(query_template, asn1=int(asn1), asn2=int(asn2),
                                 database_="neo4j", result_transformer_=neo4j.Result.to_df)

    return bool(not pandas_df.empty and pandas_df.iloc[0]['is_connected'])

#Tool 15 - Check for registered ROA for an ASN
def registered_roa_for_asn(asn):
    query = """
    MATCH (a:AS {asn: $asn})-[roa:ROUTE_ORIGIN_AUTHORIZATION]-(p:Prefix)
    OPTIONAL MATCH (b:AS)-[:ORIGINATE]->(p)
    RETURN p.prefix AS prefix, roa.maxLength AS maxLength, roa.notBefore AS notBefore, roa.notAfter AS notAfter, roa.uri AS uri, COLLECT(DISTINCT b.asn) AS bgp"""
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query, asn=int(asn), database_="neo4j", result_transformer_=neo4j.Result.to_df)
    return pandas_df

#Tool 16 - get the popular domains hosted by a given ASN
def popular_domains_hosted_by_asn(asn):
    query = """
    MATCH (:AS {asn: $asn})-[:ORIGINATE]->(p:Prefix)<-[:PART_OF]-(:IP)<-[:RESOLVES_TO]-(h:HostName)-[:PART_OF]->(d:DomainName)-[rr:RANK]->(rn:Ranking)
    WHERE rr.rank < 100000 and rr.reference_name = 'tranco.top1m' and h.name = d.name
    RETURN DISTINCT h.name AS hostName, rr.rank AS rank, rn.name AS rankingName, split(h.name, '.')[-1] AS tld, 1/toFloat(rr.rank) AS inv_rank, COLLECT(DISTINCT p.prefix) AS prefix
    ORDER BY rank"""
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query, asn=int(asn), database_="neo4j", result_transformer_=neo4j.Result.to_df)
    return pandas_df

#Tool 17 - get the popular hostnames hosted by a given ASN
def popular_hostnames_hosted_by_asn(asn):
    query = """
    MATCH (:AS {asn: $asn})-[:ORIGINATE]->(p:Prefix)<-[:PART_OF]-(:IP)<-[:RESOLVES_TO]-(h:HostName & !AuthoritativeNameServer)-[:PART_OF]->(d:DomainName)-[rr:RANK]->(rn:Ranking)
    WHERE rr.rank < 100000 and rr.reference_name = 'tranco.top1m'
    RETURN DISTINCT h.name AS hostName, rr.rank AS rank, rn.name AS rankingName, split(h.name, '.')[-1] AS tld, 1/toFloat(rr.rank) AS inv_rank, COLLECT(DISTINCT p.prefix) AS prefix
    ORDER BY rank"""
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query, asn=int(asn), database_="neo4j", result_transformer_=neo4j.Result.to_df)
    return pandas_df

#Tool 18 - get the authoritative name servers hosted by a given ASN
def authoritative_ns_hosted_by_asn(asn):
    query = """
    MATCH (:AS {asn: $asn})-[:ORIGINATE]->(p:Prefix)<-[:PART_OF]-(i:IP)<-[:RESOLVES_TO {reference_name:'openintel.infra_ns'}]-(h:AuthoritativeNameServer)
    RETURN DISTINCT h.name AS nameserver, COLLECT(DISTINCT p.prefix) AS prefix, i.ip as ip"""
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query, asn=int(asn), database_="neo4j", result_transformer_=neo4j.Result.to_df)
    return pandas_df

#Tool 19 - get the IXPs hosted by a given ASN
def ixps_for_asn(asn):
    query = """
    MATCH (a:AS {asn: $asn})-[:MEMBER_OF]->(i:IXP)-[:EXTERNAL_ID]->(p:PeeringdbIXID)
    OPTIONAL MATCH (i)-[:COUNTRY]->(c:Country)
    RETURN DISTINCT c.country_code as cc, i.name as name, p.id as id"""
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query, asn=int(asn), database_="neo4j", result_transformer_=neo4j.Result.to_df)
    return pandas_df

#Tool 20 - get the co-located ASNs for a given ASN
def co_located_asns_for_asn(asn):
    query = """
    MATCH (n:AS {asn: $asn})-[:LOCATED_IN]->(f:Facility)<-[:LOCATED_IN]-(p:AS)
    MATCH (n)-[:PEERS_WITH]-(p)
    RETURN p.asn as asn, collect(DISTINCT f.name) as name"""
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query, asn=int(asn), database_="neo4j", result_transformer_=neo4j.Result.to_df)
    return pandas_df

#Tool 21 - get the originated prefixes statistics for an ASN
def originated_prefixes_statistics_for_asn(asn):
    query = """
    MATCH (:AS {asn: $asn})-[o:ORIGINATE]->(p:Prefix)
    OPTIONAL MATCH (p)-[:COUNTRY {reference_org:'IHR'}]->(c:Country)
    OPTIONAL MATCH (p)-[creg:COUNTRY {reference_org:'NRO'}]->(creg_country:Country)
    OPTIONAL MATCH (p)-[:CATEGORIZED]->(t:Tag)
    OPTIONAL MATCH (p)-[:PART_OF*1..3]->(cover:Prefix)-[cover_creg:ASSIGNED {reference_org:'NRO'}]->(:OpaqueID)
    OPTIONAL MATCH (cover:Prefix)-[cover_creg:ASSIGNED {reference_org:'NRO'}]->(cover_creg_country:Country)
    RETURN c.country_code AS cc, toUpper(COALESCE(creg.registry, cover_creg.registry, '-')) AS rir, toUpper(COALESCE(creg_country.country_code, cover_creg_country.country_code, '-')) AS rir_country, p.prefix as prefix, collect(DISTINCT(t.label)) AS tags, collect(DISTINCT o.descr) as descr, collect(DISTINCT o.visibility) as visibility"""
    # Execute the query with the new ASN
    pandas_df = db.execute_query(query, asn=int(asn), database_="neo4j", result_transformer_=neo4j.Result.to_df)
    return pandas_df

def fetch_top_hege_peers(asn, x=10, ip_version=4):
    query_template = """
    MATCH (a:AS {asn: $asn})<-[d:PEERS_WITH]-(b:AS)
    WHERE a.asn <> b.asn AND d.af = $ip_version
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (b)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT b.asn AS asn, COALESCE(pdbn.name, btn.name, ripen.name) AS name, c.country_code AS cc, 100*d.hege AS hegemony_score, collect(DISTINCT t.label) AS tags, 'IPv'+d.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the new ASN
    pandas_df = db.execute_query(
        query_template,
        asn=int(asn),
        ip_version=int(ip_version),
        database_="neo4j",
        result_transformer_=neo4j.Result.to_df
    )
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
      # Call the blackbox function for each row
      asn = pandas_df.at[index, 'asn']
      as_data = get_as_rank_data(asn)
      pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    return pandas_df.head(x)

def fetch_top_as_rank_peers(asn, x=10, ip_version=4):
    query_template = """
    MATCH (a:AS {asn: $asn})<-[d:PEERS_WITH]-(b:AS)
    WHERE a.asn <> b.asn AND d.af = $ip_version
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (b)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT b.asn AS asn, COALESCE(pdbn.name, btn.name, ripen.name) AS name, c.country_code AS cc, 100*d.hege AS hegemony_score, collect(DISTINCT t.label) AS tags, 'IPv'+d.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the new ASN
    pandas_df = db.execute_query(
        query_template,
        asn=int(asn),
        ip_version=int(ip_version),
        database_="neo4j",
        result_transformer_=neo4j.Result.to_df
    )
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
      # Call the blackbox function for each row
      asn = pandas_df.at[index, 'asn']
      as_data = get_as_rank_data(asn)
      pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    sorted_df = pandas_df.sort_values(by="as_rank", ascending=True)
    return sorted_df.head(x)

def fetch_top_hege_siblings(asn, x=10, ip_version=4):
    query_template = """
    MATCH (a:AS {asn: $asn})<-[d:SIBLING_OF]-(b:AS)
    WHERE a.asn <> b.asn AND d.af = $ip_version
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (b)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT b.asn AS asn, COALESCE(pdbn.name, btn.name, ripen.name) AS name, c.country_code AS cc, 100*d.hege AS hegemony_score, collect(DISTINCT t.label) AS tags, 'IPv'+d.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the new ASN
    pandas_df = db.execute_query(
        query_template,
        asn=int(asn),
        ip_version=int(ip_version),
        database_="neo4j",
        result_transformer_=neo4j.Result.to_df
    )
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
      # Call the blackbox function for each row
      asn = pandas_df.at[index, 'asn']
      as_data = get_as_rank_data(asn)
      pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    return pandas_df.head(x)

def fetch_top_as_rank_siblings(asn, x=10, ip_version=4):
    query_template = """
    MATCH (a:AS {asn: $asn})<-[d:SIBLING_OF]-(b:AS)
    WHERE a.asn <> b.asn AND d.af = $ip_version
    OPTIONAL MATCH (b)-[:NAME {reference_org:'PeeringDB'}]->(pdbn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'BGP.Tools'}]->(btn:Name)
    OPTIONAL MATCH (b)-[:NAME {reference_org:'RIPE NCC'}]->(ripen:Name)
    OPTIONAL MATCH (b)-[:COUNTRY {reference_name: 'nro.delegated_stats'}]->(c:Country)
    OPTIONAL MATCH (b)-[:CATEGORIZED]->(t:Tag)
    RETURN DISTINCT b.asn AS asn, COALESCE(pdbn.name, btn.name, ripen.name) AS name, c.country_code AS cc, 100*d.hege AS hegemony_score, collect(DISTINCT t.label) AS tags, 'IPv'+d.af AS af
    ORDER BY hegemony_score DESC
    """
    # Execute the query with the new ASN
    pandas_df = db.execute_query(
        query_template,
        asn=int(asn),
        ip_version=int(ip_version),
        database_="neo4j",
        result_transformer_=neo4j.Result.to_df
    )
    pandas_df['as_rank'] = None
    for index, row in pandas_df.iterrows():
      # Call the blackbox function for each row
      asn = pandas_df.at[index, 'asn']
      as_data = get_as_rank_data(asn)
      pandas_df.at[index, 'as_rank'] = as_data['data']['asn']['rank']
    sorted_df = pandas_df.sort_values(by="as_rank", ascending=True)
    return sorted_df.head(x)

# def generate_simple_valley_free_routes(db, asn_x, num_routes, length_z):
#     query_template = """
#     MATCH (start:AS {asn: $asn_x})-[:PEERS_WITH|DEPENDS_ON]->(neighbor:AS)
#     OPTIONAL MATCH (neighbor)-[:RANK]->(rank)
#     WITH start, neighbor, COALESCE(rank.value, 0) AS rank_value
#     ORDER BY rank_value DESC
#     LIMIT $num_routes

#     MATCH path=(start)-[r*%d]-(end:AS)
#     WHERE start.asn = $asn_x AND end.asn IS NOT NULL
#     AND ALL(i IN RANGE(0, SIZE(r)-2) WHERE NOT (TYPE(r[i]) = 'DEPENDS_ON' AND TYPE(r[i+1]) = 'UPSTREAM_OF'))
#     RETURN DISTINCT [n IN nodes(path) | n.asn] AS route
#     LIMIT $num_routes
#     """ % length_z

#     pandas_df = db.execute_query(query_template, asn_x=asn_x, num_routes=num_routes,
#                                  database_="neo4j", result_transformer_=neo4j.Result.to_df)

#     pandas_df = pandas_df.dropna(subset=['route'])  # Drop rows where route contains None
#     pandas_df['route'] = pandas_df['route'].apply(
#         lambda x: [asn for asn in x if asn is not None])  # Remove None values from routes

#     return pandas_df
