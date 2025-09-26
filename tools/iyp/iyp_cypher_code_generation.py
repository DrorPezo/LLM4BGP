from neo4j import GraphDatabase, RoutingControl
import neo4j
import pyvis
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
from langchain.prompts import PromptTemplate
from langchain.chains import ConversationChain
from langchain.chains import LLMChain
from langchain.memory import ConversationBufferMemory
from langchain.agents.agent_types import AgentType
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
import pandas as pd
import re
import os

# Using IYP local instance
# URI = "neo4j://localhost:7687"
# Using IYP public instance
URI = "neo4j://iyp-bolt.ihr.live:7687"
AUTH = ('neo4j', 'password')
db = GraphDatabase.driver(URI, auth=AUTH)

# Task description prompt
task_description = '''
    You are an expert in Cypher, the query language for Neo4j, and specialize in creating queries for
    custom knowledge graphs based on user-provided entities, relationships, and examples.
    Your task is to generate accurate and efficient Cypher queries from human-language descriptions of the desired query.
    Instructions:
    1.	Familiarize yourself with the provided schema details of the knowledge graph, including entities and relationships.
    2.	Analyze the provided examples of Cypher queries to understand the structure and style.
    3.	Use the schema and examples as a reference to generate Cypher code for the human-language query that follows.
    Notes for the Model:
    1. Ensure that the output code adheres to the Cypher syntax.
    2. Include relevant filters, conditions, and return statements.
    3. Use aliases for clarity and readability.
    4. Generate THE CYPHER CODE ONLY without any additions or repeating on the task, as shown in the exmples.
  '''
# IYP entitites
iyp_entities = '''
    1.	AS: Autonomous System, uniquely identified with the 𝑎𝑠𝑛 property.
    2.	AtlasMeasurement: RIPE Atlas Measurement, uniquely identified with the 𝑖𝑑 property.
    3.	AtlasProbe: RIPE Atlas probe, uniquely identified with the 𝑖𝑑 property.
    4.	AuthoritativeNameServer: Authoritative DNS nameserver for a set of domain names, uniquely identified with the 𝑛𝑎𝑚𝑒 property.
    5.	BGPCollector: A RIPE RIS or RouteViews BGP collector, uniquely identified with the 𝑛𝑎𝑚𝑒 property.
    6.	CaidaIXID: Unique identifier for IXPs from CAIDA’s IXP dataset.
    7.	Country: Represent an economy, uniquely identified by either its two or three character code (properties 𝑐𝑜𝑢𝑛𝑡𝑟𝑦_𝑐𝑜𝑑𝑒 and 𝑎𝑙𝑝ℎ𝑎3).
    8.	DomainName: Any DNS domain name that is not a FQDN (see HostName), uniquely identified by the 𝑛𝑎𝑚𝑒 property.
    9.	Estimate: Represent a report that approximates a quantity, for example the World Bank population estimate.
    10.	Facility: Co-location facility for IXPs and ASes, uniquely identified by the 𝑛𝑎𝑚𝑒 property.
    11.	HostName: A fully qualified domain name uniquely identified by the 𝑛𝑎𝑚𝑒 property.
    12.	IP: An IPv4 or IPv6 address uniquely identified by the 𝑖𝑝 property. The 𝑎𝑓 property (address family) provides the IP version of the prefix.
    13.	IXP: An Internet Exchange Point, loosely identified by the 𝑛𝑎𝑚𝑒 property or using related IDs (see the EXTERNAL_ID relationship).
    14.	Name: Represent a name that could be associated with a network resource (e.g., an AS), uniquely identified by the 𝑛𝑎𝑚𝑒 property.
    15.	OpaqueID: Represent the opaque-id value found in RIR’s delegated files. Resources related to the same opaque-id are registered to the same resource holder. Uniquely identified by the 𝑖𝑑 property.
    16.	Organization: Represent an organization and is loosely identified by the 𝑛𝑎𝑚𝑒 property or using related IDs (see the EXTERNAL_ID relationship).
    17.	PeeringdbFacID: Unique identifier for a Facility as assigned by PeeringDB.
    18.	PeeringdbIXID: Unique identifier for an IXP as assigned by PeeringDB.
    19.	PeeringdbNetID: Unique identifier for an AS as assigned by PeeringDB.
    20.	PeeringdbOrgID: Unique identifier for an Organization as assigned by PeeringDB.
    21.	Prefix: An IPv4 or IPv6 prefix uniquely identified by the 𝑝𝑟𝑒𝑓𝑖𝑥 property. The 𝑎𝑓 property (address family) provides the IP version of the prefix.
    22.	Ranking: Represent a specific ranking of Internet resources (e.g., CAIDA’s ASRank or Tranco ranking). The rank value for each resource is given by the RANK relationship.
    23.	Tag: The output of a classification. A tag can be the result of a manual or automated classification. Uniquely identified by the 𝑙𝑎𝑏𝑒𝑙 property.
    24.	URL: The full URL for an Internet resource, uniquely identified by the 𝑢𝑟𝑙 property.
'''
# IYP relationships
iyp_relationships = '''
    1.	ALIAS_OF: Equivalent to the CNAME record in DNS. It relates two HostNames.
    2.	ASSIGNED: Represents the allocation by a RIR of a network resource (AS, Prefix) to a resource holder (see OpaqueID) or represents the assigned IP address of an AtlasProbe.
    3.	AVAILABLE: Relates ASes and Prefixes to RIRs (in the form of an OpaqueID), meaning that the resource is not allocated and available at the related RIR.
    4.	CATEGORIZED: Relates a network resource (AS, Prefix, URL) to a Tag, meaning that the resource has been classified according to the Tag. The 𝑟𝑒𝑓𝑒𝑟𝑒𝑛𝑐𝑒_𝑛𝑎𝑚𝑒 property provides the name of the original dataset/classifier.
    5.	COUNTRY: Relates any node to its corresponding country. This relation may have different meanings depending on the original dataset (e.g., geo-location or registration).
    6.	DEPENDS_ON: Relates an AS or Prefix to an AS, meaning the reachability of the AS/Prefix depends on a certain AS.
    7.	EXTERNAL_ID: Relates a node to an identifier commonly used by an organization. For example, PeeringDB assigns unique identifiers to IXPs (see PeeringdbIXID).
    8.	LOCATED_IN: Indicates the location of a resource at a specific geographical or topological location. For example, co-location Facility for an IXP or AS for an AtlasProbe.
    9.	MANAGED_BY: Entity in charge of a network resource. For example, an AS is managed by an Organization, a DomainName is managed by an AuthoritativeNameServer.
    10.	MEMBER_OF: Represents membership to an organization. For example, an AS is a member of an IXP.
    11.	NAME: Relates an entity to its usual or registered name. For example, the name of an AS.
    12.	ORIGINATE: Relates a Prefix to an AS, meaning that the prefix is seen as being originated from that AS in BGP.
    13.	PARENT: Relates two DomainNames and represents a zone cut between the parent zone and the more specific zone.
    14.	PART_OF: Represents that one entity is a part of another. For example, an IP address is part of an IP Prefix, or a HostName is part of a DomainName.
    15.	PEERS_WITH: Represents the connection between two ASes as seen in BGP. It also includes peerings between ASes and BGPCollectors.
    16.	POPULATION: Indicates that an AS hosts a certain fraction of the population of a country or represents the estimated population of a country.
    17.	QUERIED_FROM: Relates a DomainName to an AS or Country, meaning that the AS or Country appears in the Top 100 AS or Country querying the DomainName (as reported by Cloudflare radar).
    18.	RANK: Relates a resource to a Ranking, meaning that the resource appears in the Ranking. The 𝑟𝑎𝑛𝑘 property gives the exact rank position.
    19.	RESERVED: Indicates that an AS or Prefix is reserved for a certain purpose by RIRs or IANA.
    20.	RESOLVES_TO: Relates a HostName to an IP address, meaning that a DNS resolution resolved the corresponding IP.
    21.	ROUTE_ORIGIN_AUTHORIZATION: Relates an AS and a Prefix, meaning that the AS is authorized to originate the Prefix by RPKI.
    22.	SIBLING_OF: Relates ASes or Organizations together, meaning that they represent the same entity.
    23.	TARGET: Relates an AtlasMeasurement to an IP, HostName, or AS, meaning that an Atlas measurement is set up to probe that resource.
    24.	WEBSITE: Relates a URL to an Organization, Facility, IXP, or AS, representing a common website for the resource.
'''
# Examples of tasks + Cypher codes for few-shots learning
examples = [
    {
        "input": "Find the IXPs and corresponding country codes where IIJ (AS2497):",
        "output": '''MATCH (iij:AS {asn:2497})-[:MEMBER_OF]-(ix:IXP)--(cc:Country)
        RETURN iij, ix, cc'''
    },
    {
        "input": "Find 'Name' nodes directly connected to the node corresponding to AS2497:",
        "output": '''MATCH (a:AS {asn:2497})--(n:Name) RETURN a,n'''
    },
    {
        "input": "Find nodes of any type that are connected to the node corresponding to prefix 8.8.8.0/24:",
        "output": '''MATCH (gdns:Prefix {prefix:'8.8.8.0/24'})--(neighbor)
        RETURN gdns, neighbor'''
    },
    {
        "input": "Search for a country node directly connected to AS2497's node and that comes from NRO's delegated stats:",
        "output": '''MATCH (iij:AS {asn:2497})-[{reference_name:'nro.delegated_stats'}]-(cc:Country)
        RETURN iij, cc'''
    },
    {
        "input": "Select domain names in top 50k rankings that resolves to an IP originated by AS2497:",
        "output": '''MATCH (:Ranking)-[r:RANK]-(dn:DomainName)-[:PART_OF]-(hn:HostName)-[:RESOLVES_TO]-(ip:IP)--(pfx:Prefix)-[:ORIGINATE]-(iij:AS {asn:2497})
        WHERE r.rank < 50000 AND dn.name = hn.name
        RETURN hn, ip, pfx, iij'''
    },
    {
        "input": "From the top 10k domain names select domain names that ends with '.jp', the corresponding IP, prefix, and ASN:",
        "output": '''MATCH (:Ranking)-[r:RANK]-(dn:DomainName)-[:PART_OF]-(hn:HostName)-[rt:RESOLVES_TO]-(ip:IP)-[po:PART_OF]-(pfx:Prefix)-[o:ORIGINATE]-(net:AS)
        WHERE dn.name ENDS WITH '.jp' AND r.rank<10000 AND dn.name = hn.name
        RETURN hn, ip, pfx, net, rt, po, o'''
    },
    {
        "input": "Select IHR's top 20 ASes in Iran and show how they are connected to each other using AS relationships:",
        "output": '''MATCH (a:AS)-[ra:RANK]->(:Ranking {name: 'IHR country ranking: Total AS (IR)'})<-[rb:RANK]-(b:AS)-[p:PEERS_WITH]-(a)
        WHERE ra.rank < 20 AND rb.rank < 20 AND p.rel = 0
        RETURN a, p, b'''
    },
    {
        "input": "Select AS dependencies for AS2501 and find the shortest PEERS_WITH relationship to these ASes:",
        "output": '''MATCH (a:AS {asn:2501})-[h:DEPENDS_ON {af:4}]->(d:AS)
        WITH a, COLLECT(DISTINCT d) AS dependencies
        UNWIND dependencies as d
        MATCH p = allShortestPaths((a)-[:PEERS_WITH*]-(d))
        WHERE a.asn <> d.asn AND all(r IN relationships(p) WHERE r.af = 4) AND all(n IN nodes(p) WHERE n IN dependencies)
        RETURN p'''
    },
    {
        "input": "List of IPs for RIPE RIS full feed peers (more than 800k prefixes):",
        "output": '''MATCH (n:BGPCollector)-[p:PEERS_WITH]-(a:AS)
        WHERE n.project = 'riperis' AND p.num_v4_pfxs > 800000
        RETURN n.name, COUNT(DISTINCT p.ip) AS nb_full, COLLECT(DISTINCT p.ip) AS ips_full'''
    },
    {
        "input": "Active RIPE Atlas probes for the top 5 ISPs in Japan:",
        "output": '''MATCH (pb:AtlasProbe)-[:LOCATED_IN]-(a:AS)-[pop:POPULATION]-(c:Country)
        WHERE c.country_code = 'JP' AND pb.status_name = 'Connected' AND pop.rank <= 5
        RETURN pop.rank, a.asn, COLLECT(pb.id) AS probe_ids ORDER BY pop.rank'''
    },
    {
        "input": "Cypher query to find all originating ASes in IYP:",
        "output": '''// Select ASes originating prefixes
        MATCH (x:AS)-[:ORIGINATE]-(:Prefix)
        // Return the AS's ASN
        RETURN DISTINCT x.asn'''
    },
    {
        "input": "Cypher query to find all Multiple Origin AS (MOAS) prefixes in IYP:",
        "output": '''// Find Prefixes with two originating ASes
        MATCH (x:AS)-[:ORIGINATE]-(p:Prefix)-[:ORIGINATE]-(y:AS)
        // Make sure that the ASNs of the two ASes are different
        WHERE x.asn <> y.asn
        // Return the prefix attribute of the Prefix node
        RETURN DISTINCT p.prefix'''
    },
    {
        "input": "Cypher query to find the popular hostnames corresponding to prefixes originated by ASes managed by CERN and that are RPKI valid:",
        "output": '''// Find RPKI valid prefixes managed by CERN
        MATCH (org:Organization)-[:MANAGED_BY]-(:AS)-[:ORIGINATE]-(pfx:Prefix)-[:CATEGORIZED]-(:Tag {label:'RPKI Valid'})
        WHERE org.name = 'CERN'
        // Find popular hostnames in these prefixes (referred as pfx)
        MATCH (pfx)-[:PART_OF]-(:IP)-[:RESOLVES_TO {reference_name:'openintel.tranco1m'}]-(h:HostName)
        // Return the hostname's name
        RETURN DISTINCT h.name'''
    },
    {
        "input": "Find RPKI invalid prefixes for domain names in Tranco list:",
        "output": '''// Resolve IP addresses from the Tranco Top 1 million list and count the number of RPKI invalid prefixes
        MATCH (:Ranking {name:'Tranco top 1M'})-[:RANK]-(d:DomainName)-[:PART_OF]-(h:HostName)-[:RESOLVES_TO]-(:IP)-[:PART_OF]-
        (pfx:Prefix)-[:CATEGORIZED]-(t:Tag)
        WHERE d.name = h.name AND t.label STARTS WITH 'RPKI Invalid'
        RETURN COUNT(DISTINCT pfx)'''
    },
    {
        "input": "Query used to reproduce results from DNS Robustness on shared infrastructure and using /24 grouping (Table 4):",
        "output": '''// List /24 prefixes of nameservers for .com/.net/.org domain names in Tranco
        MATCH (r:Ranking {name:'Tranco top 1M'})-[:RANK]-(d:DomainName)-[:MANAGED_BY]-(a:AuthoritativeNameServer)
        -[:RESOLVES_TO]-(i:IP {af:4})
        WHERE d.name ENDS WITH '.com' OR d.name ENDS WITH '.net' OR d.name ENDS WITH '.org'
        RETURN d, COLLECT(DISTINCT REDUCE(pfx = "", n IN SPLIT(i.ip, '.')[0..3] | pfx + n + ".")) AS pfx'''
    },
    {
        "input": "Query extending results from DNS Robustness by looking at all domain names in Tranco and the BGP prefix of the corresponding nameservers:",
        "output": '''// List prefixes of nameservers for all domain names in Tranco
        MATCH (r:Ranking {name:'Tranco top 1M'})-[:RANK]-(d:DomainName)-[:MANAGED_BY]-(a:AuthoritativeNameServer)
        -[:RESOLVES_TO]-(i:IP {af:4})-[:PART_OF]-(pfx:Prefix)
        RETURN d, COLLECT(DISTINCT pfx)'''
    },
    {
        "input": "Number of ASes registered in Japan:",
        "output": '''MATCH (a:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:'JP'})
        RETURN COUNT(DISTINCT a)'''
    },
    {
        "input": "Main ASes in Japan:",
        "output": '''MATCH (a)-[:COUNTRY {reference_org:'RIPE NCC'}]-(:Country {country_code:'JP'})
        MATCH (a:AS)-[ra:RANK {reference_name:"ihr.country_dependency"}]->(r:Ranking)--(:Country {country_code:'JP'})
        WHERE ra.rank < 10
        OPTIONAL MATCH (a)-[:NAME {reference_org:"BGP.Tools"}]-(n:Name)
        RETURN DISTINCT a.asn as ASN, n.name AS AS_Name, COLLECT(r.name) as Rankings
        ORDER BY a.asn'''
    },
    {
        "input": "Dependencies for main ASes in Japan:",
        "output": '''// Select top ASes
        MATCH (a)-[:COUNTRY {reference_org:'RIPE NCC'}]-(:Country {country_code:'JP'})
        MATCH (a:AS)-[ra:RANK {reference_name:"ihr.country_dependency"}]->(:Ranking)
        WHERE ra.rank < 10
        // Find their direct dependencies
        OPTIONAL MATCH (a)-[p:PEERS_WITH]-(b), (a)-[d:DEPENDS_ON]->(b)
        WHERE p.rel = 1 AND d.hege > 0.03 AND a <> b
        RETURN a, d, b'''
    },
    {
        "input": "IXP membership for main ASes in Japan:",
        "output": '''// Select top ASNs
        MATCH (a)-[:COUNTRY {reference_org:'RIPE NCC'}]-(:Country {country_code:'JP'})
        MATCH (a:AS)-[ra:RANK {reference_name:"ihr.country_dependency"}]->(:Ranking)
        WHERE ra.rank <= 10
        // Find IXP membership
        OPTIONAL MATCH (a)-[m:MEMBER_OF]-(ix:IXP)-[:COUNTRY]-(:Country {country_code:'JP'})
        RETURN a, m, ix'''
    },
    {
        "input": "Most popular Japanese domain names:",
        "output": '''MATCH (:Ranking {name: 'Tranco top 1M'})-[ra:RANK]-(dn:DomainName)-[q:QUERIED_FROM]-(c:Country)
        WHERE q.value > 30 AND c.country_code = 'JP'
        RETURN dn.name as domain_name, ra.rank as rank, q.value as per_query_JP
        ORDER BY rank'''
    },
    {
        "input": "IP addresses, prefixes, and ASNs related to yahoo.co.jp:",
        "output": '''MATCH p = (dn:DomainName)-[:PART_OF]-(hn:HostName)-[:RESOLVES_TO]-(:IP)-[:PART_OF]-(:Prefix)-[:ORIGINATE {reference_org:'BGPKIT'}]-(a:AS)
        WHERE dn.name = 'yahoo.co.jp' AND dn.name = hn.name
        RETURN p'''
    },
    {
        "input": "Hosts of most popular Japanese domain names:",
        "output": '''MATCH (:Ranking {name: 'Tranco top 1M'})-[ra:RANK]-(dn:DomainName)-[q:QUERIED_FROM]-(c:Country)
        WHERE q.value > 30 AND c.country_code = 'JP' AND ra.rank < 10000
        MATCH (dn:DomainName)-[:PART_OF]-(hn:HostName)-[r:RESOLVES_TO]-(ip:IP)-[p:PART_OF]-(pfx:Prefix)-[o:ORIGINATE]-(net:AS)
        WHERE dn.name = hn.name
        RETURN hn, ip, pfx, net, r, p, o'''
    },
    {
        "input": "Number of ASes hosting authoritative name servers per domain:",
        "output": '''MATCH (:Ranking {name: 'Tranco top 1M'})-[ra:RANK]-(dn:DomainName)-[q:QUERIED_FROM]-(c:Country)
        WHERE q.value > 30 AND c.country_code = 'JP' AND ra.rank < 10000
        MATCH (dn:DomainName)-[:MANAGED_BY]-(:AuthoritativeNameServer)-[:RESOLVES_TO]-(:IP)-[:PART_OF]-(:Prefix)-[:ORIGINATE {reference_org:'BGPKIT'}]-(a:AS)
        RETURN dn.name, count(DISTINCT a) AS nb_asn, COLLECT(distinct a.asn) ORDER BY nb_asn DESC'''
    },
    {
        "input": "Authoritative DNS name servers and their originating AS for dmm.com:",
        "output": '''MATCH p = (dn:DomainName)-[:MANAGED_BY]-(:AuthoritativeNameServer)-[:RESOLVES_TO]-(:IP)-[:PART_OF]-(:Prefix)-[:ORIGINATE {reference_org:'BGPKIT'}]-(a:AS)
        WHERE dn.name = 'dmm.com'
        RETURN p'''
    }
]

# Generate Cypher code from user request
def generate_cypher_code(task, llm):
    prompt = PromptTemplate(
      input_variables=["task_description", "iyp_entities", "iyp_relationships", "examples", "task"],
      template="""
      Task Description:{task_description}
      Entities:{iyp_entities}
      Relationships:{iyp_relationships}
      Look at the following examples: {examples}
      Requested Task:{task}
      """
    )

    chain = prompt | llm
    answer = chain.invoke({"task_description": task_description,
                        "iyp_entities": iyp_entities,
                        "iyp_relationships": iyp_relationships,
                        "examples": examples,
                        "task":task}).content
    return answer

# Aux function to fetch the Cypher code from LLM answer
def extract_cypher_query(text):
    # Define a regular expression to match the Cypher query within code block markers
    pattern = r"```([\s\S]*?)```"
    match = re.search(pattern, text)

    if match:
        # Return the content inside the triple backticks
        return match.group(1).strip()
    else:
        return "No Cypher query found."

# Query IYP with user task
# This method gets table from IYP and summarizing it using an agent
def query_iyp(task, llm):
    cypher_code_result = generate_cypher_code(task, llm)
    query = extract_cypher_query(cypher_code_result)
    # print(query)
    pandas_df = db.execute_query(query, database_="neo4j", result_transformer_=neo4j.Result.to_df)
    agent = create_pandas_dataframe_agent(llm,
                                          pandas_df,
                                          prefix=task,
                                          verbose=True,
                                          number_of_head_rows=len(pandas_df),
                                          agent_type=AgentType.OPENAI_FUNCTIONS,
                                          max_iterations=1,
                                          allow_dangerous_code=True)
    final_answer = agent.invoke('''You are an helpfull assistant that summarizes tables and data, 
          and draws clear conclusions from them. 
          Summarize the given table data. Decide what is the most convenient way to summarize the data (list, sentences, etc)
          In your summary, refer to all the information presented in the table, and draw a final conclusion given the prefix (the asked question). 
          DO NOT MISS ANY ROW OR COLUMN.
          When you have final conclusion - give the final answer and stop calling the agent.''')
    return final_answer['output']

# Analyze neo4j records
def parse_neo4j_records(records):
    data = []
    for record in records:
        record_data = []
        for item in record.items():
          _, neo4j_element = item
          record_data.append(neo4j_element._properties)
        data.append(record_data)
    return data

