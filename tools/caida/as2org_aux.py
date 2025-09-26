import requests
import json
import pycountry

URL = 'https://api.data.caida.org/as2org/v1'
ASRANK_URL = 'https://api.asrank.caida.org/v2/graphql'
ASRANK_REST = "https://api.asrank.caida.org/v2/restful"

# Convert country name to iso code
def get_country_iso_code(country_name):
    try:
        country = pycountry.countries.lookup(country_name)
        return country.alpha_2  # Returns ISO 3166-1 alpha-2 code (e.g., 'US' for United States)
    except LookupError:
        return "Invalid country name"

def get_as_rank_data(asn): 
    asn_data = """{
        asn(asn:"%i") {
            asn
            asnName
            rank
            organization {
                orgId
                orgName
            }
            cliqueMember
            seen
            longitude
            latitude
            cone {
                numberAsns
                numberPrefixes
                numberAddresses
            }
            country {
                iso
                name
            }
            asnDegree {
                provider
                peer
                customer
                total
                transit
                sibling
            }
            announcing {
                numberPrefixes
                numberAddresses
            }
        }
    }""" % (asn)
    result = requests.post(ASRANK_URL, json={'query':asn_data})
    if result.status_code == 200:
        return result.json()
    else:
        print ("Query failed to run returned code of %d " % (result.status_code))
        return -1

# Helper method
def getJsonResponse(URL):
    response = requests.get(URL)
    return response.json()

def get_as_rank(asn):
    asn_data = get_as_rank_data(int(asn))
    return asn_data['data']['asn']['rank']
  
def findLargestASN(orgName):
    url_built = f"{URL}/search/?name={orgName}"
    orgs = getJsonResponse(url_built)
    if orgs['data']:
       if orgs["data"][-1]:
          if orgs["data"][-1]['members']:
            ases = orgs["data"][-1]['members']
            ases.sort(key=get_as_rank)
            return ases[0]
    return -1 

def fetch_org(targetASN):
    """
    Download the organization for a target AS
    """
    # Get orgid given asn
    url_built = f"{URL}/asns/{targetASN}/"
    organization = None
    orgId = ''

    try:
      response = getJsonResponse(url_built)
      if response:
          if response["data"]:
            if response["data"][0]:
                if response["data"][0]["orgId"]:
                    orgId = response["data"][0]["orgId"]

    except Exception as err:
      print(url_built)
      print(err)
    if orgId:
        # Get org given orgid
        url_built = f"{URL}/orgs/{orgId}"
        try: 
          response = getJsonResponse(url_built)
          if response:
            if response["data"]:
              if response["data"][0]:
                  organization = response["data"][0]
        except Exception as err: 
          print(url_built)
          print(err)
        if organization:
            return organization["orgName"]
    return orgId

def asn2org_data(orgName):
    """
    Find number of ASNs in an organization
    """
    # Find org id associated with orgName
    URL = 'https://api.data.caida.org/as2org/v1'
    url_built = f"{URL}/search/?name={orgName}"
    orgs = getJsonResponse(url_built)
    return orgs

def current_as2org(orgName):
    orgs = asn2org_data(orgName)
    if orgs:  
      if orgs['data']:
        if orgs['data'][-1]:
          if orgs['data'][-1]['members']:
            return orgs['data'][-1]['members']
    return []

def largest_org_in_a_country(country):
    offset = 0 
    PAGE = 1000
    first = PAGE
    orgs = None 
    while True:
        url_built = f"{ASRANK_REST}/organizations/?first={first}&offset={offset}"
        orgs = getJsonResponse(url_built)
        for AS in orgs['data']['organizations']['edges']:
          if AS['node']['country']['iso'] == country:
            return AS
        if orgs['data']['organizations']['pageInfo']['hasNextPage']:
            offset += PAGE
        else:
          break

def largest_as_in_a_country(country):
    offset = 0 
    PAGE = 1000
    first = PAGE
    asns = None
    while True:
        url_built = f"{ASRANK_REST}/asns/?first={first}&offset={offset}"
        asns = getJsonResponse(url_built)
        for AS in asns['data']['asns']['edges']:
            if AS['node']['country']['iso'] == country:
                return AS
        if asns['data']['organizations']['asns']['hasNextPage']:
            offset += PAGE
        else:
          break

def num_of_orgs_in_a_country(country):
    offset = 0 
    PAGE = 1000
    first = PAGE
    orgs_num = 0
    orgs_list = []
    while True:
        url_built = f"{ASRANK_REST}/organizations/?first={first}&offset={offset}"
        orgs = getJsonResponse(url_built)
        for AS in orgs['data']['organizations']['edges']:
          if AS['node']['country']['iso'] == country:
            orgs_num += 1
            org_name = AS['node']['orgName']
            orgs_list.append(org_name)
        if orgs['data']['organizations']['pageInfo']['hasNextPage']:
            offset += PAGE
        else:
          break
    return orgs_num, orgs_list

def num_of_as_in_a_country(country):
    offset = 0 
    PAGE = 1000
    first = PAGE
    as_num = 0
    as_list = []
    while True:
        url_built = f"{ASRANK_REST}/asns/?first={first}&offset={offset}"
        asns = getJsonResponse(url_built)
        for AS in asns['data']['asns']['edges']:
            if AS['node']['country']['iso'] == country:
                as_num += 1
                asn = AS['node']['asn']
                as_list.append(asn)
        if asns['data']['asns']['pageInfo']['hasNextPage']:
            offset += PAGE
        else:
          break
    return as_num, as_list
