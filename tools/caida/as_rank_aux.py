import re
import requests

URL = "https://api.asrank.caida.org/v2/graphql"

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
    result = requests.post(URL,json={'query':asn_data})
    if result.status_code == 200:
        return result.json()
    else:
        print ("Query failed to run returned code of %d " % (result.status_code))
        return -1
    
def extract_numbers(text: str):
    pattern = r"[-+]?\d*\.\d+|[-+]?\d+"
    matches = re.findall(pattern, text)
    if matches:
        return int(matches[0])
    else:
        return -1
    