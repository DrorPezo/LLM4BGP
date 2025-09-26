from langchain_core.tools import tool
from tools.whois.whois_aux import *

@tool
def as_imports_with_other_asn(asn1, asn2):
  '''
  Returns the import rules of asn1 with asn2.
  Input: asn1 (int), asn2 (int)
  Output: imports (list of dicts)
  '''
  return as_imports_with_other_as(asn1, asn2)

@tool
def as_exports_with_other_asn(asn1, asn2):
  '''
  Returns the export rules of asn1 with asn2.
  Input: asn1 (int), asn2 (int)
  Output: exports (list of dicts)
  '''
  return as_exports_with_other_as(asn1, asn2)

@tool
def whois_as(ASN: int) -> str:
    '''
    Given ASN, return its IRR data.
    Input: ASN (int)
    Output: IRR data (str)
    '''
    return get_full_as_irr(ASN)

@tool
def get_as_remarks(asn: int) -> str:
    '''
    Given ASN, return its remarks.
    Input: ASN (int)
    Output: Remarks (list of str)
    '''
    data = get_structured_policies(asn)
    remarks = data['remarks']
    return remarks
  
whois_tools = [as_imports_with_other_asn, as_exports_with_other_asn, whois_as, get_as_remarks]
  