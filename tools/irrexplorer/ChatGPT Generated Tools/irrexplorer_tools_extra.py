
"""LangChain tools wrapping extra IRRExplorer analytics."""
from langchain_core.tools import tool
from typing import Dict, List
from tools.irrexplorer.irrexplorer_aux_extra import (
    route_freshness_days,irr_rpki_mismatch_ratio,prefix_length_hist,
    registry_distribution,asn_hygiene_score,as_set_overlap
)

@tool
def irr_route_freshness(asn:str)->list:
    """Return [(prefix, age_days)] for each originated prefix."""
    return route_freshness_days(asn)

@tool
def irr_rpki_mismatch(asn:str)->float:
    """Fraction of originated prefixes with NON‑VALID RPKI status."""
    return irr_rpki_mismatch_ratio(asn)

@tool
def irr_prefix_length_hist(asn:str)->dict:
    """Histogram of prefix lengths for an ASN."""
    return prefix_length_hist(asn)

@tool
def irr_registry_dist(asn:str)->dict:
    """Count how many route objects each IRR registry hosts for ASN."""
    return registry_distribution(asn)

@tool
def irr_hygiene_score(asn:str)->float:
    """Heuristic 0‑1 hygiene score (lower mismatch & fresh objects)."""
    return asn_hygiene_score(asn)

@tool
def irr_as_set_overlap(params:dict)->dict:
    """Compare two AS‑SETs, return unique and common members.

    Keys: as_set1, as_set2
    """
    return as_set_overlap(**params)

irrexplorer_tools_extra=[
    irr_route_freshness,irr_rpki_mismatch,irr_prefix_length_hist,
    irr_registry_dist,irr_hygiene_score,irr_as_set_overlap
]
