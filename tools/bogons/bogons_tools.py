from langchain_core.tools import tool
from tools.bogons.bogons_aux import *

# Tool 1 - Check if a prefix is a bogon 
# @tool(return_direct=True)
@tool
def is_prefix_a_bogon(prefix: str) -> bool:
    """
    Given a prefix, check wheather is a bogon or not.
    Input: prefix or IP address (string)
    Output: True / False (boolean)
    """
    if is_bogon(prefix):
        return 'Bogon'
    else:
        return 'Non-Bogon'

# Bogons tools list
bogons_tools = [is_prefix_a_bogon]
