# import hashlib
# import os
# from langchain_openai import OpenAIEmbeddings
# from langchain_text_splitters import RecursiveCharacterTextSplitter
# from langchain_core.vectorstores import InMemoryVectorStore
# from langchain_community.document_loaders import PyPDFDirectoryLoader
# from langchain.tools.retriever import create_retriever_tool
from typing import Literal, List, Dict 
from langgraph.graph import MessagesState
import neo4j
from tools.caida.caida_tools import *
from tools.iyp.iyp_tools import *
from langgraph.graph import StateGraph, START, END
import json 
import re
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command
from langchain_core.tools import tool

SOURCE_TOOL_CATALOGUE = {
    'CAIDA_ToR': ['get_caida_tor'],
    'CAIDA': ['as_rank',
            'as_cone_size',
            'as2country',
            'as_peers_num',
            'as_providers_num',
            'as_transit_num',    
            'as_degree'
            ],
    'WHOIS': ['as_imports_with_other_asn',
            'as_exports_with_other_asn',
            'whois_as',
            'get_as_remarks'
    ],
    'PEERING_DB': ['get_as_deployed_inf_data',
                 'get_asn_aka',
                 'pdb_get_as_as_set',
                 'get_net_notes',
                 'get_netixlan_data',
                 'get_net_traffic_data_for_asn',
                 'pdb_as_type_info_type',
                 'pdb_as_type_info_types',
                 'get_net_policy_data'
    ],
    'ROUTES': [
        'get_random_routes_with_asns'  
    ]
}

# def _clean_text(txt: str) -> str:
#     """Minimal PDF cleanup: fix hyphenated line breaks + compress whitespace."""
#     txt = re.sub(r"(\w+)-\s*\n(\w+)", r"\1\2\n", txt) # glue hyphenated words across line breaks
#     txt = re.sub(r"[ \t]+\n", "\n", txt) # strip trailing spaces before newline
#     txt = re.sub(r"\n{3,}", "\n\n", txt) # limit blank lines
#     return txt

# def create_retreiver(
#         papers_folder_path: str,
#         chunk_size: int,
#         chunk_overlap: int,
#         *,
#         k: int = 6,
#         embedding_model: str = "text-embedding-3-small",
#     ):
#     # Load many papers from a folder (kept as-is)
#     loader = PyPDFDirectoryLoader(papers_folder_path)
#     docs = loader.load()

#     # Extract the Document objects from the tuples (kept your safeguard)
#     docs_list = [item[0] if isinstance(item, tuple) else item for item in docs]

#     # Minimal cleanup + stable metadata
#     for d in docs_list:
#       d.page_content = _clean_text(d.page_content or "")
#       src = d.metadata.get("source") or d.metadata.get("file_path")
#       d.metadata["source"] = src
#       if src:
#         d.metadata["doc_id"] = os.path.basename(src)

#     # Your splitter, just parameterized
#     text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
#     chunk_size=chunk_size, chunk_overlap=chunk_overlap
#     )
#     doc_splits = text_splitter.split_documents(docs_list)

#     # Drop exact-duplicate chunks (common with headers/footers/ToC); minimal & fast
#     seen, unique_splits = set(), []
#     for d in doc_splits:
#       h = hashlib.md5(d.page_content.encode("utf-8")).hexdigest()
#       if h not in seen:
#           seen.add(h)
#           unique_splits.append(d)

#     # In-memory vector store, but faster/cheaper embedding model
#     vectorstore = InMemoryVectorStore.from_documents(
#     documents=unique_splits, embedding=OpenAIEmbeddings(model=embedding_model)
#     )

#     # Expose k; same simple retriever API
#     retriever = vectorstore.as_retriever(search_kwargs={"k": k})
#     return retriever

# def build_retriever_tools(
#         papers_folder_path: str,
#         chunk_size: int,
#         chunk_overlap: int,
#         *,
#         k: int = 6,
#     ):
#     # Build the retriever first
#     retriever = create_retreiver(papers_folder_path, chunk_size, chunk_overlap, k=k)

#     # Wrap as a LangChain retriever tool (kept your interface/name)
#     retriever_tool = create_retriever_tool(
#         retriever,
#         "retrieve_BGP_problems_data",
#         "This tool has to retrieve data about BGP according user's request",
#     )
#     return retriever_tool

def _relationship_canon(rel: str) -> str:
    """Canonicalize relationship tokens to {'p2p','p2c','c2p'}."""
    if not rel:
        return ""
    s = str(rel).strip().lower()
    mapping = {
        "peers": "p2p",
        "peer": "p2p",
        "peer-to-peer": "p2p",
        "p2p": "p2p",
        "provider-to-customer": "p2c",
        "p2c": "p2c",
        "customer-to-provider": "c2p",
        "c2p": "c2p",
    }
    return mapping.get(s, s)

def _normalize_yes_no(text: str) -> str:
    """Return 'YES' or 'NO' if text indicates yes/no (robust to casing, punctuation).
    Examples: 'Yes.', ' YES ', 'no!', 'No\n' -> 'YES'/'NO'. Otherwise returns ''.
    """
    if text is None:
        return ""
    cleaned = re.sub(r"[^a-z]", "", str(text).lower())  # keep only letters
    if cleaned.startswith("yes"):
        return "YES"
    if cleaned.startswith("no"):
        return "NO"
    return ""

def route_after_connectivity(state) -> Literal["ToR_agent", "__end__"]:
    last_msg = state["messages"][-1].content if state.get("messages") else ""
    yn = _normalize_yes_no(last_msg)
    return "ToR_agent" if yn == "YES" else "__end__"


def route_after_caida_tor(state) -> Literal["ToR_agent", END]:
    last = state["messages"][-1].content.strip().upper()
    if last == "UNKNOWN" or "NO RELATIONSHIP FOUND" in last:
        return END
    return "ToR_agent"

def connectivity_check_node(state, model) -> Command:
    connectivity_check_agent = create_react_agent(
        model,
        tools=[are_connected],
        prompt=(
            """Check whether two given ASNs are directly connected to each other via are_connected tool.\n"
            "Answer Yes/No without any other information."""
            # """Accept inputs like "AS123", "asn 123", "ASN123" or "123". Extract the integer ASNs for both sides."""
            # """If the user supplies fewer or more than two ASNs, ask for exactly two ASNs (briefly and once)."""
        ),
    )
    result = connectivity_check_agent.invoke(state)
    return Command(update={"messages": result["messages"]})

# def caida_tor_node(state, model) -> Command:
#     caida_tor_agent = create_react_agent(
#         model,
#         tools=[get_caida_tor],
#         prompt=caida_tor_prompt,
#     )
#     result = caida_tor_agent.invoke(state)
#     return Command(update={"messages": result["messages"]})


def tor_agent_node(state: MessagesState, agent) -> Command[Literal[END]]:
    result = agent.invoke(state)

    return Command(
        update={
            # share internal message history of chart agent with other agents
            "messages": result["messages"],
        },
    )

def _extract_json_block(text: str):
    if not text:
        return None
    m = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"```\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        return m.group(1)
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start:end + 1]
    return None

def _normalize_asn(x):
    if x is None:
        return None
    if isinstance(x, int):
        return f"AS{x}"
    s = str(x).upper()
    m = re.search(r"(\d+)", s)
    return f"AS{m.group(1)}" if m else None

def _validate_tor_payload(obj):
    errors = []
    rel = _relationship_canon(obj.get("relationship", ""))
    if rel not in {"p2c", "c2p", "p2p"}:
        errors.append("relationship must be one of {'p2c','p2p','c2p'}")

    # Normalize provider/customer per relationship
    provider = obj.get("provider")
    customer = obj.get("customer")

    if rel == "c2p":
        # flip into p2c for downstream consistency
        provider, customer = customer, provider
        rel = "p2c"

    # Reasoning may be str or list[str]; normalize
    reasoning = obj.get("reasoning")
    if isinstance(reasoning, str):
        reasoning = [reasoning]
    if not isinstance(reasoning, list) or not all(isinstance(x, str) for x in reasoning):
        errors.append("reasoning must be a list of strings")
        reasoning = []

    # Relationship-specific ASN requirements
    if rel == "p2p":
        # For peers, provider/customer MUST be null/omitted; do not error if present, just null them.
        provider_norm = None
        customer_norm = None
    else:  # p2c
        provider_norm = _normalize_asn(provider)
        customer_norm = _normalize_asn(customer)
        if not provider_norm:
            errors.append("provider must be a valid ASN")
        if not customer_norm:
            errors.append("customer must be a valid ASN")

    normalized = {
        "relationship": rel,
        "provider": provider_norm,
        "customer": customer_norm,
        "reasoning": reasoning,
    }
    return (len(errors) == 0), errors, normalized

def _required_tools_missing(tools_used: List[str], required_tools_map: dict) -> dict:
    used = set(tools_used or [])
    missing = {}
    for source, tools in required_tools_map.items():
        tools_set = set(tools)
        # If the agent touched this data source (intersection non-empty),
        # enforce that it used ALL tools from this source
        if used.intersection(tools_set):
            miss = sorted(list(tools_set - used))
            if miss:
                missing[source] = miss
    return missing


def _observer_prompt(required_tools_map: Dict[str, List[str]]):
    required_tools_json = json.dumps(required_tools_map, ensure_ascii=False)
    return f"""
ROLE
You are the OBSERVER agent. Your job is to validate the ToR agent's final answer **and** the reasoning hygiene.

REQUIREMENTS YOU ENFORCE
- For each selected data source, the ToR workflow used **at least one** tool from that source: **{required_tools_json}**
- The final JSON answer must be certain (relationship ∈ {{'p2p','p2c'}}). For **p2p**, provider/customer **must be null**. For **p2c**, provider/customer must be valid ASNs.

SPECIAL CONTEXT
A system message titled `OBSERVER_CONTEXT` is appended to this chat with a JSON object containing:
  {{
    "tools_used": ["..."],
    "required tools": {required_tools_json}
  }}

TOOLS
- validate_tor_json(payload) → returns:
  - "VALID\\n{{normalized_json}}" on success
  - "INVALID — <errors>" on failure

WHAT TO DO (very important)
1) Locate the most recent assistant message produced by the ToR agent (it should be immediately before this observer step). Copy its FULL text as `tor_text`.
2) Read the `OBSERVER_CONTEXT` system message to get `tools_used` and the required tools mapping.
3) Call `validate_tor_json` **exactly once** with a single JSON argument:
   ```json
   {{
     "tor_text": "<paste the ToR agent's full message>",
     "tools_used": [<paste tools_used array from OBSERVER_CONTEXT>],
     "required tools": {required_tools_json}
   }}
   ```
4) If you cannot find the ToR agent's message, output: "OBSERVER: INVALID — No ToR agent output found."

OUTPUT FORMAT (no chain-of-thought)
- If the tool returns "VALID\\n{{...}}", you **must** output exactly these two lines (JSON on the second line). Do not omit the JSON:
  OBSERVER: VALID
  {{...}}
- If the tool returns "INVALID — ...", output exactly:
  OBSERVER: INVALID — ...
  ACTION: ToR_agent — retry and output a strict JSON object with keys [relationship, provider, customer, reasoning] (reasoning must be a list of strings) and no extra prose.
"""

@tool("validate_tor_json")
def validate_tor_json(payload: str) -> str:
    """
    Validate the ToR agent's JSON and enforce relaxed tool-usage coverage.
    Input: JSON string with keys { tor_text:str, tools_used:list[str], "required tools":dict }
    Returns: "VALID\n<normalized_json>" | "INVALID — <errors>"
    """
    try:
        obj = json.loads(payload)
        tor_text = obj.get("tor_text")
        tools_used = obj.get("tools_used", []) or []
        required_map = obj.get("required tools")
    except Exception:
        tor_text = payload
        tools_used = []
        required_map = None

    if not isinstance(tor_text, str) or not tor_text.strip():
        return "INVALID — No ToR agent output found or bad 'tor_text'."

    raw_json = _extract_json_block(tor_text)
    if not raw_json:
        return "INVALID — No JSON payload found in ToR message."
    try:
        tor_payload = json.loads(raw_json)
    except Exception as e:
        return f"INVALID — JSON parse error: {str(e)}"

    ok, errs, normalized = _validate_tor_payload(tor_payload)

    # After canonicalization, only 'p2p' and 'p2c' should remain
    rel = (normalized.get("relationship") or "").strip().lower()
    if rel not in {"p2p", "p2c"}:
        errs.append("answer_uncertain_or_invalid_relationship")

    # Fallback required tools map to global if missing
    if not isinstance(required_map, dict):
        required_map = SOURCE_TOOL_CATALOGUE

    missing = _required_tools_missing(tools_used, required_map)
    if missing:
        compact = "; ".join(f"{src}: {', '.join(v)}" for src, v in missing.items())
        errs.append(f"missing_required_tools — {compact}")

    if errs:
        return "INVALID — " + "; ".join(errs)
    return "VALID\n" + json.dumps(normalized, ensure_ascii=False)

def observer_node(state, model, required_tools_map=None) -> Command:
    tools_used = _tools_called_in_messages(state.get("messages", []))
    req_map = required_tools_map or SOURCE_TOOL_CATALOGUE

    ctx = json.dumps({
        "tools_used": tools_used,
        "required tools": req_map,
    }, ensure_ascii=False)

    # Keep original messages handy to recover ToR JSON if the LLM omits it
    tor_agent_msg_text = state["messages"][-1].content if state.get("messages") else ""

    tmp_state = {"messages": state["messages"] + [("system", f"OBSERVER_CONTEXT\n{ctx}")]}

    observer_agent = create_react_agent(
        model=model,
        tools=[validate_tor_json],
        prompt=_observer_prompt(req_map),
    )
    result = observer_agent.invoke(tmp_state)

    try:
        last_msg = result["messages"][-1]
        content = getattr(last_msg, "content", "") or ""
    except Exception:
        content = ""

    if content.strip().startswith("OBSERVER: VALID") and "{" not in content:
        fixed_json = None
        raw = _extract_json_block(tor_agent_msg_text)
        if raw:
            try:
                obj = json.loads(raw)
                ok, errs, norm = _validate_tor_payload(obj)
                if ok:
                    fixed_json = json.dumps(norm, ensure_ascii=False)
            except Exception:
                pass
        if fixed_json:
            new_content = "OBSERVER: VALID\n" + fixed_json
            try:
                result["messages"][-1].content = new_content
            except Exception:
                result["messages"].append(type(last_msg)(content=new_content))

    return Command(update={"messages": result["messages"]})

def route_after_observer(state, max_retries: int = 3) -> Literal["ToR_agent", END]:
    """
    If the observer reports INVALID, send control back to ToR_agent (up to max_retries).
    Otherwise, finish.
    """
    last = state["messages"][-1].content if state.get("messages") else ""
    last = (last or "").strip()
    if last.startswith("OBSERVER: INVALID"):
        # Count how many times the observer has flagged INVALID so far to avoid infinite loops
        invalid_count = sum(
            1
            for m in state["messages"]
            if hasattr(m, "content") and str(getattr(m, "content", "")).startswith("OBSERVER: INVALID")
        )
        return "ToR_agent" if invalid_count <= max_retries else END
    return END

def compose_required_tools(selected_sources: List[str]) -> Dict[str, List[str]]:
    """Return a dict limited to the selected data sources.
    Example: compose_required_tools(["CAIDA_ToR", "PEERING_DB"]) -> {
    'CAIDA_ToR': [...], 'PEERING_DB': [...]
    }
    """
    out = {}
    for src in selected_sources:
        if src in SOURCE_TOOL_CATALOGUE:
            out[src] = SOURCE_TOOL_CATALOGUE[src]
    return out

def create_workflow(agent, model, sources: List[str]):
    req_map = compose_required_tools(sources)

    workflow = StateGraph(MessagesState)
    workflow.add_node("connectivity_check", lambda s: connectivity_check_node(s, model))
    workflow.add_node("ToR_agent", lambda s: tor_agent_node(s, agent))
    workflow.add_node("observer", lambda s: observer_node(s, model, req_map))

    workflow.add_edge(START, "connectivity_check")
    workflow.add_conditional_edges(
        "connectivity_check", route_after_connectivity, {"ToR_agent": "ToR_agent", END: END}
    )
    workflow.add_edge("ToR_agent", "observer")
    workflow.add_conditional_edges(
        "observer", route_after_observer, {"ToR_agent": "ToR_agent", END: END}
    )
    return workflow.compile()

def _tools_called_in_messages(messages) -> list[str]:
    """Return ordered unique list of tool names used within the given messages."""
    names = []
    for m in messages:
        # Collect from AIMessage.tool_calls (LangChain agents)
        tool_calls = getattr(m, "tool_calls", None)
        if tool_calls:
            for tc in tool_calls:
                name = tc.get("name") if isinstance(tc, dict) else getattr(tc, "name", None)
                if name:
                    names.append(name)
        # Collect from ToolMessage-like records
        m_type = getattr(m, "type", None)
        if m_type == "tool":
            name = getattr(m, "name", None) or getattr(m, "tool", None) or getattr(m, "tool_name", None)
            if name:
                names.append(name)
    # Dedupe while preserving order
    seen, ordered = set(), []
    for n in names:
        if n not in seen:
            seen.add(n)
            ordered.append(n)
    return ordered

def get_results(res):
    match = re.search(r"\{.*\}", res)
    if match:
        tor = json.loads(match.group())
        return tor

def query_agent(question, graph):
    results = None
    events = graph.stream(
        {
            "messages": [
                (
                    "user",
                    question,
                )
            ],
        },
        {"recursion_limit": 150},
    )
    for s in events:
        key = next(iter(s), None)
        messages = s[key].get("messages", []) if key else []
        if messages:
            print(messages[-1].content)
            results = messages[-1].content
        # NEW: when ToR_agent runs, print which tools it used in this step
        if key == "ToR_agent" and messages:
            tools_used = _tools_called_in_messages(messages)
            if tools_used:
                print(f"[ToR_agent tools used]: {', '.join(tools_used)}")
        print("----")
    return results

