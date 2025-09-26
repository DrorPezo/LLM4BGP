from typing_extensions import TypedDict
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, START, END
from IPython.display import Image, display
from typing_extensions import Literal
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_core.pydantic_v1 import BaseModel, Field

def invoke_model_with_tools(model, tools, query):
    ai_msg = model.invoke(query)
    print(ai_msg.tool_calls)
    for tool_call in ai_msg.tool_calls:
        name = tool_call["name"].lower()
        tool = next(t for t in tools if t.name.lower() == name)
        tool_output = tool.invoke(tool_call["args"])
        answer = ToolMessage(tool_output, tool_call_id=tool_call["id"])
    return answer.content

# Schema for structured output to use as routing logic
class Route(BaseModel):
    step: Literal["largest_as2org", "as_num_org", "as2org"] = Field(
        None, description="The next step in the routing process"
    )

# State
class State(TypedDict):
    input: str
    decision: str
    output: str

# Nodes
def largest_as2org(state, tools, model):
    """Find the ASN of the largest AS in a given organization"""

    result = invoke_model_with_tools(model, tools, state["input"])
    return {"output": result}

def as_num_org(state, tools, model):
    """Find the number of ASes which a given organization owns"""

    result = invoke_model_with_tools(model, tools, state["input"])
    return {"output": result}

def as2org_map(state, tools, model):
    """Perform AS2Org"""

    result = invoke_model_with_tools(model, tools, state["input"])
    return {"output": result}

class Route(BaseModel):
    """Route the input to the appropriate node."""

    step: str = Field(
        description="Given the user input, decide which node to route to. Options are 'largest_as2org', 'as_num_org', or 'as2org'.",
    )

def llm_call_router(state, model):
    """Route the input to the appropriate node"""

    # Run the augmented LLM with structured output to serve as routing logic
    router = model.with_structured_output(Route)
    decision = router.invoke(
        [
            SystemMessage(
                content='''
                You are an expert in BGP routing and can categorize user questions into one of three types:
                1. Find the ASN of the largest AS in a given organization (return "largest_as2org")
                2. Find the number of ASes which a given organization owns (return "as_num_org")
                3. Find the organization's name which owns a given AS, i.e., perform AS2Org (return "as2org")

                Based on the user's input, provide ONLY one of the following strings: "largest_as2org", "as_num_org", or "as2org".
                '''
            ),
            HumanMessage(content=state["input"]),
        ]
    )

    return {"decision": decision.step}

# Conditional edge function to route to the appropriate node
def route_decision(state: State):
    # Return the node name you want to visit next
    if state["decision"] == "largest_as2org":
        return "largest_as2org"
    elif state["decision"] == "as_num_org":
        return "as_num_org"
    elif state["decision"] == "as2org":
        return "as2org"

def route_workflow(prompt, tools, model):
    # Build workflow
    router_builder = StateGraph(State)

    # Add nodes
    router_builder.add_node("largest_as2org", lambda state, config: largest_as2org(state, tools, model=model))
    router_builder.add_node("as_num_org", lambda state, config: as_num_org(state, tools, model=model))
    router_builder.add_node("as2org", lambda state, config: as2org_map(state, tools, model=model))
    router_builder.add_node("llm_call_router", lambda state, config: llm_call_router(state, model=model))

    # Add edges to connect nodes
    router_builder.add_edge(START, "llm_call_router")
    router_builder.add_conditional_edges(
        "llm_call_router",
        route_decision,
        {  # Name returned by route_decision : Name of next node to visit
            "largest_as2org": "largest_as2org",
            "as_num_org": "as_num_org",
            "as2org": "as2org",
        },
    )
    router_builder.add_edge("largest_as2org", END)
    router_builder.add_edge("as_num_org", END)
    router_builder.add_edge("as2org", END)

    # Compile workflow
    router_workflow = router_builder.compile()

    # Show the workflow
    display(Image(router_workflow.get_graph().draw_mermaid_png()))

    # Invoke
    state = router_workflow.invoke({"input": prompt})
    print(state["output"])
    return state["output"]
