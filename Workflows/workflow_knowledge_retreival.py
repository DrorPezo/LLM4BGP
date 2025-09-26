from typing_extensions import TypedDict
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, START, END
from IPython.display import Image, display
from typing_extensions import Literal
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_core.pydantic_v1 import BaseModel, Field


def invoke_model_with_tools(model, tools, query):
    ai_msg = model.invoke(query)
    for tool_call in ai_msg.tool_calls:
        name = tool_call["name"].lower()
        tool = next(t for t in tools if t.name.lower() == name)
        print(tool.name)
        tool_output = tool.invoke(tool_call["args"])
        answer = ToolMessage(tool_output, tool_call_id=tool_call["id"])
    return answer.content

# Schema for structured output to use as routing logic
class Route(BaseModel):
    step: Literal["as_rank", "as_customer_cone_size", "bogon"] = Field(
        None, description="The next step in the routing process"
    )

# State
class State(TypedDict):
    input: str
    decision: str
    output: str

# Nodes
def as_rank_calc(state, tools, model):
    """Perform AS rank"""
    
    result = invoke_model_with_tools(model, tools, state["input"])
    return {"output": result}

def as_customer_cone_size(state, tools, model):
    """Perform AS customer cone size"""

    result = invoke_model_with_tools(model, tools, state["input"])
    return {"output": result}

def bogon_detection(state, tools, model):
    """Perform bogon detection"""

    result = invoke_model_with_tools(model, tools, state["input"])
    return {"output": result}

class Route(BaseModel):
    """Route the input to the appropriate node."""

    step: str = Field(
        description="Given the user input, decide which node to route to. Options are 'as_rank', 'as_customer_cone_size', or 'bogon_detection'.",
    )

def llm_call_router(state, model):
    """Route the input to the appropriate node"""

    # Run the augmented LLM with structured output to serve as routing logic
    router = model.with_structured_output(Route)
    decision = router.invoke(
        [
            SystemMessage(
                content='''You are an expert in BGP routing and can categorize user questions into one of three types:
                1. CAIDA AS rank (return "as_rank")
                2. CAIDA AS customer cone size (return "as_customer_cone_size")
                3. Bogon IP prefix detection (return "bogon_detection")

                Based on the user's input, provide ONLY one of the following strings: "as_rank", "as_customer_cone_size", or "bogon_detection".
                '''
            ),
            HumanMessage(content=state["input"]),
        ]
    )

    return {"decision": decision.step}

# Conditional edge function to route to the appropriate node
def route_decision(state: State):
    # Return the node name you want to visit next
    if state["decision"] == "as_rank":
        return "as_rank"
    elif state["decision"] == "as_customer_cone_size":
        return "as_customer_cone_size"
    elif state["decision"] == "bogon_detection":
        return "bogon_detection"

def route_workflow(prompt, tools, model):
    # Build workflow
    router_builder = StateGraph(State)
    # Add nodes
    router_builder.add_node("as_rank", lambda state, config: as_rank_calc(state, tools, model=model))
    router_builder.add_node("as_customer_cone_size", lambda state, config: as_customer_cone_size(state, tools, model=model))
    router_builder.add_node("bogon_detection", lambda state, config: bogon_detection(state, tools, model=model))
    router_builder.add_node("llm_call_router", lambda state, config: llm_call_router(state, model=model))

    # Add edges to connect nodes
    router_builder.add_edge(START, "llm_call_router")
    router_builder.add_conditional_edges(
        "llm_call_router",
        route_decision,
        {  # Name returned by route_decision : Name of next node to visit
            "as_rank": "as_rank",
            "as_customer_cone_size": "as_customer_cone_size",
            "bogon_detection": "bogon_detection",
        },
    )
    router_builder.add_edge("as_rank", END)
    router_builder.add_edge("as_customer_cone_size", END)
    router_builder.add_edge("bogon_detection", END)

    # Compile workflow
    router_workflow = router_builder.compile()

    # Show the workflow
    display(Image(router_workflow.get_graph().draw_mermaid_png()))

    # Invoke
    state = router_workflow.invoke({"input": prompt})
    return state["output"]
