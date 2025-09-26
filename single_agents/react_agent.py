from typing import (
    Annotated,
    Sequence,
    TypedDict,
)
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from langchain import hub
import json
from langchain_core.messages import ToolMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, END
from IPython.display import Image, display
from evaluations.bogons import *

react_prompt = hub.pull("hwchase17/react")

class AgentState(TypedDict):
    """The state of the agent."""
    messages: Annotated[Sequence[BaseMessage], add_messages]

# Define our tool node
def tool_node(state: AgentState, tools_by_name):
    outputs = []
    for tool_call in state["messages"][-1].tool_calls:
        tool_result = tools_by_name[tool_call["name"]].invoke(tool_call["args"])
        outputs.append(
            ToolMessage(
                content=json.dumps(tool_result),
                name=tool_call["name"],
                tool_call_id=tool_call["id"],
            )
        )
    return {"messages": outputs}

# Define the node that calls the model
def call_model(state: AgentState, config: RunnableConfig, task_prompt, model):
    # this is similar to customizing the create_react_agent with 'prompt' parameter, but is more flexible
    system_prompt = SystemMessage(task_prompt)
    response = model.invoke([system_prompt] + state["messages"], config)
    # We return a list, because this will get added to the existing list
    return {"messages": [response]}

# Define the conditional edge that determines whether to continue or not
def should_continue(state: AgentState):
    messages = state["messages"]
    last_message = messages[-1]
    # If there is no function call, then we finish
    if not last_message.tool_calls:
        return "end"
    # Otherwise if there is, we continue
    else:
        return "continue"

# Helper function for formatting the stream nicely
def print_stream(stream):
    for s in stream:
        message = s["messages"][-1]
        if isinstance(message, tuple):
            print(message)  
        else:
            message.pretty_print()


def call_react_agent(task_prompt, model, tools, query):
    # Define a new graph
    workflow = StateGraph(AgentState)

    # Create a dictionary of tools by name
    tools_by_name = {tool.name: tool for tool in tools}

    # Define the two nodes we will cycle between
    workflow.add_node("agent", lambda state, config: call_model(state, config, task_prompt=task_prompt, model=model))
    workflow.add_node("tools", lambda state, config: tool_node(state, tools_by_name))

    # Set the entrypoint as `agent`
    # This means that this node is the first one called
    workflow.set_entry_point("agent")

    # We now add a conditional edge
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {
            "continue": "tools",
            "end": END,
        },
    )

    workflow.add_edge("tools", "agent")

    # Now we can compile and visualize our graph
    graph = workflow.compile()
    # try:
    #     display(Image(graph.get_graph().draw_mermaid_png()))
    # except Exception:
    #     pass
    
    inputs = {"messages": [("user", query)]}
    stream_output = graph.stream(inputs, {"recursion_limit": 15}, stream_mode="values")
    final_answer = None
    for rel in stream_output:
        if 'messages' in rel and len(rel['messages']) > 0:
            messages = rel['messages'][-1]
            final_answer =  messages.content
    return final_answer

