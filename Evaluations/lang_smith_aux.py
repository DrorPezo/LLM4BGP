from langchain_core.runnables import RunnableLambda, RunnableSequence
from langchain_core.runnables.base import RunnableLike
from collections import Counter
from typing import List, Callable, Awaitable
from langsmith.evaluation import EvaluationResult
from langsmith.run_trees import RunTree
from langchain_openai import ChatOpenAI
from openevals.llm import create_llm_as_judge
from openevals.prompts import CORRECTNESS_PROMPT
from langchain.schema import SystemMessage, HumanMessage
from langchain_core.messages.tool import ToolMessage
from collections import Counter


def convert_to_examples(qas):
    """Return a list of examples in the desired schema."""
    return [
        {
            "inputs":  {"question": qa["question"]},
            "outputs": {"answer":   qa["answer"]},
        }
        for qa in qas
    ]

# Prompt Engineering 
def target_wrap(model, prompt):
    """Return a target fn with `prompt` baked in."""

    def return_tools_list(outputs):
        called_tools = []
        # Check if the outputs object has a tool_calls attribute and if it's not empty
        if hasattr(outputs, 'tool_calls') and outputs.tool_calls:
            for tool in outputs.tool_calls:
                # Access the 'name' key from the tool dictionary
                if 'name' in tool:
                    called_tools.append(tool['name'])
        return called_tools

    def target(inputs) -> dict:         
        response = model.invoke([
            SystemMessage(content=prompt),
            HumanMessage(content=inputs["question"]),
        ])
        tools_called = return_tools_list(response)
        return {
            "answer": response.content.strip(),
            "tools_called": tools_called
        }
    return target

def correctness_evaluator(inputs: dict, outputs: dict, reference_outputs: dict):
    evaluator = create_llm_as_judge(
        prompt=CORRECTNESS_PROMPT,
        model="openai:o3-mini",
        feedback_key="correctness",
    )
    eval_result = evaluator(
        inputs=inputs,
        outputs=outputs,
        reference_outputs=reference_outputs
    )
    return eval_result

def hallucination_evaluator(prompt:str, inputs: dict, outputs: dict, reference_outputs: dict):
    llm_as_judge = create_llm_as_judge(
    prompt=prompt,
    feedback_key="hallucination",
    model="openai:o3-mini",
    )

    eval_result = llm_as_judge(
        inputs=inputs,
        outputs=outputs,
        context="",
        reference_outputs="",
    )
    return eval_result

def eval_prompt_engineering_on_openai(client, model, prompt, hall_prompt, data, experiment, summary_evaluators):
    # temps = list(i / 5 for i in range(6))
    temps = [0.0]
    for temp in temps:
        model.temperature = temp
        target = target_wrap(model, prompt)
        client.evaluate(
            target,
            data=data,
            evaluators=[
                correctness_evaluator,
                # lambda inputs, outputs, reference_outputs, hall_prompt=hall_prompt: hallucination_evaluator(hall_prompt, inputs, outputs, reference_outputs)
            ],
            summary_evaluators=summary_evaluators,
            experiment_prefix=f"{experiment}-temp-{temp}",
            max_concurrency=2,
        )

# Agents
async def correct(outputs, correcntess_prompt, reference_outputs):
    actual_answer = outputs
    print(f"Agent answer:{actual_answer}")
    expected_answer = reference_outputs["answer"]
    print(f"Ground truth:{expected_answer}")
    user_msg = (
        f"ACTUAL ANSWER: {actual_answer}"
        f"\n\nEXPECTED ANSWER: {expected_answer}"
    )
    judge_llm = ChatOpenAI(model_name="o3-mini")
    response = await judge_llm.ainvoke(
        [
            correcntess_prompt +
            f"""Do the judgement for the following data: 
                Ground Truth: {expected_answer} 
                LLM Output: {actual_answer}"""
        ]
    )
    print(f"Judgement:{response.content}")
    return int(response.content)

def make_correctness_evaluator(correct_prompt):
    """Return an async evaluator compatible with `aevaluate`."""

    async def correctness_evaluator(outputs, reference_outputs):
        return await correct(outputs, correct_prompt, reference_outputs)

    return correctness_evaluator

async def hallucination_judge(outputs, hall_prompt, reference_outputs):
    actual_answer = outputs
    print(f"Agent answer:{actual_answer}")
    expected_answer = reference_outputs["answer"]
    print(f"Ground truth:{expected_answer}")
    user_msg = (
        f"ACTUAL ANSWER: {actual_answer}"
        f"\n\nEXPECTED ANSWER: {expected_answer}"
    )
    judge_llm = ChatOpenAI(model_name="o3-mini")
    response = await judge_llm.ainvoke(
        [
            hall_prompt
        ]
    )
    print(f"Judgement:{response.content}")
    return int(response.content)

def make_hallucination_evaluator(hall_prompt):
    """Return an async evaluator compatible with `aevaluate`."""

    async def hallucination_evaluator(outputs, reference_outputs):
        return await hallucination_judge(outputs, hall_prompt, reference_outputs)

    return hallucination_evaluator

def make_unordered_trajectory_match_evaluator(
    expected_tools: List[str],
) -> Callable[[RunTree], Awaitable[EvaluationResult]]:

    async def unordered_trajectory_match_evaluator(
        run: RunTree,
        reference_run: RunTree | None = None,
        **kwargs,
    ) -> EvaluationResult:

        tool_calls = run.outputs.get("tools_called", [])

        match = Counter(tool_calls) == Counter(expected_tools)
        score = 1.0 if match else 0.0

        return {
            "score": score,
            "value": score,  # Helpful for quick inspection in the UI.
            "comment": (
                f"tools_called={tool_calls} | expected={expected_tools}"
            ),
        }

    return unordered_trajectory_match_evaluator

def make_strict_trajectory_match_evaluator(
    expected_tools: List[str],
) -> Callable[[RunTree], Awaitable[EvaluationResult]]:

    async def strict_trajectory_match_evaluator(
        run: RunTree,
        reference_run: RunTree | None = None,
        **kwargs,
    ) -> EvaluationResult:

        tool_calls = run.outputs.get("tools_called", [])

        match = tool_calls == expected_tools
        score = 1.0 if match else 0.0

        return {
            "score": score,
            "value": score,  # Helpful for quick inspection in the UI.
            "comment": (
                f"tools_called={tool_calls} | expected={expected_tools}"
            ),
        }

    return strict_trajectory_match_evaluator

def agent_wrap(agent: RunnableLike, prompt: str) -> RunnableSequence: 
    build_state = RunnableLambda(
        lambda inputs: {
            "messages": [
                {
                    "role": "system",
                    "content": inputs.get("question", prompt),
                }
            ]
        }
    )

    def return_tools_list(outputs):
        called_tools = []

        for message in outputs['messages']:
            if isinstance(message, ToolMessage):
              called_tools.append(message.name)

        return called_tools

    extract_answer = RunnableLambda(
        lambda outputs: {
                        "answer": outputs["messages"][-1].content,
                        "tools_called": return_tools_list(outputs)}
    )

    return build_state | agent | extract_answer
