import json
import re
from typing import Any, Dict, List, Tuple, Callable
from langsmith import Client
from langsmith.evaluation import EvaluationResult
from langchain_openai import ChatOpenAI
from langchain.schema import SystemMessage, HumanMessage
from single_agents.tor_agent import *

_REL_ALLOWED = {"p2p", "p2c", "c2p"}
_OBSERVER_VALID_RE = re.compile(r"OBSERVER\s*:\s*VALID\s*(?:\n|\r\n|\r)\s*(\{[\s\S]*?\})", re.IGNORECASE)
_ASN_RE = re.compile(r"AS(\d+)")
_GT_REL_RE = re.compile(r"(Peers|Provider to Customer)", re.IGNORECASE)
_GT_P2C_DETAIL_RE = re.compile(
r"Provider\s*to\s*Customer\s*,?\s*where\s*AS(\d+)\s*is\s*the\s*provider\s*and\s*AS(\d+)\s*is\s*the\s*customer",
re.IGNORECASE,
)

def _find_json_objects(text: str) -> List[str]:
    objs: List[str] = []
    i, n = 0, len(text)
    while i < n:
        if text[i] == '{':
            depth = 1
            j = i + 1
            while j < n and depth > 0:
                if text[j] == '{':
                    depth += 1
                elif text[j] == '}':
                    depth -= 1
                j += 1
            if depth == 0:
                objs.append(text[i:j])
                i = j
                continue
        i += 1
    return objs

def _norm_asn(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, int):
        return f"AS{v}"
    s = str(v).strip().upper()
    m = _ASN_RE.fullmatch(s) or _ASN_RE.search(s)
    if m:
        return f"AS{m.group(1)}"
    if s.isdigit():
        return f"AS{s}"
    return None

def _coerce_reasoning(x: Any) -> List[str]:
    if isinstance(x, list):
        return [str(y) for y in x]
    if x is None:
        return []
    return [str(x)]

def _normalize_candidate(d: Dict[str, Any]) -> Dict[str, Any] | None:
    if not isinstance(d, dict):
        return None
    rel = str(d.get("relationship", "")).lower().strip()
    # NEW: map common synonyms to canonical labels
    if rel in {"peer", "peers", "peering"}:
        rel = "p2p"
    elif rel in {"provider to customer", "provider→customer", "provider-customer", "provider_to_customer"}:
        rel = "p2c"
    elif rel in {"customer to provider", "customer→provider", "customer-provider", "customer_to_provider"}:
        rel = "c2p"

    if rel not in _REL_ALLOWED:
        return None

    prov = _norm_asn(d.get("provider"))
    cust = _norm_asn(d.get("customer"))

    if rel in {"p2c", "c2p"}:
        if not (prov and cust):
            return None
        # Canonicalize c2p → p2c with swapped ends
        if rel == "c2p":
            prov, cust = cust, prov
            rel = "p2c"

    return {
        "relationship": rel,
        "provider": prov,
        "customer": cust,
        "reasoning": _coerce_reasoning(d.get("reasoning")),
    }

def rule_based_eval(inputs: Dict[str, Any], outputs: Dict[str, Any], reference_outputs: Dict[str, Any]) -> EvaluationResult:
    gt = parse_ground_truth(reference_outputs.get("answer", "")) or {}
    pred = outputs.get("normalized") or {}
    if not gt or not pred:
        return {"score": 0.0, "value": 0.0, "comment": "missing_gt_or_pred"}

    if gt["relationship"] != pred.get("relationship"):
        return {"score": 0.0, "value": 0.0, "comment": _RULE_COMMENT}

    if gt["relationship"] == "p2p":
        # For p2p we ignore provider/customer entirely
        return {"score": 1.0, "value": 1.0, "comment": "ok"}

    # p2c
    gprov, gcust = gt.get("provider"), gt.get("customer")
    pprov, pcust = pred.get("provider"), pred.get("customer")

    # Require predicted ends present and valid for p2c
    if not pprov or not pcust:
        return {"score": 0.0, "value": 0.0, "comment": "pred_missing_asn"}

    # If GT specified ends, they must match exactly
    if gprov and gprov != pprov:
        return {"score": 0.0, "value": 0.0, "comment": f"provider_mismatch gt={gprov} pred={pprov}"}
    if gcust and gcust != pcust:
        return {"score": 0.0, "value": 0.0, "comment": f"customer_mismatch gt={gcust} pred={pcust}"}

    return {"score": 1.0, "value": 1.0, "comment": "ok"}

def extract_normalized_from_transcript(text: str) -> Tuple[Dict[str, Any] | None, str]:
    """Pick the *best* structured JSON from the transcript, not just the last one.
    Preference order:
      • Valid ToR JSON with relationship='p2c' AND both provider & customer
      • Valid ToR JSON with relationship='p2c'
      • Valid ToR JSON with relationship='p2p'
    Sources scanned:
      1) All JSON blocks captured after 'OBSERVER: VALID' lines (if present)
      2) All JSON-looking blocks in the entire transcript
    """
    if not text:
        return None, "empty"

    candidates: List[Tuple[str, str]] = []

    # 1) Collect any JSON immediately following 'OBSERVER: VALID' lines
    for m in _OBSERVER_VALID_RE.finditer(text):
        js = m.group(1)
        if js:
            candidates.append(("observer_valid", js))

    # 2) Collect every JSON-looking block from the transcript
    for js in _find_json_objects(text):
        candidates.append(("json_scan", js))

    best_norm: Dict[str, Any] | None = None
    best_src = "no_valid_json"
    best_score = -1

    for src, js in candidates:
        try:
            obj = json.loads(js)
        except Exception:
            continue
        norm = _normalize_candidate(obj)
        if not norm:
            continue

        # Scoring: prefer most informative / complete answer
        score = 1  # baseline for a valid normalized candidate
        rel = norm.get("relationship")
        prov = norm.get("provider")
        cust = norm.get("customer")

        if rel == "p2p":
            score += 2
        elif rel == "p2c":
            score += 3  # p2c is more constrained than p2p
            if prov and cust:
                score += 3  # has both ends → much stronger

        # Tie-breaker: prefer JSONs from the general scan (often the full agent answer)
        if score > best_score or (score == best_score and best_src == "observer_valid" and src == "json_scan"):
            best_score = score
            best_norm = norm
            best_src = src

    return (best_norm, best_src) if best_norm else (None, "no_valid_json")


def parse_ground_truth(answer: Any) -> Dict[str, Any]:
    """Accepts either:
    • Natural-language sentence (legacy), or
    • A dict / JSON string produced by the model (preferred).
    Returns canonical GT with relationship in {p2p,p2c} and provider/customer as "AS###" or None.
    """
    # 1) If already a dict, normalize directly
    if isinstance(answer, dict):
        norm = _normalize_candidate(answer)
        return norm or {}

    # 2) If it looks like a JSON string, try to parse then normalize
    if isinstance(answer, str) and answer.strip().startswith("{"):
        try:
            obj = json.loads(answer)
            norm = _normalize_candidate(obj)
            if norm:
                return norm
        except Exception:
            pass

    # 3) Fallback: parse the legacy natural-language sentence
    if not isinstance(answer, str) or not answer:
        return {}

    m_rel = _GT_REL_RE.search(answer)
    if not m_rel:
        return {}

    kind = m_rel.group(1).lower()
    if kind.startswith("peer"):
        return {"relationship": "p2p", "provider": None, "customer": None}

    m = _GT_P2C_DETAIL_RE.search(answer)
    if m:
        return {"relationship": "p2c", "provider": f"AS{m.group(1)}", "customer": f"AS{m.group(2)}"}

    # If no explicit provider/customer specified, still mark p2c but leave ends None
    return {"relationship": "p2c", "provider": None, "customer": None}

def compare(gt: Dict[str, Any], pred: Dict[str, Any]) -> bool:
    if not gt:
        return False
    if gt["relationship"] == "p2p":
        return pred.get("relationship") == "p2p"
    if gt["relationship"] == "p2c":
        if pred.get("relationship") != "p2c":
            return False
        if gt["provider"] and gt["provider"] != pred.get("provider"):
            return False
        if gt["customer"] and gt["customer"] != pred.get("customer"):
            return False
        return True
    return False

def evaluate(gt_answers: List[str], agent_outputs: List[Dict[str, Any]]) -> Dict[str, Any]:
    results = []
    correct = 0
    for gt_str, pred in zip(gt_answers, agent_outputs):
        gt = parse_ground_truth(gt_str)
        ok = compare(gt, pred)
        results.append({"ground_truth": gt, "predicted": pred, "correct": ok})
        if ok:
            correct += 1
    acc = correct / len(gt_answers) if gt_answers else 0.0
    return {"summary": {"accuracy": acc, "total": len(gt_answers), "correct": correct}, "details": results}

def llm_as_judge_eval(model_name: str = "o3-mini") -> Callable[[Dict[str, Any], Dict[str, Any], Dict[str, Any]], EvaluationResult]:
    _LLM_SYS = (
        "You are an AS Type-of-Relationship (ToR) judge. "
        "Compare MODEL_JSON vs GROUND_TRUTH using ONLY keys {relationship, provider, customer}. "
        "Normalize before judging:\n"
        "- relationship: map {peer, peers, peering, p2p}→'p2p'; "
        "{provider to customer, provider→customer, provider-customer, provider_to_customer, p2c}→'p2c'; "
        "{customer to provider, customer→provider, customer-provider, customer_to_provider, c2p}→'p2c' AND swap provider/customer.\n"
        "- provider/customer: accept 'AS123' or '123' and normalize to 'AS123'.\n"
        "Rules:\n"
        "1) If relationship is 'p2p', return 1 (ignore provider/customer).\n"
        "2) If relationship is 'p2c', MODEL_JSON must have both provider and customer. "
        "If GROUND_TRUTH specifies either end, it must match; if an end is None in GROUND_TRUTH, do not require it to match.\n"
        "Answer strictly with 1 or 0."
    )
    judge = ChatOpenAI(model_name=model_name)
    def _eval(inputs: Dict[str, Any], outputs: Dict[str, Any], reference_outputs: Dict[str, Any]):
        gt = parse_ground_truth(reference_outputs.get("answer", "")) or {}
        pred = outputs.get("normalized") or {}
        msg = f"GROUND_TRUTH: {json.dumps(gt)}\nMODEL_JSON: {json.dumps(pred)}"
        res = judge.invoke([SystemMessage(content=_LLM_SYS), HumanMessage(content=msg)])
        score = 1.0 if str(res.content).strip().startswith("1") else 0.0
        return {"score": score, "value": score}
    return _eval

def _make_target(graph) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    def target(inputs: Dict[str, Any]) -> Dict[str, Any]:
        q = inputs.get("question", "")
        transcript = query_agent(q, graph)
        norm, path = extract_normalized_from_transcript(transcript or "")
        return {"transcript": transcript, "normalized": norm or {}, "parse_path": path}
    return target

def run_langsmith_eval(
    client: Client,
    dataset,
    graph,
    *,
    experiment_prefix: str = "tor-multi-agent",
    judge_model: str = "o3-mini",
) -> None:
    """
    Run LangSmith evaluation directly on an existing dataset.
    The dataset should already contain inputs {"question": ...} and outputs {"answer": ...}.
    """
    evaluators: List[Any] = [llm_as_judge_eval(judge_model)]

    client.evaluate(
        _make_target(graph),
        data=dataset,
        evaluators=evaluators,
        experiment_prefix=experiment_prefix,
        max_concurrency=2,
    )

def convert_tor_qas_to_examples(qas: List[Dict[str, str]]) -> List[Dict[str, Dict[str, Any]]]:
    """Convert ToR Q&A list into LangSmith examples."""
    return [
        {"inputs": {"question": qa["question"]}, "outputs": {"answer": qa["answer"]}}
        for qa in qas
    ]

def ensure_tor_dataset(client: Client, qas: List[Dict[str, str]], dataset_name: str = "AS ToR Inference Q&A dataset"):
    tor_dataset = None
    for ds in client.list_datasets():
        if ds.name == dataset_name:
            tor_dataset = ds
            break
    if tor_dataset is None:
        tor_dataset = client.create_dataset(
            dataset_name=dataset_name,
            description="LangSmith dataset for AS ToR inference evaluation"
        )
        examples = convert_tor_qas_to_examples(qas)
        client.create_examples(dataset_id=tor_dataset.id, examples=examples)
    return tor_dataset
