import re
from typing import List, Dict, Any, Tuple, Optional
from sklearn.metrics import precision_recall_fscore_support

def bogons_extract_label(text: str) -> Optional[str]:
    _BOGON_RE = re.compile(r"\bbogon\b", flags=re.IGNORECASE)
    _NON_BOGON_RE = re.compile(r"\bnon[-\s]?bogon\b", flags=re.IGNORECASE)
    if text is None:
        return None
    if _BOGON_RE.search(text):
        return "bogon"
    if _NON_BOGON_RE.search(text):
        return "non-bogon"
    return None


def bogons_collect_labels(outputs: List[Dict[str, Any]], reference_outputs: List[Dict[str, Any]],) -> Tuple[List[str], List[str]]:
    """Extract paired (y_true, y_pred) label lists, skipping invalid pairs."""
    y_true: List[str] = []
    y_pred: List[str] = []

    for out_d, ref_d in zip(outputs, reference_outputs):
        pred_raw = out_d.get("class") or out_d.get("text") or out_d.get("output") or ""
        true_raw = ref_d.get("class") or ref_d.get("text") or ref_d.get("output") or ""

        pred = bogons_extract_label(pred_raw)
        true = bogons_extract_label(true_raw)

        if pred is None or true is None:
            continue  # skip pairs lacking a valid label

        y_pred.append(pred)
        y_true.append(true)

    return y_true, y_pred

def bogon_precision_evaluator(outputs: List[Dict[str, Any]], reference_outputs: List[Dict[str, Any]],) -> Dict[str, float]:
    """Return precision for *bogon* detection as a LangSmith metric dict."""
    y_true, y_pred = bogons_collect_labels(outputs, reference_outputs)
    if not y_true:
        return {"key": "precision", "score": 0.0}

    precision, _, _, _ = precision_recall_fscore_support(
        y_true, y_pred, pos_label="bogon", average="binary", zero_division=0
    )
    return {"key": "precision", "score": precision}


def bogon_recall_evaluator(outputs: List[Dict[str, Any]], reference_outputs: List[Dict[str, Any]],) -> Dict[str, float]:
    """Return recall for *bogon* detection as a LangSmith metric dict."""
    y_true, y_pred = bogons_collect_labels(outputs, reference_outputs)
    if not y_true:
        return {"key": "recall", "score": 0.0}

    _, recall, _, _ = precision_recall_fscore_support(
        y_true, y_pred, pos_label="bogon", average="binary", zero_division=0
    )
    return {"key": "recall", "score": recall}

def bogon_f1_evaluator(outputs: List[Dict[str, Any]], reference_outputs: List[Dict[str, Any]],) -> Dict[str, float]:
    """Return F1‑score for *bogon* detection as a LangSmith metric dict."""
    y_true, y_pred = bogons_collect_labels(outputs, reference_outputs)
    if not y_true:
        return {"key": "f1_score", "score": 0.0}

    _, _, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, pos_label="bogon", average="binary", zero_division=0
    )
    return {"key": "f1_score", "score": f1}
    