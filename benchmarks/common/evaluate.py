"""Answer evaluation helpers shared by all benchmark tiers."""

from __future__ import annotations

import json
import math
import re
import statistics


def _extract_first_number(text: str) -> float | None:
    """Extract the first numeric token from text."""
    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if match:
        return float(match.group())
    return None


def match_number(
    llm_answer: str,
    gold_answer: str,
    *,
    tolerance: float = 0.0,
) -> bool:
    """Check if LLM answer matches gold numerically within tolerance.

    Tolerance is absolute.  For example, ``tolerance=0.01`` accepts
    answers within +-0.01 of gold.  The default ``0.0`` requires
    exact numeric match.
    """
    try:
        gold_val = float(gold_answer.replace(",", ""))
    except ValueError:
        return False

    try:
        llm_val = float(llm_answer.strip().replace(",", ""))
    except ValueError:
        llm_val_maybe = _extract_first_number(llm_answer)
        if llm_val_maybe is None:
            return False
        llm_val = llm_val_maybe

    return abs(llm_val - gold_val) <= tolerance


def match_string(llm_answer: str, gold_answer: str) -> bool:
    """Check if LLM answer matches gold string.

    Accepts exact match or gold appearing as a whole-word sequence
    inside the LLM answer (the LLM elaborated).  Uses word-boundary
    anchors so short golds like ``"ak"`` do not false-match inside
    unrelated words like ``"make"``.
    """
    llm_clean = llm_answer.strip().lower()
    gold_clean = gold_answer.strip().lower()
    if not llm_clean or not gold_clean:
        return llm_clean == gold_clean
    if llm_clean == gold_clean:
        return True
    # Use \b when the gold edge is a word char; otherwise require
    # whitespace or string boundary so non-word chars like "+"
    # don't mis-anchor.
    start = (
        r"\b"
        if gold_clean[0].isalnum() or gold_clean[0] == "_"
        else r"(?:^|(?<=\s))"
    )
    end = (
        r"\b"
        if gold_clean[-1].isalnum() or gold_clean[-1] == "_"
        else r"(?:$|(?=\s))"
    )
    pattern = start + re.escape(gold_clean) + end
    return re.search(pattern, llm_clean) is not None


_TRUE_VARIANTS = frozenset({"yes", "true", "1"})
_FALSE_VARIANTS = frozenset({"no", "false", "0"})


def match_boolean(llm_answer: str, gold_answer: str) -> bool:
    """Check if LLM answer matches gold boolean value.

    Recognises ``yes/true/1`` as truthy and ``no/false/0`` as falsy.
    Both inputs are stripped and lowercased before comparison.
    Returns ``False`` when gold is not a recognised boolean variant.
    """
    llm_clean = llm_answer.strip().lower()
    gold_clean = gold_answer.strip().lower()
    if gold_clean in _TRUE_VARIANTS:
        return llm_clean in _TRUE_VARIANTS
    if gold_clean in _FALSE_VARIANTS:
        return llm_clean in _FALSE_VARIANTS
    return False


def match_list(llm_answer: str, gold_answer: str) -> bool:
    """Check if LLM answer matches gold list (order-insensitive).

    Uses sorted multiset comparison so duplicate elements are
    preserved (``["a", "a"]`` does NOT match ``["a"]``).
    """
    try:
        llm_list = json.loads(llm_answer)
    except json.JSONDecodeError:
        return False
    try:
        gold_list = json.loads(gold_answer)
    except json.JSONDecodeError:
        return False
    if not isinstance(llm_list, list) or not isinstance(gold_list, list):
        return False
    llm_norm = sorted(str(x).strip().lower() for x in llm_list)
    gold_norm = sorted(str(x).strip().lower() for x in gold_list)
    return llm_norm == gold_norm


def evaluate_answer(
    llm_answer: str,
    gold_answer: str,
    *,
    answer_type: str,
    tolerance: float = 0.0,
) -> bool:
    """Evaluate a single answer against gold."""
    if answer_type == "number":
        return match_number(llm_answer, gold_answer, tolerance=tolerance)
    if answer_type == "boolean":
        return match_boolean(llm_answer, gold_answer)
    if answer_type == "list":
        return match_list(llm_answer, gold_answer)
    return match_string(llm_answer, gold_answer)


def latency_percentiles(
    latencies: list[float],
) -> dict[str, float | int]:
    """Compute p50, p90, and mean latency from a list of values.

    Returns an empty dict when *latencies* is empty.  p50 uses
    ``statistics.median`` (interpolated midpoint for even *n*);
    p90 uses nearest-rank indexing.
    """
    if not latencies:
        return {}
    s = sorted(latencies)
    n = len(s)
    p90_idx = math.ceil(n * 0.9) - 1
    return {
        "p50_ms": round(statistics.median(s), 1),
        "p90_ms": round(s[p90_idx], 1),
        "mean_ms": round(sum(s) / n, 1),
        "count": n,
    }
