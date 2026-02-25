"""Baseline (context-stuffed) helpers shared by all benchmark tiers."""

from __future__ import annotations

import json
from typing import Any

MAX_BASELINE_BYTES_DEFAULT = 400_000

# Conservative estimate: structured JSON tokenizes poorly -- short
# keys, numbers, and punctuation each become separate tokens.
# Empirically, GeoJSON and similar structured data tokenize at
# ~2 bytes/token; plain-text JSON is closer to 3.  Using 2 here
# ensures the token-derived cap actually constrains the payload
# (at 3, the byte cap always wins and the token limit is dead
# code).  The token limit leaves ~20K tokens headroom for the
# system prompt + question text.
MAX_BASELINE_TOKENS_DEFAULT = 180_000
_BYTES_PER_TOKEN_JSON = 2

BASELINE_SYSTEM = (
    "You are a data analyst. Answer the question about the JSON data "
    "provided. Give ONLY the final answer value — no explanation, "
    "no units, no surrounding text. For numbers, give the numeric "
    "value. For strings, give the exact value."
)


def _effective_max_bytes(
    max_bytes: int,
    max_tokens: int,
) -> int:
    """Return the smaller of the byte cap and the token-derived cap.

    JSON tokenizes poorly (~3 bytes/token for structured data),
    so a byte cap alone often exceeds the model's context window.
    """
    token_derived = max_tokens * _BYTES_PER_TOKEN_JSON
    return min(max_bytes, token_derived)


def _fits(candidate: str, limit: int) -> bool:
    """Check if a JSON string fits within the byte limit."""
    # String length is a lower bound on UTF-8 byte length,
    # so skip the encode when the string alone exceeds cap.
    if len(candidate) > limit:
        return False
    return len(candidate.encode("utf-8")) <= limit


def _truncate_list(data: list[Any], limit: int) -> str:
    """Binary search for the largest array prefix that fits."""
    best_json = json.dumps(data[:1], ensure_ascii=False)
    if not _fits(best_json, limit):
        # Even a single item exceeds the limit -- return it anyway
        # so callers always get valid JSON (best-effort).
        return best_json
    low, high = 1, len(data)
    while low <= high:
        mid = (low + high) // 2
        candidate = json.dumps(data[:mid], ensure_ascii=False)
        if not _fits(candidate, limit):
            high = mid - 1
        else:
            best_json = candidate
            low = mid + 1
    return best_json


def _truncate_dict(data: dict[str, Any], limit: int) -> str | None:
    """Shrink array values inside a dict to fit within *limit* bytes.

    Finds lists at the top level and one level deep, then binary-
    searches on a keep-fraction applied uniformly to all of them.
    Returns ``None`` if the dict cannot be shrunk to fit (e.g. no
    array values to trim).
    """
    # Collect (key_path, original_length) for all arrays.
    arrays: list[tuple[tuple[str, ...], int]] = []
    for key, val in data.items():
        if isinstance(val, list) and len(val) > 1:
            arrays.append(((key,), len(val)))
        elif isinstance(val, dict):
            for subkey, subval in val.items():
                if isinstance(subval, list) and len(subval) > 1:
                    arrays.append(((key, subkey), len(subval)))

    if not arrays:
        return None

    # Binary search on the fraction of array elements to keep.
    # Instead of deepcopy, build a shallow trial dict per iteration:
    # non-array values are shared (safe -- we only serialize to JSON,
    # never mutate), and arrays are sliced to the desired length.
    low_f, high_f = 0.0, 1.0
    best_json: str | None = None
    for _ in range(30):
        mid_f = (low_f + high_f) / 2
        # Compute keep counts for this fraction.
        keeps: dict[tuple[str, ...], int] = {
            kp: max(1, int(orig_len * mid_f)) for kp, orig_len in arrays
        }
        # Build a shallow trial dict with sliced arrays.
        trial: dict[str, Any] = {}
        for key, val in data.items():
            if (key,) in keeps:
                trial[key] = val[: keeps[(key,)]]
            elif isinstance(val, dict) and any(
                kp[0] == key and len(kp) == 2 for kp in keeps
            ):
                sub: dict[str, Any] = {}
                for subkey, subval in val.items():
                    kp = (key, subkey)
                    if kp in keeps:
                        sub[subkey] = subval[: keeps[kp]]
                    else:
                        sub[subkey] = subval
                trial[key] = sub
            else:
                trial[key] = val

        candidate = json.dumps(trial, ensure_ascii=False)
        if _fits(candidate, limit):
            best_json = candidate
            low_f = mid_f
        else:
            high_f = mid_f

    return best_json


def truncate_for_baseline(
    data: Any,
    *,
    max_bytes: int,
    max_tokens: int,
) -> tuple[str, bool]:
    """Serialize data for baseline, truncating if too large.

    Uses both a byte cap and a token-estimate cap to avoid
    exceeding the model's context window.
    """
    limit = _effective_max_bytes(max_bytes, max_tokens)
    full_json = json.dumps(data, ensure_ascii=False)
    if _fits(full_json, limit):
        return full_json, False

    if isinstance(data, list):
        return _truncate_list(data, limit), True

    if isinstance(data, dict):
        shrunk = _truncate_dict(data, limit)
        if shrunk is not None:
            return shrunk, True

    note = {
        "_truncated": True,
        "_note": "payload too large for baseline",
    }
    return json.dumps(note, ensure_ascii=False), True
