"""Thin LLM API client using urllib only (no third-party deps)."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import time
from typing import Any
import urllib.error
import urllib.request


@dataclass(frozen=True)
class LLMResponse:
    """Response from an LLM API call."""

    text: str
    input_tokens: int
    output_tokens: int
    model: str
    latency_ms: float


def _detect_provider(model: str) -> str:
    if model.startswith("claude"):
        return "anthropic"
    return "openai"


def _resolve_api_key(
    *,
    provider: str,
    api_key: str | None,
) -> str:
    if api_key:
        return api_key
    if provider == "anthropic":
        key = os.environ.get("ANTHROPIC_API_KEY", "")
    else:
        key = os.environ.get("OPENAI_API_KEY", "")
    if not key:
        msg = (
            f"No API key for {provider}. "
            f"Pass --api-key or set "
            f"{'ANTHROPIC_API_KEY' if provider == 'anthropic' else 'OPENAI_API_KEY'}"
        )
        raise ValueError(msg)
    return key


def _call_anthropic(
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    user_message: str,
    temperature: float,
    max_tokens: int,
) -> LLMResponse:
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}],
    }
    request = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        method="POST",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
    )
    start = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Anthropic API error ({exc.code}): {error_body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Anthropic API request failed: {exc}") from exc
    latency = (time.monotonic() - start) * 1000.0

    text = ""
    content = body.get("content", [])
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            text += block.get("text", "")

    usage = body.get("usage", {})
    return LLMResponse(
        text=text.strip(),
        input_tokens=usage.get("input_tokens", 0),
        output_tokens=usage.get("output_tokens", 0),
        model=body.get("model", model),
        latency_ms=latency,
    )


def _call_openai(
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    user_message: str,
    temperature: float,
    max_tokens: int,
) -> LLMResponse:
    payload: dict[str, Any] = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
    }
    request = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        method="POST",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    start = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"OpenAI API error ({exc.code}): {error_body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"OpenAI API request failed: {exc}") from exc
    latency = (time.monotonic() - start) * 1000.0

    text = ""
    choices = body.get("choices", [])
    if choices:
        message = choices[0].get("message", {})
        text = message.get("content", "")

    usage = body.get("usage", {})
    return LLMResponse(
        text=text.strip(),
        input_tokens=usage.get("prompt_tokens", 0),
        output_tokens=usage.get("completion_tokens", 0),
        model=body.get("model", model),
        latency_ms=latency,
    )


def call_llm(
    *,
    model: str,
    system_prompt: str,
    user_message: str,
    api_key: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 4096,
) -> LLMResponse:
    """Send a prompt to an LLM and return the response."""
    provider = _detect_provider(model)
    resolved_key = _resolve_api_key(
        provider=provider,
        api_key=api_key,
    )
    if provider == "anthropic":
        return _call_anthropic(
            api_key=resolved_key,
            model=model,
            system_prompt=system_prompt,
            user_message=user_message,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    return _call_openai(
        api_key=resolved_key,
        model=model,
        system_prompt=system_prompt,
        user_message=user_message,
        temperature=temperature,
        max_tokens=max_tokens,
    )
