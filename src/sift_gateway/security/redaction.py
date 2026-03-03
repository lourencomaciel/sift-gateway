"""Redact known secret patterns from tool responses before model exposure."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, cast

_QUERY_SECRET_RE = re.compile(
    r"(?i)([?&](?:access_token|api[_-]?key|client_secret|password)=)"
    r"([^&#\"\s]+)"
)
_BEARER_SECRET_RE = re.compile(r"(?i)(\bBearer\s+)([A-Za-z0-9._~+/=-]{6,})")
_KNOWN_TOKEN_PATTERNS = (
    re.compile(r"\bsk_(?:live|test)_[A-Za-z0-9]{8,}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    re.compile(r"\bEA[A-Za-z0-9]{20,}\b"),
)


class SecretRedactionError(RuntimeError):
    """Raised when response redaction cannot run in fail-closed mode."""


@dataclass(frozen=True)
class RedactionResult:
    """Result payload for one redaction pass."""

    payload: dict[str, Any]
    redacted_count: int


class ResponseSecretRedactor:
    """Recursively redact known secret patterns from JSON-like payloads."""

    def __init__(
        self,
        *,
        enabled: bool,
        fail_closed: bool,
        max_scan_bytes: int,
        replacement: str,
    ) -> None:
        """Build a response redactor with policy controls."""
        self.enabled = enabled
        self.fail_closed = fail_closed
        self.max_scan_bytes = max_scan_bytes
        self.replacement = replacement

    def redact_payload(self, payload: dict[str, Any]) -> RedactionResult:
        """Redact secrets from a structured tool response payload."""
        if not self.enabled:
            return RedactionResult(payload=payload, redacted_count=0)

        try:
            value, redacted_count = self._redact_value(payload)
        except Exception as exc:
            if self.fail_closed:
                msg = "secret redaction failed while scanning response payload"
                raise SecretRedactionError(msg) from exc
            return RedactionResult(payload=payload, redacted_count=0)

        return RedactionResult(
            payload=cast(dict[str, Any], value),
            redacted_count=redacted_count,
        )

    def _redact_value(self, value: Any) -> tuple[Any, int]:
        if isinstance(value, str):
            return self._redact_string(value)
        if isinstance(value, list):
            return self._redact_list(value)
        if isinstance(value, dict):
            return self._redact_dict(value)
        return value, 0

    def _redact_list(self, values: list[Any]) -> tuple[list[Any], int]:
        total = 0
        changed = False
        updated_items: list[Any] = []
        for item in values:
            updated_item, item_redactions = self._redact_value(item)
            total += item_redactions
            changed = changed or updated_item is not item
            updated_items.append(updated_item)
        return (updated_items if changed else values), total

    def _redact_dict(self, values: dict[str, Any]) -> tuple[dict[str, Any], int]:
        total = 0
        changed = False
        updated_map: dict[str, Any] = {}
        for key, current in values.items():
            updated_current, current_redactions = self._redact_value(current)
            total += current_redactions
            changed = changed or updated_current is not current
            updated_map[key] = updated_current
        return (updated_map if changed else values), total

    def _should_scan_string(self, text: str) -> bool:
        raw = text.encode("utf-8", errors="replace")
        return len(raw) <= self.max_scan_bytes

    def _redact_string(self, text: str) -> tuple[str, int]:
        json_result = self._redact_embedded_json_string(text)
        if json_result is not None:
            return json_result

        if not self._should_scan_string(text):
            return text, 0

        redacted, total_hits = self._redact_known_secret_patterns(text)
        if total_hits == 0 or redacted == text:
            return text, 0
        return redacted, total_hits

    def _redact_embedded_json_string(
        self,
        text: str,
    ) -> tuple[str, int] | None:
        stripped = text.strip()
        if len(stripped) < 2:
            return None
        if stripped[0] not in {"{", "["}:
            return None
        if stripped[-1] not in {"}", "]"}:
            return None

        try:
            parsed = json.loads(stripped)
        except Exception:
            return None

        if not isinstance(parsed, (dict, list)):
            return None

        redacted_value, redacted_count = self._redact_value(parsed)
        if redacted_count == 0:
            return text, 0

        serialized = json.dumps(
            redacted_value,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        if stripped == text:
            return serialized, redacted_count

        leading = len(text) - len(text.lstrip())
        trailing = len(text) - len(text.rstrip())
        prefix = text[:leading]
        suffix = text[len(text) - trailing :] if trailing else ""
        return f"{prefix}{serialized}{suffix}", redacted_count

    def _replace_group_two_pattern(
        self,
        *,
        text: str,
        pattern: re.Pattern[str],
    ) -> tuple[str, int]:
        hits = 0

        def _replace_group_two(match: re.Match[str]) -> str:
            nonlocal hits
            prefix = match.group(1)
            value = match.group(2)
            if value == self.replacement:
                return match.group(0)
            hits += 1
            return f"{prefix}{self.replacement}"

        return pattern.sub(_replace_group_two, text), hits

    def _replace_full_token_pattern(
        self,
        *,
        text: str,
        pattern: re.Pattern[str],
    ) -> tuple[str, int]:
        hits = 0

        def _replace_token(match: re.Match[str]) -> str:
            nonlocal hits
            token = match.group(0)
            if token == self.replacement:
                return token
            hits += 1
            return self.replacement

        return pattern.sub(_replace_token, text), hits

    def _redact_known_secret_patterns(self, text: str) -> tuple[str, int]:
        redacted, query_hits = self._replace_group_two_pattern(
            text=text,
            pattern=_QUERY_SECRET_RE,
        )
        redacted, bearer_hits = self._replace_group_two_pattern(
            text=redacted,
            pattern=_BEARER_SECRET_RE,
        )
        total_hits = query_hits + bearer_hits

        for pattern in _KNOWN_TOKEN_PATTERNS:
            redacted, token_hits = self._replace_full_token_pattern(
                text=redacted,
                pattern=pattern,
            )
            total_hits += token_hits

        return redacted, total_hits
