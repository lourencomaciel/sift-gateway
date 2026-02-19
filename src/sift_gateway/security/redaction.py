"""Redact detected secrets from tool responses before model exposure."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import importlib
import re
from typing import Any, Protocol, cast

_MIN_SECRET_CANDIDATE_LENGTH = 8
_MIN_PLAUSIBLE_SECRET_LENGTH = 24
_HEX_ONLY_RE = re.compile(r"^[0-9a-fA-F]+$")
_QUERY_SECRET_RE = re.compile(
    r"(?i)([?&](?:access_token|api[_-]?key|client_secret|password|token)=)"
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
_PUBLIC_ID_PREFIXES = (
    "art_",
    "act_",
    "ad_",
    "adset_",
    "campaign_",
    "page_",
    "biz_",
)


class SecretRedactionError(RuntimeError):
    """Raised when response redaction cannot run in fail-closed mode."""


class _SecretScanner(Protocol):
    """Secret detector abstraction for response redaction."""

    @property
    def available(self) -> bool:
        """Whether the scanner backend is available."""

    def detect(self, text: str) -> set[str]:
        """Return detected secret values from a string payload."""


class DetectSecretsScanner:
    """Adapter over ``detect-secrets`` line scanning."""

    def __init__(self) -> None:
        """Initialize scanner backend handles when installed."""
        self._scan_line: Any | None = None
        self._default_settings: Any | None = None
        self._import_error: Exception | None = None
        try:
            scan_module = importlib.import_module("detect_secrets.core.scan")
            settings_module = importlib.import_module("detect_secrets.settings")
            self._scan_line = cast(Any, scan_module).scan_line
            self._default_settings = cast(Any, settings_module).default_settings
        except Exception as exc:  # pragma: no cover - import-path specific
            self._import_error = exc

    @property
    def available(self) -> bool:
        """Return true when ``detect-secrets`` imports successfully."""
        return (
            self._scan_line is not None and self._default_settings is not None
        )

    @property
    def import_error(self) -> Exception | None:
        """Return the import error when scanner backend is unavailable."""
        return self._import_error

    def detect(self, text: str) -> set[str]:
        """Scan text and return detected secret values."""
        if not self.available:
            return set()
        assert self._default_settings is not None
        assert self._scan_line is not None
        detected: set[str] = set()
        lines = text.splitlines() or [text]
        with self._default_settings():
            for line in lines:
                findings = self._scan_line(line)
                for finding in cast(Iterable[Any], findings):
                    secret_value = getattr(finding, "secret_value", None)
                    if isinstance(secret_value, str) and secret_value:
                        detected.add(secret_value)
        return detected


@dataclass(frozen=True)
class RedactionResult:
    """Result payload for one redaction pass."""

    payload: dict[str, Any]
    redacted_count: int


class ResponseSecretRedactor:
    """Recursively redact detected secrets from JSON-like payloads."""

    def __init__(
        self,
        *,
        enabled: bool,
        fail_closed: bool,
        max_scan_bytes: int,
        replacement: str,
        scanner: _SecretScanner | None = None,
    ) -> None:
        """Build a response redactor with scanner and policy controls."""
        self.enabled = enabled
        self.fail_closed = fail_closed
        self.max_scan_bytes = max_scan_bytes
        self.replacement = replacement
        self._scanner = scanner or DetectSecretsScanner()

    def redact_payload(self, payload: dict[str, Any]) -> RedactionResult:
        """Redact secrets from a structured tool response payload."""
        if not self.enabled:
            return RedactionResult(payload=payload, redacted_count=0)

        if not self._scanner.available:
            if self.fail_closed:
                msg = "secret redaction is enabled but detect-secrets is unavailable"
                raise SecretRedactionError(msg)
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

    def _redact_dict(
        self, values: dict[str, Any]
    ) -> tuple[dict[str, Any], int]:
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

    def _detected_secret_candidates(self, text: str) -> set[str]:
        return {
            candidate
            for candidate in self._scanner.detect(text)
            if (
                len(candidate) >= _MIN_SECRET_CANDIDATE_LENGTH
                and self._is_plausible_secret_candidate(candidate)
            )
        }

    def _replace_detected_secrets(
        self, text: str, detected: set[str]
    ) -> tuple[str, int]:
        redacted = text
        total_hits = 0
        for secret in sorted(detected, key=len, reverse=True):
            if secret in redacted:
                total_hits += 1
                redacted = redacted.replace(secret, self.replacement)
        return redacted, total_hits

    def _redact_string(self, text: str) -> tuple[str, int]:
        if not self._should_scan_string(text):
            return text, 0

        redacted, total_hits = self._redact_known_secret_patterns(text)
        detected = self._detected_secret_candidates(text)
        redacted, scanner_hits = self._replace_detected_secrets(
            redacted, detected
        )
        total_hits += scanner_hits

        if total_hits == 0 or redacted == text:
            return text, 0
        return redacted, total_hits

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
        total_hits = 0
        total_hits += query_hits + bearer_hits

        for pattern in _KNOWN_TOKEN_PATTERNS:
            redacted, token_hits = self._replace_full_token_pattern(
                text=redacted,
                pattern=pattern,
            )
            total_hits += token_hits

        return redacted, total_hits

    def _matches_known_secret_prefix(self, candidate: str) -> bool:
        lower = candidate.lower()
        if lower.startswith("sk_") and len(candidate) >= 8:
            return True
        if lower.startswith("ghp_") and len(candidate) >= 8:
            return True
        if lower.startswith("bearer "):
            token = candidate[7:].strip()
            return len(token) >= 6
        return False

    def _is_long_structural_secret_candidate(self, candidate: str) -> bool:
        if len(candidate) < _MIN_PLAUSIBLE_SECRET_LENGTH:
            return False
        if any(ch.isspace() for ch in candidate):
            return False
        if candidate.startswith(_PUBLIC_ID_PREFIXES):
            return False
        if _HEX_ONLY_RE.fullmatch(candidate):
            return False
        has_alpha = any(ch.isalpha() for ch in candidate)
        has_digit = any(ch.isdigit() for ch in candidate)
        return has_alpha and has_digit

    def _is_plausible_secret_candidate(self, candidate: str) -> bool:
        return self._matches_known_secret_prefix(
            candidate
        ) or self._is_long_structural_secret_candidate(candidate)
