from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from sift_gateway.security.redaction import (
    DetectSecretsScanner,
    RedactionResult,
    ResponseSecretRedactor,
    SecretRedactionError,
)


class _FakeScanner:
    def __init__(
        self,
        *,
        available: bool = True,
        error: Exception | None = None,
    ) -> None:
        self.available = available
        self._error = error

    def detect(self, text: str) -> set[str]:
        if self._error is not None:
            raise self._error
        findings: set[str] = set()
        for candidate in ("sk_live_123", "Bearer tok_456", "ghp_secret"):
            if candidate in text:
                findings.add(candidate)
        return findings


def _redactor(
    *,
    scanner: Any,
    enabled: bool = True,
    fail_closed: bool = False,
    max_scan_bytes: int = 32_768,
) -> ResponseSecretRedactor:
    return ResponseSecretRedactor(
        enabled=enabled,
        fail_closed=fail_closed,
        max_scan_bytes=max_scan_bytes,
        replacement="[REDACTED_SECRET]",
        scanner=scanner,
    )


def test_redact_payload_nested_values() -> None:
    scanner = _FakeScanner()
    redactor = _redactor(scanner=scanner)
    payload = {
        "top": "token=sk_live_123",
        "nested": {
            "auth": "Authorization: Bearer tok_456",
            "list": ["ok", "ghp_secret"],
        },
        "untouched_key_sk_live_123": "ok",
    }

    result = redactor.redact_payload(payload)

    assert isinstance(result, RedactionResult)
    assert result.redacted_count == 3
    assert result.payload["top"] == "token=[REDACTED_SECRET]"
    assert result.payload["nested"]["auth"] == (
        "Authorization: Bearer [REDACTED_SECRET]"
    )
    assert result.payload["nested"]["list"][1] == "[REDACTED_SECRET]"
    assert result.payload["untouched_key_sk_live_123"] == "ok"


def test_redact_payload_disabled_returns_original() -> None:
    scanner = _FakeScanner()
    redactor = _redactor(scanner=scanner, enabled=False)
    payload = {"value": "sk_live_123"}

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_scanner_unavailable_fail_open() -> None:
    scanner = _FakeScanner(available=False)
    redactor = _redactor(scanner=scanner, fail_closed=False)
    payload = {"value": "sk_live_123"}

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_scanner_unavailable_fail_closed() -> None:
    scanner = _FakeScanner(available=False)
    redactor = _redactor(scanner=scanner, fail_closed=True)

    with pytest.raises(SecretRedactionError):
        redactor.redact_payload({"value": "sk_live_123"})


def test_redact_payload_scanner_error_fail_open() -> None:
    scanner = _FakeScanner(error=RuntimeError("boom"))
    redactor = _redactor(scanner=scanner, fail_closed=False)
    payload = {"value": "sk_live_123"}

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_scanner_error_fail_closed() -> None:
    scanner = _FakeScanner(error=RuntimeError("boom"))
    redactor = _redactor(scanner=scanner, fail_closed=True)

    with pytest.raises(SecretRedactionError):
        redactor.redact_payload({"value": "sk_live_123"})


def test_redact_payload_skips_over_limit_string() -> None:
    scanner = _FakeScanner()
    redactor = _redactor(scanner=scanner, max_scan_bytes=8)
    payload = {"value": "sk_live_123"}

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_ignores_short_secret_candidates() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"ac", "da", "bef"}

    redactor = _redactor(scanner=_Scanner())
    payload = {"value": "account data before"}

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_redacts_long_candidates_only() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"ac", "da", "sk_live_123"}

    redactor = _redactor(scanner=_Scanner())
    payload = {"value": "account data token=sk_live_123"}

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert result.payload["value"] == "account data token=[REDACTED_SECRET]"


def test_redact_payload_skips_non_matching_candidates() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"this_secret_value_is_not_present"}

    redactor = _redactor(scanner=_Scanner())
    payload = {"value": "account data token=sk_live_123"}

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_redacts_access_token_query_param() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return set()

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "next": (
            "https://graph.facebook.com/v23.0/act_1/insights?"
            "fields=account_id&access_token=EAASlfHJq1gcBQq6VZAMRHvQ"
            "&after=NTg4NDEwNTE5NTUz"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert "fields=account_id" in result.payload["next"]
    assert "after=NTg4NDEwNTE5NTUz" in result.payload["next"]
    assert "access_token=[REDACTED_SECRET]" in result.payload["next"]
    assert "EAASlfHJq1gcBQq6VZAMRHvQ" not in result.payload["next"]


def test_redact_payload_ignores_hex_digests_and_public_ids() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {
                "abcd1234abcd1234abcd1234",
                "art_fb55ded7de7864c126ee92f0ff686b03",
            }

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "hash": "sha256:abcd1234abcd1234abcd1234",
        "artifact_id": "art_fb55ded7de7864c126ee92f0ff686b03",
    }

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_detect_secrets_scanner_calls_scan_line_with_line_only() -> None:
    class _Finding:
        def __init__(self, secret_value: str) -> None:
            self.secret_value = secret_value

    scanned_lines: list[str] = []

    @contextmanager
    def _fake_default_settings():
        yield object()

    def _fake_scan_line(line: str) -> list[_Finding]:
        scanned_lines.append(line)
        if "sk_live_123" in line:
            return [_Finding("sk_live_123")]
        return []

    scanner = DetectSecretsScanner()
    scanner._default_settings = _fake_default_settings
    scanner._scan_line = _fake_scan_line

    detected = scanner.detect("hello\nsk_live_123")

    assert scanned_lines == ["hello", "sk_live_123"]
    assert detected == {"sk_live_123"}
