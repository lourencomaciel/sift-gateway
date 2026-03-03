from __future__ import annotations

import json

import pytest

from sift_gateway.security.redaction import (
    RedactionResult,
    ResponseSecretRedactor,
    SecretRedactionError,
)


def _redactor(
    *,
    enabled: bool = True,
    fail_closed: bool = False,
    max_scan_bytes: int = 32_768,
) -> ResponseSecretRedactor:
    return ResponseSecretRedactor(
        enabled=enabled,
        fail_closed=fail_closed,
        max_scan_bytes=max_scan_bytes,
        replacement="[REDACTED_SECRET]",
    )


def test_redact_payload_nested_values() -> None:
    redactor = _redactor()
    payload = {
        "top": (
            "https://api.example.test/items"
            "?access_token=EAASlfHJq1gcBQq6VZAMRHvQ"
        ),
        "nested": {
            "auth": "Authorization: Bearer tok_456",
            "list": [
                "ok",
                "ghp_abcdefghijklmnopqrstuvwxyz123456",
                "sk_live_ABCDEFGH12345678",
            ],
        },
        "untouched_key_sk_live_123": "ok",
    }

    result = redactor.redact_payload(payload)

    assert isinstance(result, RedactionResult)
    assert result.redacted_count == 4
    assert result.payload["top"] == (
        "https://api.example.test/items"
        "?access_token=[REDACTED_SECRET]"
    )
    assert result.payload["nested"]["auth"] == (
        "Authorization: Bearer [REDACTED_SECRET]"
    )
    assert result.payload["nested"]["list"][1] == "[REDACTED_SECRET]"
    assert result.payload["nested"]["list"][2] == "[REDACTED_SECRET]"
    assert result.payload["untouched_key_sk_live_123"] == "ok"


def test_redact_payload_disabled_returns_original() -> None:
    redactor = _redactor(enabled=False)
    payload = {"value": "sk_live_ABCDEFGH12345678"}

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_skips_over_limit_string() -> None:
    redactor = _redactor(max_scan_bytes=8)
    payload = {"value": "sk_live_ABCDEFGH12345678"}

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_does_not_redact_unknown_high_entropy_values() -> None:
    redactor = _redactor()
    payload = {
        "download_link": (
            "https://api.example.test/download"
            "?sig=customsig_ABCdef1234567890xyz987"
        ),
        "resource_id": "Abcdef1234567890GhijklmnopqrstUVWX",
    }

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_redacts_access_token_query_param() -> None:
    redactor = _redactor()
    payload = {
        "next": (
            "https://graph.facebook.com/v23.0/act_1/insights"
            "?fields=account_id&access_token=EAASlfHJq1gcBQq6VZAMRHvQ"
            "&after=NTg4NDEwNTE5NTUz"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert "fields=account_id" in result.payload["next"]
    assert "after=NTg4NDEwNTE5NTUz" in result.payload["next"]
    assert "access_token=[REDACTED_SECRET]" in result.payload["next"]


def test_redact_payload_does_not_redact_generic_token_query_param() -> None:
    redactor = _redactor()
    payload = {
        "next": (
            "https://api.example.test/items"
            "?token=PAGE_TOKEN_ABC123&limit=100"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


@pytest.mark.parametrize(
    "raw_value",
    [
        "sk_test_ABCDEFGH12345678",
        "ghp_abcdefghijklmnopqrstuvwxyz123456",
        "github_pat_abcdefghijklmnopqrstuvwxyz_1234567890",
        "xoxb-1234567890-0987654321",
        "EAASlfHJq1gcBQq6VZAMRHvQ",
    ],
)
def test_redact_payload_redacts_known_token_patterns(raw_value: str) -> None:
    redactor = _redactor()

    result = redactor.redact_payload({"value": raw_value})

    assert result.redacted_count == 1
    assert result.payload["value"] == "[REDACTED_SECRET]"


def test_redact_payload_redacts_bearer_header() -> None:
    redactor = _redactor()
    payload = {
        "header": "Authorization: Bearer abcdefghijklmnop",
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert result.payload["header"] == "Authorization: Bearer [REDACTED_SECRET]"


def test_redact_payload_embedded_json_string_redacts_known_query_tokens() -> None:
    redactor = _redactor()
    payload = {
        "result": json.dumps(
            {
                "next": (
                    "https://graph.facebook.com/v23.0/act_1/insights"
                    "?fields=account_id&access_token=EAASlfHJq1gcBQq6VZAMRHvQ"
                    "&after=NTg4NDEwNTE5NTUz"
                )
            }
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    decoded = json.loads(result.payload["result"])
    assert decoded["next"] == (
        "https://graph.facebook.com/v23.0/act_1/insights"
        "?fields=account_id&access_token=[REDACTED_SECRET]"
        "&after=NTg4NDEwNTE5NTUz"
    )


def test_redact_payload_invalid_embedded_json_falls_back_to_plain_string() -> None:
    redactor = _redactor()
    payload = {
        "result": "{not_valid_json",
    }

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_preserves_binary_ref_blob_uri_and_id() -> None:
    redactor = _redactor()
    payload = {
        "type": "binary_ref",
        "blob_id": "bin_42b7d9ae99ee75a488d474c30fb0a61c",
        "binary_hash": (
            "42b7d9ae99ee75a488d474c30fb0a61c"
            "3b224d89ccb42026f051c64083cfe36f"
        ),
        "mime": "image/jpeg",
        "byte_count": 225585,
        "uri": "sift://blob/bin_42b7d9ae99ee75a488d474c30fb0a61c",
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload is payload


def test_redact_payload_error_fail_open() -> None:
    redactor = _redactor(fail_closed=False)

    def _raise(_text: str) -> tuple[str, int]:
        raise RuntimeError("boom")

    redactor._redact_known_secret_patterns = _raise  # type: ignore[method-assign]
    payload = {"value": "sk_live_ABCDEFGH12345678"}

    result = redactor.redact_payload(payload)

    assert result.payload is payload
    assert result.redacted_count == 0


def test_redact_payload_error_fail_closed() -> None:
    redactor = _redactor(fail_closed=True)

    def _raise(_text: str) -> tuple[str, int]:
        raise RuntimeError("boom")

    redactor._redact_known_secret_patterns = _raise  # type: ignore[method-assign]

    with pytest.raises(SecretRedactionError):
        redactor.redact_payload({"value": "sk_live_ABCDEFGH12345678"})
