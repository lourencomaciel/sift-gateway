from __future__ import annotations

from contextlib import contextmanager
import json
from typing import Any

import pytest

from sift_gateway.security.redaction import (
    DetectSecretsScanner,
    RedactionResult,
    ResponseSecretRedactor,
    SecretRedactionError,
)

_SIGNED_FBCDN_IMAGE_URL = (
    "https://scontent.xx.fbcdn.net/v/t39.30808-6/"
    "480888509_1647065635966054_3551861822251931659_n.jpg"
    "?stp=dst-jpg_s526x395_tt6"
    "&_nc_cat=110"
    "&ccb=1-7"
    "&_nc_sid=127cfc"
    "&_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz"
    "&_nc_zt=23"
    "&_nc_ht=scontent.xx"
    "&oh=00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A"
    "&oe=67E7C2A3"
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


def test_redact_payload_image_blob_data_skips_scanner() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"A1B2C3D4E5F6G7H8I9J0K1L2M3N4P5Q6"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "type": "image",
        "mimeType": "image/jpeg",
        "data": (
            "base64prefix_A1B2C3D4E5F6G7H8I9J0K1L2M3N4P5Q6_base64suffix"
        ),
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["data"] == payload["data"]


def test_redact_payload_non_image_blob_data_still_scans() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"A1B2C3D4E5F6G7H8I9J0K1L2M3N4P5Q6"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "type": "text",
        "mimeType": "text/plain",
        "data": (
            "blobprefix_A1B2C3D4E5F6G7H8I9J0K1L2M3N4P5Q6_blobsuffix"
        ),
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert result.payload["data"] == "blobprefix_[REDACTED_SECRET]_blobsuffix"


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


def test_redact_payload_skips_scanner_for_image_url_fields() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {
                "_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz",
                "oh=00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A",
            }

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "image_url": _SIGNED_FBCDN_IMAGE_URL,
        "thumbnail_url": _SIGNED_FBCDN_IMAGE_URL,
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["image_url"] == _SIGNED_FBCDN_IMAGE_URL
    assert result.payload["thumbnail_url"] == _SIGNED_FBCDN_IMAGE_URL


@pytest.mark.parametrize(
    "field_name",
    [
        "image_url",
        "thumbnail_url",
        "file_url",
        "video_url",
        "Image_URL",
        "PROFILE_IMAGE_URL",
    ],
)
def test_redact_payload_scanner_bypass_field_name_is_case_insensitive(
    field_name: str,
) -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz"}

    redactor = _redactor(scanner=_Scanner())
    payload = {field_name: _SIGNED_FBCDN_IMAGE_URL}

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload[field_name] == _SIGNED_FBCDN_IMAGE_URL


def test_redact_payload_database_url_field_still_scans_non_http_urls() -> None:
    secret = "Abcdef1234567890Ghijklmnopqrst"

    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {secret}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "database_url": f"postgres://user:{secret}@db.example.test/prod"
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert (
        result.payload["database_url"]
        == "postgres://user:[REDACTED_SECRET]@db.example.test/prod"
    )


def test_redact_payload_scanner_bypass_propagates_into_relaxed_url_lists() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "thumbnail_url": [_SIGNED_FBCDN_IMAGE_URL],
        "images": [{"image_url": _SIGNED_FBCDN_IMAGE_URL}],
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["thumbnail_url"][0] == _SIGNED_FBCDN_IMAGE_URL
    assert result.payload["images"][0]["image_url"] == _SIGNED_FBCDN_IMAGE_URL


def test_redact_payload_image_url_still_redacts_known_query_tokens() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return set()

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "image_url": (
            "https://cdn.example.test/photo.jpg"
            "?access_token=EAASlfHJq1gcBQq6VZAMRHvQ&size=large"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert (
        result.payload["image_url"]
        == "https://cdn.example.test/photo.jpg"
        "?access_token=[REDACTED_SECRET]&size=large"
    )


def test_redact_payload_thumbnail_url_still_redacts_known_token_patterns() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return set()

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "thumbnail_url": (
            "https://cdn.example.test/photo.jpg"
            "?sig=ghp_abcdefghijklmnopqrstuvwxyz123456"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert (
        result.payload["thumbnail_url"]
        == "https://cdn.example.test/photo.jpg?sig=[REDACTED_SECRET]"
    )


def test_redact_payload_skips_scanner_for_plain_url_field_with_file_extension() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz"}

    redactor = _redactor(scanner=_Scanner())
    payload = {"url": _SIGNED_FBCDN_IMAGE_URL}

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["url"] == _SIGNED_FBCDN_IMAGE_URL


def test_redact_payload_file_url_skips_scanner_query_candidates() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"customsig_ABCdef1234567890xyz987"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "file_url": (
            "https://cdn.example.test/archive.zip"
            "?sig=customsig_ABCdef1234567890xyz987"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert (
        result.payload["file_url"]
        == "https://cdn.example.test/archive.zip"
        "?sig=customsig_ABCdef1234567890xyz987"
    )


def test_redact_payload_url_suffix_field_without_extension_skips_scanner_when_signed() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "image_url": (
            "https://scontent.xx.fbcdn.net/v/t45"
            "?oh=00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A"
            "&_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["image_url"] == payload["image_url"]


def test_redact_payload_plain_url_field_without_extension_skips_scanner_for_fbcdn() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "url": (
            "https://scontent.xx.fbcdn.net/v/t45"
            "?oh=00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A"
            "&_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["url"] == payload["url"]


def test_redact_payload_plain_url_field_without_extension_still_scans_non_media_host() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"customsig_ABCdef1234567890xyz987"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "url": (
            "https://api.example.test/download"
            "?sig=customsig_ABCdef1234567890xyz987"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert result.payload["url"] == (
        "https://api.example.test/download?sig=[REDACTED_SECRET]"
    )


def test_redact_payload_fbcdn_url_unknown_field_skips_scanner_even_without_signed_query() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"Abcdef1234567890Ghijklmnopqrst"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "download_link": (
            "https://scontent.xx.fbcdn.net/v/t45.1600x1600/"
            "Abcdef1234567890Ghijklmnopqrst.png"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["download_link"] == payload["download_link"]


def test_redact_payload_fbcdn_url_still_redacts_known_query_tokens() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return set()

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "download_link": (
            "https://scontent.xx.fbcdn.net/v/t45.1600x1600/photo.png"
            "?access_token=EAASlfHJq1gcBQq6VZAMRHvQ"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert result.payload["download_link"] == (
        "https://scontent.xx.fbcdn.net/v/t45.1600x1600/photo.png"
        "?access_token=[REDACTED_SECRET]"
    )


def test_redact_payload_png_image_url_skips_scanner_when_signed() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {
                "00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A",
                "_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz",
            }

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "image_url": (
            "https://scontent.fcgh16-1.fna.fbcdn.net/v/t45.1600x1600.png"
            "?stp=dst-jpg_tt6&_nc_cat=102&ccb=1-7&_nc_sid=d5bd00"
            "&_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz&_nc_zt=1"
            "&_nc_ht=scontent.fcgh16-1.fna&edm=AEuWsiQEAAAA"
            "&oh=00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A"
            "&oe=69AC2CF3"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["image_url"] == payload["image_url"]


def test_redact_payload_embedded_json_string_preserves_fbcdn_image_url() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "result": json.dumps(
            {
                "data": [
                    {
                        "image_url": _SIGNED_FBCDN_IMAGE_URL,
                    }
                ]
            }
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    decoded = json.loads(result.payload["result"])
    assert decoded["data"][0]["image_url"] == _SIGNED_FBCDN_IMAGE_URL


def test_redact_payload_embedded_json_string_redacts_known_query_tokens() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return set()

    redactor = _redactor(scanner=_Scanner())
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


def test_redact_payload_signed_redirect_url_skips_scanner_without_extension() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"customsig_ABCdef1234567890xyz987"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "thumb": (
            "https://external.xx.fbcdn.net/safe_image.php"
            "?url=https%3A%2F%2Fwww.facebook.com%2Fads%2Fimage%2F%3Fd%3Dcustomsig_ABCdef1234567890xyz987"
            "&_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["thumb"] == payload["thumb"]


def test_redact_payload_unknown_field_skips_scanner_when_value_looks_like_file_url() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"customsig_ABCdef1234567890xyz987"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "download_link": (
            "https://cdn.example.test/manual.pdf"
            "?sig=customsig_ABCdef1234567890xyz987"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["download_link"] == payload["download_link"]


def test_redact_payload_unknown_field_with_json_extension_still_scans() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"customsig_ABCdef1234567890xyz987"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "download_link": (
            "https://api.example.test/export.json"
            "?sig=customsig_ABCdef1234567890xyz987"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert result.payload["download_link"] == (
        "https://api.example.test/export.json?sig=[REDACTED_SECRET]"
    )


def test_redact_payload_unknown_field_without_file_extension_still_scans() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"customsig_ABCdef1234567890xyz987"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "download_link": (
            "https://cdn.example.test/download"
            "?sig=customsig_ABCdef1234567890xyz987"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert result.payload["download_link"] == (
        "https://cdn.example.test/download?sig=[REDACTED_SECRET]"
    )


def test_redact_payload_extension_trigger_still_redacts_known_tokens() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"customsig_ABCdef1234567890xyz987"}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "download_link": (
            "https://cdn.example.test/manual.pdf"
            "?access_token=EAASlfHJq1gcBQq6VZAMRHvQ"
            "&sig=customsig_ABCdef1234567890xyz987"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 1
    assert result.payload["download_link"] == (
        "https://cdn.example.test/manual.pdf"
        "?access_token=[REDACTED_SECRET]"
        "&sig=customsig_ABCdef1234567890xyz987"
    )


def test_redact_payload_scanner_bypass_applies_to_any_url_suffix_field() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz"}

    redactor = _redactor(scanner=_Scanner())
    payload = {"profile_image_url": _SIGNED_FBCDN_IMAGE_URL}

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["profile_image_url"] == _SIGNED_FBCDN_IMAGE_URL


def test_redact_payload_image_url_skips_scanner_raw_value_candidates() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {"00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A"}

    redactor = _redactor(scanner=_Scanner())
    payload = {"image_url": _SIGNED_FBCDN_IMAGE_URL}

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["image_url"] == _SIGNED_FBCDN_IMAGE_URL


def test_redact_payload_image_url_relaxed_mode_is_host_agnostic() -> None:
    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {
                "_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz",
                "oh=00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A",
            }

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "image_url": (
            "https://cdn.example.test/photo.jpg"
            "?_nc_ohc=q4GQ1f7xJ7QQ7kNvgEMn-mz"
            "&oh=00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["image_url"] == payload["image_url"]


def test_redact_payload_image_url_skips_scanner_raw_values_outside_query() -> None:
    shared_value = "Abcdef1234567890Ghijklmnopqrst"

    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {shared_value}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "image_url": (
            f"https://cdn.example.test/{shared_value}/photo.jpg"
            f"?sig={shared_value}"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["image_url"] == payload["image_url"]


def test_redact_payload_image_url_skips_scanner_for_path_and_query_values() -> None:
    long_sig = "Abcdef1234567890Ghijklmnopqrst" * 8
    query_value = "00_AYCG6k0omNJ7J2b7mE06Yev8QaN1V1zGgH93yqR5qqqF6A"

    class _Scanner:
        available = True

        def detect(self, _text: str) -> set[str]:
            return {long_sig, query_value}

    redactor = _redactor(scanner=_Scanner())
    payload = {
        "image_url": (
            f"https://cdn.example.test/{long_sig}/photo.jpg"
            f"?sig={long_sig}&any={query_value}"
        )
    }

    result = redactor.redact_payload(payload)

    assert result.redacted_count == 0
    assert result.payload["image_url"] == payload["image_url"]


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
