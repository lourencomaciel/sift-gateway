"""Google Application Default Credentials helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import threading
from typing import Any

from sift_gateway.auth.config import google_adc_scopes

_GOOGLE_ADC_CREDENTIALS_LOCK = threading.Lock()
_GOOGLE_ADC_CREDENTIALS: dict[tuple[str, ...], Any] = {}


def google_adc_access_token_sync(
    *, scopes: tuple[str, ...] | None = None
) -> str:
    """Return a Google ADC access token from google-auth discovery."""
    try:
        import google.auth
        from google.auth.exceptions import DefaultCredentialsError, RefreshError
        from google.auth.transport.requests import Request
    except ImportError as exc:
        msg = (
            "Google ADC auth requires the `google-auth` package. "
            "Install the gateway with Google auth support and retry."
        )
        raise RuntimeError(msg) from exc

    normalized_scopes = scopes or google_adc_scopes(None)

    try:
        with _GOOGLE_ADC_CREDENTIALS_LOCK:
            credentials = _GOOGLE_ADC_CREDENTIALS.get(normalized_scopes)
            if credentials is None:
                credentials, _project_id = google.auth.default(
                    scopes=list(normalized_scopes)
                )
                _GOOGLE_ADC_CREDENTIALS[normalized_scopes] = credentials

            token = getattr(credentials, "token", None)
            token_text = token.strip() if isinstance(token, str) else ""
            if not token_text or not bool(getattr(credentials, "valid", False)):
                credentials.refresh(Request())
                token = getattr(credentials, "token", None)
                token_text = token.strip() if isinstance(token, str) else ""
    except DefaultCredentialsError as exc:
        msg = (
            "Google ADC credentials were not found. Set "
            "`GOOGLE_APPLICATION_CREDENTIALS`, use workload identity, or run "
            "`gcloud auth application-default login` and retry."
        )
        raise RuntimeError(msg) from exc
    except RefreshError as exc:
        _GOOGLE_ADC_CREDENTIALS.pop(normalized_scopes, None)
        msg = f"Google ADC token refresh failed: {exc}"
        raise RuntimeError(msg) from exc

    if not token_text:
        msg = "Google ADC credential refresh returned an empty access token."
        raise RuntimeError(msg)
    return token_text


def google_adc_authorized_headers_sync(
    *,
    method: str,
    url: str,
    headers: Mapping[str, str] | None = None,
    scopes: tuple[str, ...] | None = None,
) -> dict[str, str]:
    """Return request headers after applying Google ADC credentials."""
    try:
        import google.auth
        from google.auth.exceptions import DefaultCredentialsError, RefreshError
        from google.auth.transport.requests import Request
    except ImportError as exc:
        msg = (
            "Google ADC auth requires the `google-auth` package. "
            "Install the gateway with Google auth support and retry."
        )
        raise RuntimeError(msg) from exc

    normalized_scopes = scopes or google_adc_scopes(None)
    authorized_headers = {
        str(key): str(value) for key, value in (headers or {}).items()
    }

    try:
        with _GOOGLE_ADC_CREDENTIALS_LOCK:
            credentials = _GOOGLE_ADC_CREDENTIALS.get(normalized_scopes)
            if credentials is None:
                credentials, _project_id = google.auth.default(
                    scopes=list(normalized_scopes)
                )
                _GOOGLE_ADC_CREDENTIALS[normalized_scopes] = credentials

            credentials.before_request(
                Request(),
                method,
                url,
                authorized_headers,
            )
    except DefaultCredentialsError as exc:
        msg = (
            "Google ADC credentials were not found. Set "
            "`GOOGLE_APPLICATION_CREDENTIALS`, use workload identity, or run "
            "`gcloud auth application-default login` and retry."
        )
        raise RuntimeError(msg) from exc
    except RefreshError as exc:
        _GOOGLE_ADC_CREDENTIALS.pop(normalized_scopes, None)
        msg = f"Google ADC token refresh failed: {exc}"
        raise RuntimeError(msg) from exc

    return authorized_headers


async def google_adc_authorized_headers(
    *,
    method: str,
    url: str,
    headers: Mapping[str, str] | None = None,
    scopes: tuple[str, ...] | None = None,
) -> dict[str, str]:
    """Return request headers after applying Google ADC credentials."""
    return await asyncio.to_thread(
        google_adc_authorized_headers_sync,
        method=method,
        url=url,
        headers=headers,
        scopes=scopes,
    )


async def google_adc_access_token(
    *, scopes: tuple[str, ...] | None = None
) -> str:
    """Return a Google ADC access token from google-auth discovery."""
    return await asyncio.to_thread(
        google_adc_access_token_sync,
        scopes=scopes,
    )
