"""Normalized auth configuration helpers for upstream secrets."""

from __future__ import annotations

from typing import Any

AUTH_MODE_OAUTH = "oauth"
AUTH_MODE_GOOGLE_ADC = "google-adc"
_LEGACY_PROVIDER_TO_MODE = {
    "fastmcp": AUTH_MODE_OAUTH,
    AUTH_MODE_GOOGLE_ADC: AUTH_MODE_GOOGLE_ADC,
}
OAUTH_REGISTRATION_DYNAMIC = "dynamic"
OAUTH_REGISTRATION_PREREGISTERED = "preregistered"
_OAUTH_REGISTRATIONS = frozenset(
    {
        OAUTH_REGISTRATION_DYNAMIC,
        OAUTH_REGISTRATION_PREREGISTERED,
    }
)
OAUTH_STATIC_CLIENT_CALLBACK_PORT = 45789
OAUTH_TOKEN_ENDPOINT_AUTH_METHODS = frozenset(
    {
        "none",
        "client_secret_post",
        "client_secret_basic",
    }
)
OAUTH_SECRET_TOKEN_ENDPOINT_AUTH_METHODS = frozenset(
    {
        "client_secret_post",
        "client_secret_basic",
    }
)
_OAUTH_ONLY_FIELDS = frozenset(
    {
        "callback_port",
        "client_id",
        "client_secret",
        "client_metadata_url",
        "registration",
        "scope",
        "scopes",
        "token_endpoint_auth_method",
        "token_storage",
    }
)

_DEFAULT_GOOGLE_ADC_SCOPES: tuple[str, ...] = (
    "https://www.googleapis.com/auth/cloud-platform",
)


def auth_enabled(auth_config: dict[str, Any] | None) -> bool:
    """Return whether one auth config is enabled."""
    return isinstance(auth_config, dict) and bool(auth_config.get("enabled"))


def auth_mode(auth_config: dict[str, Any] | None) -> str | None:
    """Return the normalized auth mode for one secret payload."""
    if not auth_enabled(auth_config):
        return None
    if not isinstance(auth_config, dict):
        return None
    raw_mode = auth_config.get("mode")
    if isinstance(raw_mode, str):
        mode = raw_mode.strip()
        if mode in {AUTH_MODE_OAUTH, AUTH_MODE_GOOGLE_ADC}:
            return mode
        legacy_mode = _LEGACY_PROVIDER_TO_MODE.get(mode)
        if legacy_mode is not None:
            return legacy_mode
    raw_provider = auth_config.get("provider")
    if isinstance(raw_provider, str):
        provider = raw_provider.strip()
        legacy_mode = _LEGACY_PROVIDER_TO_MODE.get(provider)
        if legacy_mode is not None:
            return legacy_mode
    return AUTH_MODE_OAUTH


def oauth_registration(auth_config: dict[str, Any] | None) -> str | None:
    """Return the normalized OAuth registration strategy."""
    if auth_mode(auth_config) != AUTH_MODE_OAUTH:
        return None
    if not isinstance(auth_config, dict):
        return None
    raw_registration = auth_config.get("registration")
    if isinstance(raw_registration, str):
        registration = raw_registration.strip()
        if registration in _OAUTH_REGISTRATIONS:
            return registration
    raw_client_id = auth_config.get("client_id")
    if isinstance(raw_client_id, str) and raw_client_id.strip():
        return OAUTH_REGISTRATION_PREREGISTERED
    return OAUTH_REGISTRATION_DYNAMIC


def auth_scope(auth_config: dict[str, Any] | None) -> str | None:
    """Return a normalized scope string from stored auth config."""
    if not isinstance(auth_config, dict):
        return None
    raw_scope = auth_config.get("scope")
    if isinstance(raw_scope, str):
        scope = raw_scope.strip()
        return scope if scope else None
    raw_scopes = auth_config.get("scopes")
    if not isinstance(raw_scopes, list):
        return None
    scopes = [str(item).strip() for item in raw_scopes if str(item).strip()]
    if not scopes:
        return None
    return " ".join(scopes)


def oauth_callback_port(auth_config: dict[str, Any] | None) -> int | None:
    """Return a validated OAuth callback port from stored config."""
    if not isinstance(auth_config, dict):
        return None
    raw_callback_port = auth_config.get("callback_port")
    if isinstance(raw_callback_port, bool):
        raw_callback_port = None
    if isinstance(raw_callback_port, int):
        callback_port = raw_callback_port
    elif isinstance(raw_callback_port, str) and raw_callback_port.strip():
        try:
            callback_port = int(raw_callback_port.strip())
        except ValueError as exc:
            msg = "OAuth callback port must be an integer between 1 and 65535."
            raise RuntimeError(msg) from exc
    else:
        return None

    if 1 <= callback_port <= 65535:
        return callback_port
    msg = "OAuth callback port must be an integer between 1 and 65535."
    raise RuntimeError(msg)


def google_adc_scopes(
    auth_config: dict[str, Any] | None,
) -> tuple[str, ...]:
    """Return normalized per-upstream ADC scopes from auth config."""
    if not isinstance(auth_config, dict):
        return _DEFAULT_GOOGLE_ADC_SCOPES

    raw_scopes = auth_config.get("google_scopes")
    scopes: list[str] = []
    if isinstance(raw_scopes, str):
        scopes = [part.strip() for part in raw_scopes.split() if part.strip()]
    elif isinstance(raw_scopes, list):
        scopes = [str(item).strip() for item in raw_scopes if str(item).strip()]

    return tuple(scopes) if scopes else _DEFAULT_GOOGLE_ADC_SCOPES


def uses_oauth_session(auth_config: dict[str, Any] | None) -> bool:
    """Return whether auth config uses a persisted interactive OAuth session."""
    return auth_mode(auth_config) == AUTH_MODE_OAUTH


def normalize_auth_config(
    auth_config: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Normalize stored auth config to the current mode/registration model."""
    if not isinstance(auth_config, dict):
        return None

    normalized = dict(auth_config)
    if "enabled" in normalized:
        normalized["enabled"] = bool(normalized["enabled"])
    mode = auth_mode(normalized)
    if mode is None:
        return None

    normalized["mode"] = mode
    normalized.pop("provider", None)

    if mode == AUTH_MODE_GOOGLE_ADC:
        for key in _OAUTH_ONLY_FIELDS:
            normalized.pop(key, None)
        return normalized

    normalized["registration"] = oauth_registration(normalized)
    normalized.pop("google_scopes", None)
    return normalized


def oauth_session_settings(
    auth_config: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return auth settings that require a fresh login when changed."""
    normalized = normalize_auth_config(auth_config)
    if not isinstance(normalized, dict):
        return {}

    settings: dict[str, Any] = {
        "mode": auth_mode(normalized),
        "registration": oauth_registration(normalized),
        "scope": auth_scope(normalized),
        "callback_port": oauth_callback_port(normalized),
    }
    for key in (
        "client_id",
        "client_secret",
        "client_metadata_url",
        "token_endpoint_auth_method",
    ):
        raw_value = normalized.get(key)
        if isinstance(raw_value, str):
            value = raw_value.strip()
            settings[key] = value or None
        else:
            settings[key] = None
    return settings


def oauth_login_requires_session_reset(
    *,
    existing_auth: dict[str, Any] | None,
    auth_config: dict[str, Any] | None,
) -> bool:
    """Return whether cached OAuth session state must be purged before login."""
    normalized_existing = normalize_auth_config(existing_auth)
    if normalized_existing is None:
        return False
    return oauth_session_settings(existing_auth) != oauth_session_settings(
        auth_config
    )
