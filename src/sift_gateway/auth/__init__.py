"""Auth helpers for upstream login and runtime request signing."""

from sift_gateway.auth.config import (
    AUTH_MODE_GOOGLE_ADC,
    AUTH_MODE_OAUTH,
    OAUTH_REGISTRATION_DYNAMIC,
    OAUTH_REGISTRATION_PREREGISTERED,
    OAUTH_SECRET_TOKEN_ENDPOINT_AUTH_METHODS,
    OAUTH_STATIC_CLIENT_CALLBACK_PORT,
    OAUTH_TOKEN_ENDPOINT_AUTH_METHODS,
    auth_enabled,
    auth_mode,
    auth_scope,
    google_adc_scopes,
    normalize_auth_config,
    oauth_callback_port,
    oauth_login_requires_session_reset,
    oauth_registration,
    uses_oauth_session,
)

__all__ = [
    "AUTH_MODE_GOOGLE_ADC",
    "AUTH_MODE_OAUTH",
    "OAUTH_REGISTRATION_DYNAMIC",
    "OAUTH_REGISTRATION_PREREGISTERED",
    "OAUTH_SECRET_TOKEN_ENDPOINT_AUTH_METHODS",
    "OAUTH_STATIC_CLIENT_CALLBACK_PORT",
    "OAUTH_TOKEN_ENDPOINT_AUTH_METHODS",
    "auth_enabled",
    "auth_mode",
    "auth_scope",
    "google_adc_scopes",
    "normalize_auth_config",
    "oauth_callback_port",
    "oauth_login_requires_session_reset",
    "oauth_registration",
    "uses_oauth_session",
]
