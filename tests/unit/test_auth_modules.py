from __future__ import annotations

import asyncio
import builtins
import sys
from types import ModuleType, SimpleNamespace
from typing import ClassVar

from fastmcp.client.auth.oauth import ClientNotFoundError
import httpx
import pytest

from sift_gateway.auth import config as auth_config_mod
from sift_gateway.auth import google_adc as google_adc_mod
from sift_gateway.auth.oauth_login import (
    oauth_apply_client_config,
    oauth_async_auth_flow_once,
    oauth_client_info_from_config,
    oauth_context_access_token,
    oauth_has_server_binding,
    oauth_login_access_token,
    oauth_login_access_token_proactive,
)


def test_auth_mode_handles_legacy_mode_alias(monkeypatch) -> None:
    monkeypatch.setattr(auth_config_mod, "auth_enabled", lambda _cfg: True)
    assert auth_config_mod.auth_mode(None) is None
    assert auth_config_mod.auth_mode({"enabled": True, "mode": "fastmcp"}) == (
        auth_config_mod.AUTH_MODE_OAUTH
    )


def test_oauth_registration_returns_none_for_non_dict_when_mode_is_oauth(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        auth_config_mod,
        "auth_mode",
        lambda _cfg: auth_config_mod.AUTH_MODE_OAUTH,
    )
    assert auth_config_mod.oauth_registration(None) is None


def test_auth_scope_callback_port_and_google_adc_scope_normalization() -> None:
    assert auth_config_mod.auth_scope(None) is None
    assert (
        auth_config_mod.auth_scope({"scopes": [" ", "scope.a", "", "scope.b"]})
        == "scope.a scope.b"
    )
    assert auth_config_mod.auth_scope({"scopes": [" ", ""]}) is None
    assert auth_config_mod.oauth_callback_port({"callback_port": False}) is None
    with pytest.raises(RuntimeError, match="between 1 and 65535"):
        auth_config_mod.oauth_callback_port({"callback_port": "oops"})
    assert auth_config_mod.google_adc_scopes(
        {"google_scopes": "scope.a scope.b"}
    ) == ("scope.a", "scope.b")


def test_oauth_session_settings_returns_empty_for_disabled_auth() -> None:
    assert auth_config_mod.oauth_session_settings({"enabled": False}) == {}


def _install_fake_google_auth(
    monkeypatch,
    *,
    default_impl,
    request_cls=object,
    default_error_cls=RuntimeError,
    refresh_error_cls=RuntimeError,
) -> None:
    google_mod = ModuleType("google")
    auth_mod = ModuleType("google.auth")
    exceptions_mod = ModuleType("google.auth.exceptions")
    transport_mod = ModuleType("google.auth.transport")
    requests_mod = ModuleType("google.auth.transport.requests")

    auth_mod.default = default_impl
    exceptions_mod.DefaultCredentialsError = default_error_cls
    exceptions_mod.RefreshError = refresh_error_cls
    requests_mod.Request = request_cls
    transport_mod.requests = requests_mod
    auth_mod.exceptions = exceptions_mod
    auth_mod.transport = transport_mod
    google_mod.auth = auth_mod

    monkeypatch.setitem(sys.modules, "google", google_mod)
    monkeypatch.setitem(sys.modules, "google.auth", auth_mod)
    monkeypatch.setitem(sys.modules, "google.auth.exceptions", exceptions_mod)
    monkeypatch.setitem(sys.modules, "google.auth.transport", transport_mod)
    monkeypatch.setitem(
        sys.modules,
        "google.auth.transport.requests",
        requests_mod,
    )


def test_google_adc_access_token_sync_rewrites_import_error(
    monkeypatch,
) -> None:
    real_import = builtins.__import__

    def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "google.auth" or name.startswith("google.auth."):
            raise ImportError("missing google auth")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    with pytest.raises(RuntimeError, match="google-auth"):
        google_adc_mod.google_adc_access_token_sync()

    with pytest.raises(RuntimeError, match="google-auth"):
        google_adc_mod.google_adc_authorized_headers_sync(
            method="GET",
            url="https://example.com/mcp",
        )


def test_google_adc_access_token_sync_rewrites_default_credentials_error(
    monkeypatch,
) -> None:
    class _DefaultCredentialsError(Exception):
        pass

    class _RefreshError(Exception):
        pass

    def _fake_default(*, scopes):
        _ = scopes
        raise _DefaultCredentialsError("missing")

    _install_fake_google_auth(
        monkeypatch,
        default_impl=_fake_default,
        default_error_cls=_DefaultCredentialsError,
        refresh_error_cls=_RefreshError,
    )

    with pytest.raises(RuntimeError, match="credentials were not found"):
        google_adc_mod.google_adc_access_token_sync()

    with pytest.raises(RuntimeError, match="credentials were not found"):
        google_adc_mod.google_adc_authorized_headers_sync(
            method="GET",
            url="https://example.com/mcp",
        )


def test_google_adc_access_token_sync_rewrites_refresh_error_and_empty_token(
    monkeypatch,
) -> None:
    monkeypatch.setattr(google_adc_mod, "_GOOGLE_ADC_CREDENTIALS", {})

    class _DefaultCredentialsError(Exception):
        pass

    class _RefreshError(Exception):
        pass

    class _RefreshFailsCredentials:
        token = ""
        valid = False

        def refresh(self, _request) -> None:
            raise _RefreshError("expired")

    def _fake_default_refresh_error(*, scopes):
        _ = scopes
        return _RefreshFailsCredentials(), "demo-project"

    _install_fake_google_auth(
        monkeypatch,
        default_impl=_fake_default_refresh_error,
        default_error_cls=_DefaultCredentialsError,
        refresh_error_cls=_RefreshError,
    )

    with pytest.raises(RuntimeError, match="token refresh failed"):
        google_adc_mod.google_adc_access_token_sync()

    assert google_adc_mod._GOOGLE_ADC_CREDENTIALS == {}

    class _EmptyTokenCredentials:
        token = ""
        valid = False

        def refresh(self, _request) -> None:
            self.token = "  "
            self.valid = True

    monkeypatch.setattr(google_adc_mod, "_GOOGLE_ADC_CREDENTIALS", {})

    def _fake_default_empty_token(*, scopes):
        _ = scopes
        return _EmptyTokenCredentials(), "demo-project"

    _install_fake_google_auth(
        monkeypatch,
        default_impl=_fake_default_empty_token,
        default_error_cls=_DefaultCredentialsError,
        refresh_error_cls=_RefreshError,
    )

    with pytest.raises(RuntimeError, match="empty access token"):
        google_adc_mod.google_adc_access_token_sync()


def test_google_adc_authorized_headers_sync_rewrites_refresh_error(
    monkeypatch,
) -> None:
    monkeypatch.setattr(google_adc_mod, "_GOOGLE_ADC_CREDENTIALS", {})

    class _DefaultCredentialsError(Exception):
        pass

    class _RefreshError(Exception):
        pass

    class _FailingCredentials:
        def before_request(self, _request, _method, _url, _headers) -> None:
            raise _RefreshError("expired")

    def _fake_default(*, scopes):
        _ = scopes
        return _FailingCredentials(), "demo-project"

    _install_fake_google_auth(
        monkeypatch,
        default_impl=_fake_default,
        default_error_cls=_DefaultCredentialsError,
        refresh_error_cls=_RefreshError,
    )

    with pytest.raises(RuntimeError, match="token refresh failed"):
        google_adc_mod.google_adc_authorized_headers_sync(
            method="POST",
            url="https://example.com/mcp",
        )

    assert google_adc_mod._GOOGLE_ADC_CREDENTIALS == {}


@pytest.mark.asyncio
async def test_google_adc_async_wrappers_delegate_via_to_thread(
    monkeypatch,
) -> None:
    seen: list[tuple[object, tuple[object, ...], dict[str, object]]] = []

    async def _fake_to_thread(func, /, *args, **kwargs):
        seen.append((func, args, kwargs))
        return "value"

    monkeypatch.setattr(asyncio, "to_thread", _fake_to_thread)

    assert (
        await google_adc_mod.google_adc_authorized_headers(
            method="GET",
            url="https://example.com/mcp",
        )
        == "value"
    )
    assert await google_adc_mod.google_adc_access_token() == "value"
    assert seen[0][0] is google_adc_mod.google_adc_authorized_headers_sync
    assert seen[1][0] is google_adc_mod.google_adc_access_token_sync


def test_oauth_context_access_token_and_server_binding_helpers() -> None:
    assert oauth_context_access_token(object()) == ""
    assert (
        oauth_context_access_token(
            SimpleNamespace(
                context=SimpleNamespace(
                    current_tokens=SimpleNamespace(access_token=" tok ")
                )
            )
        )
        == "tok"
    )
    assert oauth_has_server_binding(SimpleNamespace(_bound=True)) is True
    assert (
        oauth_has_server_binding(
            SimpleNamespace(
                context=SimpleNamespace(server_url=None),
                mcp_url="https://example.com/mcp",
            )
        )
        is True
    )


def test_oauth_client_info_from_config_validation() -> None:
    assert (
        oauth_client_info_from_config(
            auth_config=None,
            redirect_uris=["http://localhost:45789/callback"],
            client_name="demo",
        )
        is None
    )
    assert (
        oauth_client_info_from_config(
            auth_config={"client_secret": "secret"},
            redirect_uris=["http://localhost:45789/callback"],
            client_name="demo",
        )
        is None
    )
    with pytest.raises(RuntimeError, match="requires a client_secret"):
        oauth_client_info_from_config(
            auth_config={
                "client_id": "client-123",
                "token_endpoint_auth_method": "client_secret_basic",
            },
            redirect_uris=["http://localhost:45789/callback"],
            client_name="demo",
        )
    with pytest.raises(
        RuntimeError, match="cannot be used with a client_secret"
    ):
        oauth_client_info_from_config(
            auth_config={
                "client_id": "client-123",
                "client_secret": "secret-456",
                "token_endpoint_auth_method": "none",
            },
            redirect_uris=["http://localhost:45789/callback"],
            client_name="demo",
        )
    with pytest.raises(
        RuntimeError, match="Unsupported OAuth token auth method"
    ):
        oauth_client_info_from_config(
            auth_config={
                "client_id": "client-123",
                "token_endpoint_auth_method": "private_key_jwt",
            },
            redirect_uris=["http://localhost:45789/callback"],
            client_name="demo",
        )


@pytest.mark.asyncio
async def test_oauth_apply_client_config_handles_missing_context_and_no_client_info() -> (
    None
):
    oauth = SimpleNamespace(context=None)
    await oauth_apply_client_config(
        oauth=oauth,
        auth_config={"client_id": "client-123"},
    )

    storage_calls: list[object] = []

    class _Storage:
        async def set_client_info(self, client_info) -> None:
            storage_calls.append(client_info)

    oauth = SimpleNamespace(
        context=SimpleNamespace(
            client_metadata=SimpleNamespace(
                redirect_uris=["http://localhost:45789/callback"],
                client_name="demo",
                scope=None,
            ),
            storage=_Storage(),
            client_info=None,
        )
    )
    await oauth_apply_client_config(
        oauth=oauth,
        auth_config={"enabled": True, "mode": "oauth", "scope": "scope.a"},
    )
    assert oauth._sift_explicit_scope == "scope.a"
    assert oauth.context.client_info is None
    assert storage_calls == []


@pytest.mark.asyncio
async def test_oauth_async_auth_flow_once_requires_binding() -> None:
    request = httpx.Request("GET", "https://example.com/mcp")
    flow = oauth_async_auth_flow_once(
        oauth=SimpleNamespace(context=SimpleNamespace()),
        request=request,
        explicit_scope=None,
    )
    with pytest.raises(RuntimeError, match="has no server URL"):
        await flow.asend(None)


@pytest.mark.asyncio
async def test_oauth_async_auth_flow_once_refresh_failure_then_request(
    monkeypatch,
) -> None:
    import mcp.client.auth.utils as oauth_utils

    monkeypatch.setattr(
        oauth_utils,
        "build_oauth_authorization_server_metadata_discovery_urls",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        oauth_utils,
        "build_protected_resource_metadata_discovery_urls",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_client_info_from_metadata_url",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_client_registration_request",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        oauth_utils, "create_oauth_metadata_request", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        oauth_utils, "extract_field_from_www_auth", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        oauth_utils,
        "extract_resource_metadata_from_www_auth",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        oauth_utils, "extract_scope_from_www_auth", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        oauth_utils, "get_client_metadata_scopes", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_auth_metadata_response",
        lambda *_a, **_k: pytest.fail("should not reach auth metadata"),
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_protected_resource_response",
        lambda *_a, **_k: pytest.fail("should not reach resource metadata"),
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_registration_response",
        lambda *_a, **_k: pytest.fail("should not register"),
    )
    monkeypatch.setattr(
        oauth_utils, "should_use_client_metadata_url", lambda *_a, **_k: False
    )

    seen: dict[str, object] = {}
    context = SimpleNamespace(
        lock=asyncio.Lock(),
        protocol_version=None,
        is_token_valid=lambda: False,
        can_refresh_token=lambda: True,
        protected_resource_metadata=None,
        oauth_metadata=None,
        auth_server_url=None,
        client_metadata=SimpleNamespace(scope=None, redirect_uris=[]),
        client_info=object(),
        client_metadata_url=None,
        storage=None,
        server_url="https://example.com/mcp",
    )

    class _OAuth:
        def __init__(self) -> None:
            self.context = context
            self._initialized = False

        async def _initialize(self) -> None:
            seen["initialized"] = True
            self._initialized = True

        async def _refresh_token(self):
            return httpx.Request("POST", "https://auth.example.test/token")

        async def _handle_refresh_response(self, _response) -> bool:
            return False

        def _add_auth_header(self, request) -> None:
            request.headers["Authorization"] = "Bearer tok"

    request = httpx.Request("GET", "https://example.com/mcp")
    flow = oauth_async_auth_flow_once(
        oauth=_OAuth(),
        request=request,
        explicit_scope=None,
    )
    refresh_request = await flow.asend(None)
    assert refresh_request.method == "POST"
    retry_request = await flow.asend(
        httpx.Response(401, request=refresh_request)
    )
    assert retry_request.url == request.url
    with pytest.raises(StopAsyncIteration):
        await flow.asend(httpx.Response(200, request=retry_request))
    assert seen["initialized"] is True


@pytest.mark.asyncio
async def test_oauth_async_auth_flow_once_401_uses_metadata_url(
    monkeypatch,
) -> None:
    import mcp.client.auth.utils as oauth_utils

    stored_client_info: list[object] = []

    class _Storage:
        async def set_client_info(self, client_info) -> None:
            stored_client_info.append(client_info)

    monkeypatch.setattr(
        oauth_utils,
        "build_protected_resource_metadata_discovery_urls",
        lambda *_args, **_kwargs: ["https://resource.example/meta"],
    )
    monkeypatch.setattr(
        oauth_utils,
        "build_oauth_authorization_server_metadata_discovery_urls",
        lambda *_args, **_kwargs: ["https://auth.example/meta"],
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_oauth_metadata_request",
        lambda url: httpx.Request("GET", url),
    )

    async def _handle_protected_resource_response(_response):
        return SimpleNamespace(
            authorization_servers=["https://auth.example.test"]
        )

    async def _handle_auth_metadata_response(_response):
        return False, None

    monkeypatch.setattr(
        oauth_utils,
        "handle_protected_resource_response",
        _handle_protected_resource_response,
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_auth_metadata_response",
        _handle_auth_metadata_response,
    )
    monkeypatch.setattr(
        oauth_utils,
        "extract_resource_metadata_from_www_auth",
        lambda _response: None,
    )
    monkeypatch.setattr(
        oauth_utils,
        "extract_scope_from_www_auth",
        lambda _response: "scope.server",
    )
    monkeypatch.setattr(
        oauth_utils,
        "get_client_metadata_scopes",
        lambda *_args, **_kwargs: "scope.server",
    )
    monkeypatch.setattr(
        oauth_utils,
        "should_use_client_metadata_url",
        lambda *_args, **_kwargs: True,
    )
    client_info = SimpleNamespace(client_id="client-meta")
    monkeypatch.setattr(
        oauth_utils,
        "create_client_info_from_metadata_url",
        lambda *_args, **_kwargs: client_info,
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_client_registration_request",
        lambda *_args, **_kwargs: pytest.fail("should not register"),
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_registration_response",
        lambda *_args, **_kwargs: pytest.fail("should not register"),
    )
    monkeypatch.setattr(
        oauth_utils, "extract_field_from_www_auth", lambda *_a, **_k: None
    )

    context = SimpleNamespace(
        lock=asyncio.Lock(),
        protocol_version=None,
        is_token_valid=lambda: False,
        can_refresh_token=lambda: False,
        protected_resource_metadata=None,
        oauth_metadata=None,
        auth_server_url=None,
        client_metadata=SimpleNamespace(
            scope=None, redirect_uris=["http://localhost:45789/callback"]
        ),
        client_info=None,
        client_metadata_url="https://client.example/meta",
        storage=_Storage(),
        server_url="https://example.com/mcp",
        get_authorization_base_url=lambda _url: "https://auth.example.test",
    )

    class _OAuth:
        def __init__(self) -> None:
            self.context = context
            self._initialized = True

        async def _perform_authorization(self):
            return httpx.Request("POST", "https://auth.example.test/token")

        async def _handle_token_response(self, _response) -> None:
            return None

        def _add_auth_header(self, request) -> None:
            request.headers["Authorization"] = "Bearer tok"

    request = httpx.Request("POST", "https://example.com/mcp")
    flow = oauth_async_auth_flow_once(
        oauth=_OAuth(),
        request=request,
        explicit_scope=None,
    )
    first_request = await flow.asend(None)
    assert first_request.url == request.url
    discovery_request = await flow.asend(
        httpx.Response(
            401,
            request=first_request,
            headers={"WWW-Authenticate": 'Bearer scope="scope.server"'},
        )
    )
    assert str(discovery_request.url) == "https://resource.example/meta"
    asm_request = await flow.asend(
        httpx.Response(200, request=discovery_request)
    )
    assert str(asm_request.url) == "https://auth.example/meta"
    token_request = await flow.asend(httpx.Response(200, request=asm_request))
    assert str(token_request.url) == "https://auth.example.test/token"
    final_request = await flow.asend(httpx.Response(200, request=token_request))
    assert final_request.headers["Authorization"] == "Bearer tok"
    with pytest.raises(StopAsyncIteration):
        await flow.asend(httpx.Response(200, request=final_request))
    assert context.client_metadata.scope == "scope.server"
    assert stored_client_info == [client_info]


@pytest.mark.asyncio
async def test_oauth_async_auth_flow_once_403_reauths_for_insufficient_scope(
    monkeypatch,
) -> None:
    import mcp.client.auth.utils as oauth_utils

    monkeypatch.setattr(
        oauth_utils,
        "build_oauth_authorization_server_metadata_discovery_urls",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        oauth_utils,
        "build_protected_resource_metadata_discovery_urls",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_client_info_from_metadata_url",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_client_registration_request",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        oauth_utils, "create_oauth_metadata_request", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        oauth_utils,
        "extract_field_from_www_auth",
        lambda _response, field: (
            "insufficient_scope" if field == "error" else None
        ),
    )
    monkeypatch.setattr(
        oauth_utils,
        "extract_resource_metadata_from_www_auth",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        oauth_utils,
        "extract_scope_from_www_auth",
        lambda *_args, **_kwargs: "scope.server",
    )
    monkeypatch.setattr(
        oauth_utils,
        "get_client_metadata_scopes",
        lambda *_args, **_kwargs: "scope.server",
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_auth_metadata_response",
        lambda *_args, **_kwargs: (True, None),
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_protected_resource_response",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_registration_response",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        oauth_utils, "should_use_client_metadata_url", lambda *_a, **_k: False
    )

    seen: dict[str, object] = {}
    context = SimpleNamespace(
        lock=asyncio.Lock(),
        protocol_version=None,
        is_token_valid=lambda: True,
        can_refresh_token=lambda: False,
        protected_resource_metadata=SimpleNamespace(),
        oauth_metadata=None,
        auth_server_url=None,
        client_metadata=SimpleNamespace(scope=None, redirect_uris=[]),
        client_info=object(),
        client_metadata_url=None,
        storage=None,
        server_url="https://example.com/mcp",
    )

    class _OAuth:
        def __init__(self) -> None:
            self.context = context
            self._initialized = False

        async def _initialize(self) -> None:
            self._initialized = True

        async def _perform_authorization(self):
            seen["scope"] = self.context.client_metadata.scope
            return httpx.Request("POST", "https://auth.example.test/token")

        async def _handle_token_response(self, _response) -> None:
            return None

        def _add_auth_header(self, request) -> None:
            request.headers["Authorization"] = "Bearer tok"

    request = httpx.Request("GET", "https://example.com/mcp")
    flow = oauth_async_auth_flow_once(
        oauth=_OAuth(),
        request=request,
        explicit_scope=None,
    )
    first_request = await flow.asend(None)
    assert first_request.headers["Authorization"] == "Bearer tok"
    token_request = await flow.asend(
        httpx.Response(
            403,
            request=first_request,
            headers={"WWW-Authenticate": 'Bearer error="insufficient_scope"'},
        )
    )
    assert str(token_request.url) == "https://auth.example.test/token"
    retry_request = await flow.asend(httpx.Response(200, request=token_request))
    assert retry_request.headers["Authorization"] == "Bearer tok"
    with pytest.raises(StopAsyncIteration):
        await flow.asend(httpx.Response(200, request=retry_request))
    assert seen["scope"] == "scope.server"


@pytest.mark.asyncio
async def test_oauth_login_access_token_proactive_error_branches(
    monkeypatch,
) -> None:
    with pytest.raises(RuntimeError, match="context is unavailable"):
        await oauth_login_access_token_proactive(
            oauth=SimpleNamespace(context=None),
            url="https://example.com/mcp",
        )

    with pytest.raises(RuntimeError, match="HTTP client factory"):
        await oauth_login_access_token_proactive(
            oauth=SimpleNamespace(
                context=SimpleNamespace(current_tokens=None),
                httpx_client_factory=None,
            ),
            url="https://example.com/mcp",
        )


@pytest.mark.asyncio
async def test_oauth_login_access_token_proactive_refresh_and_retry_paths(
    monkeypatch,
) -> None:
    import mcp.client.auth.utils as oauth_utils

    class _FakeClient:
        def __init__(self) -> None:
            self.responses: list[object] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

        async def send(self, request):
            self.responses.append(request)
            return object()

    client = _FakeClient()
    context = SimpleNamespace(
        current_tokens=None,
        is_token_valid=lambda: False,
        can_refresh_token=lambda: True,
        client_metadata=SimpleNamespace(
            scope=None,
            redirect_uris=["http://localhost:45789/callback"],
        ),
        protected_resource_metadata=None,
        oauth_metadata=None,
        auth_server_url=None,
        client_info=None,
        client_metadata_url="https://client.example/meta",
        storage=SimpleNamespace(set_client_info=lambda _client_info: None),
        server_url="https://example.com/mcp",
        get_authorization_base_url=lambda _url: "https://auth.example.test",
    )

    async def _set_client_info(client_info) -> None:
        context.client_info = client_info

    context.storage = SimpleNamespace(set_client_info=_set_client_info)

    calls = {"auth": 0, "clears": 0}

    class _OAuth:
        def __init__(
            self, *, static_client: bool, succeed_refresh: bool
        ) -> None:
            self.context = context
            self.httpx_client_factory = lambda: client
            self._initialized = True
            self._static_client_info = object() if static_client else None
            self.token_storage_adapter = SimpleNamespace(clear=self._clear)
            self._succeed_refresh = succeed_refresh

        async def _clear(self) -> None:
            calls["clears"] += 1

        async def _refresh_token(self):
            return httpx.Request("POST", "https://auth.example.test/refresh")

        async def _handle_refresh_response(self, _response) -> bool:
            if self._succeed_refresh:
                self.context.current_tokens = SimpleNamespace(
                    access_token="tok.refresh"
                )
                return True
            return False

        async def _perform_authorization(self):
            calls["auth"] += 1
            if calls["auth"] == 1:
                raise ClientNotFoundError("missing client")
            return httpx.Request("POST", "https://auth.example.test/token")

        async def _handle_token_response(self, _response) -> None:
            self.context.current_tokens = SimpleNamespace(
                access_token="tok.retried"
            )

    monkeypatch.setattr(
        oauth_utils,
        "build_protected_resource_metadata_discovery_urls",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        oauth_utils,
        "build_oauth_authorization_server_metadata_discovery_urls",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_oauth_metadata_request",
        lambda url: httpx.Request("GET", url),
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_protected_resource_response",
        lambda _response: None,
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_auth_metadata_response",
        lambda _response: (False, None),
    )
    monkeypatch.setattr(
        oauth_utils,
        "get_client_metadata_scopes",
        lambda *_args, **_kwargs: "scope.server",
    )
    monkeypatch.setattr(
        oauth_utils,
        "should_use_client_metadata_url",
        lambda *_args, **_kwargs: True,
    )
    client_info = SimpleNamespace(client_id="client-meta")
    monkeypatch.setattr(
        oauth_utils,
        "create_client_info_from_metadata_url",
        lambda *_args, **_kwargs: client_info,
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_client_registration_request",
        lambda *_args, **_kwargs: pytest.fail("should not register"),
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_registration_response",
        lambda *_args, **_kwargs: pytest.fail("should not register"),
    )

    token = await oauth_login_access_token_proactive(
        oauth=_OAuth(static_client=False, succeed_refresh=False),
        url="https://example.com/mcp",
    )
    assert token == "tok.retried"
    assert calls["clears"] == 1

    calls["auth"] = 0
    context.current_tokens = None
    with pytest.raises(
        ClientNotFoundError,
        match="rejected the static client credentials",
    ):
        await oauth_login_access_token_proactive(
            oauth=_OAuth(static_client=True, succeed_refresh=False),
            url="https://example.com/mcp",
        )

    context.current_tokens = None
    token = await oauth_login_access_token_proactive(
        oauth=_OAuth(static_client=False, succeed_refresh=True),
        url="https://example.com/mcp",
    )
    assert token == "tok.refresh"


@pytest.mark.asyncio
async def test_oauth_login_access_token_proactive_raises_when_no_token_returned(
    monkeypatch,
) -> None:
    import mcp.client.auth.utils as oauth_utils

    class _FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

        async def send(self, _request):
            return object()

    async def _set_client_info(_client_info) -> None:
        return None

    context = SimpleNamespace(
        current_tokens=None,
        is_token_valid=lambda: False,
        can_refresh_token=lambda: False,
        client_metadata=SimpleNamespace(
            scope="scope.explicit",
            redirect_uris=["http://localhost:45789/callback"],
        ),
        protected_resource_metadata=None,
        oauth_metadata=None,
        auth_server_url=None,
        client_info=object(),
        client_metadata_url=None,
        storage=SimpleNamespace(set_client_info=_set_client_info),
        server_url="https://example.com/mcp",
        get_authorization_base_url=lambda _url: "https://auth.example.test",
    )

    class _OAuth:
        def __init__(self) -> None:
            self.context = context
            self.httpx_client_factory = _FakeClient
            self._initialized = True
            self._static_client_info = None
            self.token_storage_adapter = SimpleNamespace(clear=lambda: None)

        async def _perform_authorization(self):
            return httpx.Request("POST", "https://auth.example.test/token")

        async def _handle_token_response(self, _response) -> None:
            context.current_tokens = SimpleNamespace(access_token=None)

    monkeypatch.setattr(
        oauth_utils,
        "build_protected_resource_metadata_discovery_urls",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        oauth_utils,
        "build_oauth_authorization_server_metadata_discovery_urls",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_oauth_metadata_request",
        lambda url: httpx.Request("GET", url),
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_protected_resource_response",
        lambda _response: None,
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_auth_metadata_response",
        lambda _response: (True, None),
    )
    monkeypatch.setattr(
        oauth_utils,
        "get_client_metadata_scopes",
        lambda *_args, **_kwargs: "scope.server",
    )
    monkeypatch.setattr(
        oauth_utils,
        "should_use_client_metadata_url",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_client_info_from_metadata_url",
        lambda *_args, **_kwargs: pytest.fail("should not create client info"),
    )
    monkeypatch.setattr(
        oauth_utils,
        "create_client_registration_request",
        lambda *_args, **_kwargs: httpx.Request(
            "POST", "https://auth.example.test/register"
        ),
    )
    monkeypatch.setattr(
        oauth_utils,
        "handle_registration_response",
        lambda _response: SimpleNamespace(client_id="client-123"),
    )

    with pytest.raises(RuntimeError, match="no access token was returned"):
        await oauth_login_access_token_proactive(
            oauth=_OAuth(),
            url="https://example.com/mcp",
        )


class _FakeOAuthBase:
    instances: ClassVar[list[object]] = []

    def __init__(
        self,
        _mcp_url: str,
        *,
        token_storage=None,
        callback_port=None,
    ) -> None:
        _ = token_storage
        self.redirect_port = callback_port or 45789
        self.token_storage_adapter = SimpleNamespace(clear=self._clear)
        self.context = SimpleNamespace(
            client_metadata=SimpleNamespace(
                redirect_uris=["http://localhost:45789/callback"],
                client_name="FastMCP Client",
                scope=None,
            ),
            storage=None,
            client_info=None,
            current_tokens=None,
            is_token_valid=lambda: True,
        )
        self._initialized = True
        self._static_client_info = None
        self.clear_calls = 0
        _FakeOAuthBase.instances.append(self)

    async def _clear(self) -> None:
        self.clear_calls += 1


class _FakeTransport:
    def __init__(self, *, url: str, auth, headers=None) -> None:
        _ = (url, headers)
        self.auth = auth


class _FakeClient:
    def __init__(self, transport, timeout: float = 30.0) -> None:
        _ = timeout
        self.transport = transport

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _tb) -> bool:
        return False

    async def list_tools(self) -> list[object]:
        flow = self.transport.auth.async_auth_flow(
            httpx.Request("GET", "https://example.com/mcp")
        )
        response = None
        while True:
            try:
                yielded_request = await flow.asend(response)
            except StopAsyncIteration:
                break
            response = httpx.Response(200, request=yielded_request)
        return []


@pytest.mark.asyncio
async def test_oauth_login_access_token_retries_stale_client_in_async_flow(
    monkeypatch,
) -> None:
    _FakeOAuthBase.instances.clear()
    call_count = {"flow": 0}

    async def _fake_auth_flow_once(*, oauth, request, explicit_scope):
        _ = (oauth, request, explicit_scope)
        call_count["flow"] += 1
        if call_count["flow"] == 1:
            raise ClientNotFoundError("client missing")
        yield httpx.Request("POST", "https://auth.example.test/token")

    async def _fake_proactive_access_token(*, oauth, url):
        _ = (oauth, url)
        return "tok.retry"

    monkeypatch.setattr("fastmcp.client.auth.OAuth", _FakeOAuthBase)
    monkeypatch.setattr(
        "fastmcp.client.transports.SSETransport", _FakeTransport
    )
    monkeypatch.setattr(
        "fastmcp.client.transports.StreamableHttpTransport",
        _FakeTransport,
    )
    monkeypatch.setattr(
        "fastmcp.mcp_config.infer_transport_type_from_url",
        lambda _url: "streamable-http",
    )
    monkeypatch.setattr("fastmcp.Client", _FakeClient)

    token = await oauth_login_access_token(
        url="https://example.com/mcp",
        resolve_callback_url_headless=lambda **_kwargs: pytest.fail(
            "headless resolver should not be used"
        ),
        auth_flow_once=_fake_auth_flow_once,
        proactive_access_token=_fake_proactive_access_token,
    )

    assert token == "tok.retry"
    assert _FakeOAuthBase.instances[0].clear_calls == 1


@pytest.mark.asyncio
async def test_oauth_login_access_token_retries_stale_client_in_async_flow_clears_in_memory_state(
    monkeypatch,
) -> None:
    _FakeOAuthBase.instances.clear()
    call_count = {"flow": 0}

    async def _fake_auth_flow_once(*, oauth, request, explicit_scope):
        _ = (request, explicit_scope)
        call_count["flow"] += 1
        if call_count["flow"] == 1:
            oauth.context.client_info = object()
            oauth.context.current_tokens = SimpleNamespace(
                access_token="tok.stale"
            )
            raise ClientNotFoundError("client missing")
        assert oauth.context.client_info is None
        assert oauth.context.current_tokens is None
        yield httpx.Request("POST", "https://auth.example.test/token")

    async def _fake_proactive_access_token(*, oauth, url):
        _ = (oauth, url)
        return "tok.retry"

    monkeypatch.setattr("fastmcp.client.auth.OAuth", _FakeOAuthBase)
    monkeypatch.setattr(
        "fastmcp.client.transports.SSETransport", _FakeTransport
    )
    monkeypatch.setattr(
        "fastmcp.client.transports.StreamableHttpTransport",
        _FakeTransport,
    )
    monkeypatch.setattr(
        "fastmcp.mcp_config.infer_transport_type_from_url",
        lambda _url: "streamable-http",
    )
    monkeypatch.setattr("fastmcp.Client", _FakeClient)

    token = await oauth_login_access_token(
        url="https://example.com/mcp",
        resolve_callback_url_headless=lambda **_kwargs: pytest.fail(
            "headless resolver should not be used"
        ),
        auth_flow_once=_fake_auth_flow_once,
        proactive_access_token=_fake_proactive_access_token,
    )

    assert token == "tok.retry"
    assert _FakeOAuthBase.instances[0].clear_calls == 1


@pytest.mark.asyncio
async def test_oauth_login_access_token_static_client_not_found_raises(
    monkeypatch,
) -> None:
    _FakeOAuthBase.instances.clear()

    async def _fake_auth_flow_once(*, oauth, request, explicit_scope):
        _ = (oauth, request, explicit_scope)
        if False:
            yield request
        raise ClientNotFoundError("client missing")

    monkeypatch.setattr("fastmcp.client.auth.OAuth", _FakeOAuthBase)
    monkeypatch.setattr(
        "fastmcp.client.transports.SSETransport", _FakeTransport
    )
    monkeypatch.setattr(
        "fastmcp.client.transports.StreamableHttpTransport",
        _FakeTransport,
    )
    monkeypatch.setattr(
        "fastmcp.mcp_config.infer_transport_type_from_url",
        lambda _url: "streamable-http",
    )
    monkeypatch.setattr("fastmcp.Client", _FakeClient)

    with pytest.raises(
        ClientNotFoundError,
        match="rejected the static client credentials",
    ):
        await oauth_login_access_token(
            url="https://example.com/mcp",
            resolve_callback_url_headless=lambda **_kwargs: pytest.fail(
                "headless resolver should not be used"
            ),
            auth_flow_once=_fake_auth_flow_once,
            auth_config={
                "client_id": "bad-client",
                "callback_port": 45789,
            },
        )

    assert _FakeOAuthBase.instances[0].clear_calls == 0


@pytest.mark.asyncio
async def test_oauth_login_access_token_headless_callback_variants(
    monkeypatch,
) -> None:
    _FakeOAuthBase.instances.clear()

    class _HeadlessClient:
        def __init__(self, transport, timeout: float = 30.0) -> None:
            _ = timeout
            self.transport = transport

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb) -> bool:
            return False

        async def list_tools(self) -> list[object]:
            await self.transport.auth.redirect_handler(
                "https://auth.example.test/authorize"
            )
            code, state = await self.transport.auth.callback_handler()
            assert code == "code-123"
            assert state == "state-456"
            self.transport.auth.context.current_tokens = SimpleNamespace(
                access_token="tok.headless"
            )
            self.transport.auth.context.is_token_valid = lambda: True
            return []

    monkeypatch.setattr("fastmcp.client.auth.OAuth", _FakeOAuthBase)
    monkeypatch.setattr(
        "fastmcp.client.transports.SSETransport", _FakeTransport
    )
    monkeypatch.setattr(
        "fastmcp.client.transports.StreamableHttpTransport",
        _FakeTransport,
    )
    monkeypatch.setattr(
        "fastmcp.mcp_config.infer_transport_type_from_url",
        lambda _url: "streamable-http",
    )
    monkeypatch.setattr("fastmcp.Client", _HeadlessClient)

    async def _resolve_callback_url_headless(
        *, authorization_url: str, callback_port: int
    ) -> str:
        assert authorization_url == "https://auth.example.test/authorize"
        assert callback_port == 45789
        return "http://localhost:45789/callback?code=code-123&state=state-456"

    token = await oauth_login_access_token(
        url="https://example.com/mcp",
        resolve_callback_url_headless=_resolve_callback_url_headless,
        headless=True,
    )
    assert token == "tok.headless"


@pytest.mark.asyncio
async def test_oauth_login_access_token_headless_callback_errors(
    monkeypatch,
) -> None:
    _FakeOAuthBase.instances.clear()

    class _HeadlessClient:
        def __init__(self, transport, timeout: float = 30.0) -> None:
            _ = timeout
            self.transport = transport

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb) -> bool:
            return False

        async def list_tools(self) -> list[object]:
            await self.transport.auth.callback_handler()
            return []

    monkeypatch.setattr("fastmcp.client.auth.OAuth", _FakeOAuthBase)
    monkeypatch.setattr(
        "fastmcp.client.transports.SSETransport", _FakeTransport
    )
    monkeypatch.setattr(
        "fastmcp.client.transports.StreamableHttpTransport",
        _FakeTransport,
    )
    monkeypatch.setattr(
        "fastmcp.mcp_config.infer_transport_type_from_url",
        lambda _url: "streamable-http",
    )
    monkeypatch.setattr("fastmcp.Client", _HeadlessClient)

    with pytest.raises(RuntimeError, match="redirect did not start"):
        await oauth_login_access_token(
            url="https://example.com/mcp",
            resolve_callback_url_headless=lambda **_kwargs: "unused",
            headless=True,
        )

    class _ErrorClient(_HeadlessClient):
        async def list_tools(self) -> list[object]:
            await self.transport.auth.redirect_handler(
                "https://auth.example.test/authorize"
            )
            await self.transport.auth.callback_handler()
            return []

    monkeypatch.setattr("fastmcp.Client", _ErrorClient)

    async def _resolve_callback_error(
        *, authorization_url: str, callback_port: int
    ) -> str:
        _ = (authorization_url, callback_port)
        return (
            "http://localhost:45789/callback?"
            "error=access_denied&error_description=Denied"
        )

    with pytest.raises(RuntimeError, match="access_denied - Denied"):
        await oauth_login_access_token(
            url="https://example.com/mcp",
            resolve_callback_url_headless=_resolve_callback_error,
            headless=True,
        )

    async def _resolve_callback_missing_code(
        *, authorization_url: str, callback_port: int
    ) -> str:
        _ = (authorization_url, callback_port)
        return "http://localhost:45789/callback?state=state-456"

    with pytest.raises(RuntimeError, match="missing authorization code"):
        await oauth_login_access_token(
            url="https://example.com/mcp",
            resolve_callback_url_headless=_resolve_callback_missing_code,
            headless=True,
        )
