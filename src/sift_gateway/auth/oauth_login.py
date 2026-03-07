"""Interactive OAuth login helpers for upstream registration."""

from __future__ import annotations

from collections.abc import AsyncGenerator
import contextlib
from typing import Any, Literal, cast
from urllib.parse import parse_qs, urlparse

from sift_gateway.auth.config import (
    OAUTH_SECRET_TOKEN_ENDPOINT_AUTH_METHODS,
    OAUTH_TOKEN_ENDPOINT_AUTH_METHODS,
    auth_scope,
    oauth_callback_port,
)

TokenEndpointAuthMethod = Literal[
    "none",
    "client_secret_post",
    "client_secret_basic",
]


def oauth_context_access_token(oauth: Any) -> str:
    """Return the current OAuth access token from provider context."""
    tokens = getattr(oauth, "context", None)
    current_tokens = (
        getattr(tokens, "current_tokens", None) if tokens is not None else None
    )
    raw_access_token = (
        getattr(current_tokens, "access_token", None)
        if current_tokens is not None
        else None
    )
    if not isinstance(raw_access_token, str):
        return ""
    return raw_access_token.strip()


def oauth_has_server_binding(oauth: Any) -> bool:
    """Return whether an OAuth provider is bound to one MCP server URL.

    FastMCP 2.14 binds eagerly and exposes only ``context.server_url``.
    Newer releases also expose ``_bound`` for deferred binding.
    """
    if hasattr(oauth, "_bound"):
        return bool(oauth._bound)

    context = getattr(oauth, "context", None)
    server_url = getattr(context, "server_url", None)
    if isinstance(server_url, str) and server_url.strip():
        return True

    mcp_url = getattr(oauth, "mcp_url", None)
    return isinstance(mcp_url, str) and bool(mcp_url.strip())


def oauth_client_info_from_config(
    *,
    auth_config: dict[str, Any] | None,
    redirect_uris: Any,
    client_name: str | None,
) -> Any | None:
    """Build OAuth client info from pre-registered client config."""
    if not isinstance(auth_config, dict):
        return None
    raw_client_id = auth_config.get("client_id")
    if not isinstance(raw_client_id, str) or not raw_client_id.strip():
        return None

    client_id = raw_client_id.strip()
    raw_client_secret = auth_config.get("client_secret")
    client_secret = (
        raw_client_secret.strip()
        if isinstance(raw_client_secret, str) and raw_client_secret.strip()
        else None
    )
    raw_auth_method = auth_config.get("token_endpoint_auth_method")
    if isinstance(raw_auth_method, str):
        auth_method = raw_auth_method.strip()
    elif client_secret:
        auth_method = "client_secret_post"
    else:
        auth_method = "none"

    if (
        auth_method in OAUTH_SECRET_TOKEN_ENDPOINT_AUTH_METHODS
        and not client_secret
    ):
        msg = (
            f"OAuth token auth method {auth_method!r} requires a client_secret."
        )
        raise RuntimeError(msg)

    if auth_method == "none" and client_secret:
        msg = (
            "OAuth token auth method 'none' cannot be used with a "
            "client_secret."
        )
        raise RuntimeError(msg)

    if auth_method not in OAUTH_TOKEN_ENDPOINT_AUTH_METHODS:
        msg = f"Unsupported OAuth token auth method: {auth_method!r}"
        raise RuntimeError(msg)
    typed_auth_method = cast(TokenEndpointAuthMethod, auth_method)

    from mcp.shared.auth import OAuthClientInformationFull

    return OAuthClientInformationFull(
        client_id=client_id,
        client_secret=client_secret,
        token_endpoint_auth_method=typed_auth_method,
        redirect_uris=redirect_uris,
        grant_types=["authorization_code", "refresh_token"],
        response_types=["code"],
        scope=auth_scope(auth_config),
        client_name=client_name,
    )


async def oauth_apply_client_config(
    *,
    oauth: Any,
    auth_config: dict[str, Any] | None,
) -> None:
    """Preload explicit OAuth client configuration onto an auth provider."""
    if not isinstance(auth_config, dict):
        return

    context = getattr(oauth, "context", None)
    if context is None:
        return

    scope = auth_scope(auth_config)
    if scope is not None:
        context.client_metadata.scope = scope
    oauth._sift_explicit_scope = scope

    client_info = oauth_client_info_from_config(
        auth_config=auth_config,
        redirect_uris=context.client_metadata.redirect_uris,
        client_name=getattr(context.client_metadata, "client_name", None),
    )
    if client_info is None:
        return

    context.client_info = client_info
    oauth._static_client_info = client_info
    storage = getattr(context, "storage", None)
    if storage is not None:
        await storage.set_client_info(client_info)


async def oauth_async_auth_flow_once(
    *,
    oauth: Any,
    request: Any,
    explicit_scope: str | None,
) -> AsyncGenerator[Any, Any]:
    """Run one OAuth auth-flow attempt while preserving explicit scopes."""
    from mcp.client.auth.utils import (
        build_oauth_authorization_server_metadata_discovery_urls,
        build_protected_resource_metadata_discovery_urls,
        create_client_info_from_metadata_url,
        create_client_registration_request,
        create_oauth_metadata_request,
        extract_field_from_www_auth,
        extract_resource_metadata_from_www_auth,
        extract_scope_from_www_auth,
        get_client_metadata_scopes,
        handle_auth_metadata_response,
        handle_protected_resource_response,
        handle_registration_response,
        should_use_client_metadata_url,
    )
    from mcp.client.streamable_http import MCP_PROTOCOL_VERSION

    if not oauth_has_server_binding(oauth):
        msg = (
            "OAuth provider has no server URL. Either pass mcp_url to OAuth() "
            "or use it with Client(auth=...) which provides the URL "
            "automatically."
        )
        raise RuntimeError(msg)

    context = oauth.context
    async with context.lock:
        if not getattr(oauth, "_initialized", False):
            await oauth._initialize()

        context.protocol_version = request.headers.get(MCP_PROTOCOL_VERSION)

        if not context.is_token_valid() and context.can_refresh_token():
            refresh_request = await oauth._refresh_token()
            refresh_response = yield refresh_request
            if not await oauth._handle_refresh_response(refresh_response):
                oauth._initialized = False

        if context.is_token_valid():
            oauth._add_auth_header(request)

        response = yield request

        if response.status_code == 401:
            try:
                www_auth_resource_metadata_url = (
                    extract_resource_metadata_from_www_auth(response)
                )
                prm_discovery_urls = (
                    build_protected_resource_metadata_discovery_urls(
                        www_auth_resource_metadata_url,
                        context.server_url,
                    )
                )

                for url in prm_discovery_urls:
                    discovery_request = create_oauth_metadata_request(url)
                    discovery_response = yield discovery_request
                    prm = await handle_protected_resource_response(
                        discovery_response
                    )
                    if prm:
                        context.protected_resource_metadata = prm
                        if prm.authorization_servers:
                            context.auth_server_url = str(
                                prm.authorization_servers[0]
                            )
                        break

                asm_discovery_urls = (
                    build_oauth_authorization_server_metadata_discovery_urls(
                        context.auth_server_url,
                        context.server_url,
                    )
                )

                for url in asm_discovery_urls:
                    oauth_metadata_request = create_oauth_metadata_request(url)
                    oauth_metadata_response = yield oauth_metadata_request
                    ok, asm = await handle_auth_metadata_response(
                        oauth_metadata_response
                    )
                    if not ok:
                        break
                    if asm:
                        context.oauth_metadata = asm
                        break

                context.client_metadata.scope = (
                    explicit_scope
                    if explicit_scope is not None
                    else get_client_metadata_scopes(
                        extract_scope_from_www_auth(response),
                        context.protected_resource_metadata,
                        context.oauth_metadata,
                    )
                )

                if not context.client_info:
                    if should_use_client_metadata_url(
                        context.oauth_metadata,
                        context.client_metadata_url,
                    ):
                        client_information = create_client_info_from_metadata_url(
                            context.client_metadata_url,
                            redirect_uris=context.client_metadata.redirect_uris,
                        )
                        context.client_info = client_information
                        await context.storage.set_client_info(
                            client_information
                        )
                    else:
                        registration_request = (
                            create_client_registration_request(
                                context.oauth_metadata,
                                context.client_metadata,
                                context.get_authorization_base_url(
                                    context.server_url
                                ),
                            )
                        )
                        registration_response = yield registration_request
                        client_information = await handle_registration_response(
                            registration_response
                        )
                        context.client_info = client_information
                        await context.storage.set_client_info(
                            client_information
                        )

                token_response = yield await oauth._perform_authorization()
                await oauth._handle_token_response(token_response)
            except Exception:
                raise

            oauth._add_auth_header(request)
            yield request
        elif response.status_code == 403:
            error = extract_field_from_www_auth(response, "error")
            if error == "insufficient_scope":
                try:
                    context.client_metadata.scope = (
                        explicit_scope
                        if explicit_scope is not None
                        else get_client_metadata_scopes(
                            extract_scope_from_www_auth(response),
                            context.protected_resource_metadata,
                        )
                    )
                    token_response = yield await oauth._perform_authorization()
                    await oauth._handle_token_response(token_response)
                except Exception:
                    raise

            oauth._add_auth_header(request)
            yield request


async def oauth_login_access_token_proactive(
    *,
    oauth: Any,
    url: str,
    retry_on_stale_client: bool = True,
) -> str:
    """Run OAuth login without relying on an upstream 401 challenge."""
    from fastmcp.client.auth.oauth import ClientNotFoundError
    from mcp.client.auth.utils import (
        build_oauth_authorization_server_metadata_discovery_urls,
        build_protected_resource_metadata_discovery_urls,
        create_client_info_from_metadata_url,
        create_client_registration_request,
        create_oauth_metadata_request,
        get_client_metadata_scopes,
        handle_auth_metadata_response,
        handle_protected_resource_response,
        handle_registration_response,
        should_use_client_metadata_url,
    )

    context = getattr(oauth, "context", None)
    if context is None:
        msg = "OAuth provider context is unavailable."
        raise RuntimeError(msg)

    if not getattr(oauth, "_initialized", False) and hasattr(
        oauth, "_initialize"
    ):
        await oauth._initialize()

    if oauth_context_access_token(oauth):
        is_token_valid = getattr(context, "is_token_valid", None)
        if callable(is_token_valid) and is_token_valid():
            return oauth_context_access_token(oauth)

    httpx_client_factory = getattr(oauth, "httpx_client_factory", None)
    if httpx_client_factory is None:
        msg = "OAuth provider does not expose an HTTP client factory."
        raise RuntimeError(msg)

    async with httpx_client_factory() as client:
        is_token_valid = getattr(context, "is_token_valid", None)
        can_refresh_token = getattr(context, "can_refresh_token", None)
        if (
            callable(is_token_valid)
            and callable(can_refresh_token)
            and not is_token_valid()
            and can_refresh_token()
        ):
            refresh_request = await oauth._refresh_token()
            refresh_response = await client.send(refresh_request)
            if await oauth._handle_refresh_response(refresh_response):
                access_token = oauth_context_access_token(oauth)
                if access_token:
                    return access_token
            else:
                oauth._initialized = False

        prm_urls = build_protected_resource_metadata_discovery_urls(None, url)
        for discovery_url in prm_urls:
            response = await client.send(
                create_oauth_metadata_request(discovery_url)
            )
            prm = await handle_protected_resource_response(response)
            if prm is None:
                continue
            context.protected_resource_metadata = prm
            if prm.authorization_servers:
                context.auth_server_url = str(prm.authorization_servers[0])
            break

        asm_urls = build_oauth_authorization_server_metadata_discovery_urls(
            context.auth_server_url,
            url,
        )
        for discovery_url in asm_urls:
            response = await client.send(
                create_oauth_metadata_request(discovery_url)
            )
            ok, asm = await handle_auth_metadata_response(response)
            if not ok:
                break
            if asm is None:
                continue
            context.oauth_metadata = asm
            break

        selected_scope = get_client_metadata_scopes(
            None,
            context.protected_resource_metadata,
            context.oauth_metadata,
        )
        existing_scope = getattr(context.client_metadata, "scope", None)
        if selected_scope is not None and not (
            isinstance(existing_scope, str) and existing_scope.strip()
        ):
            context.client_metadata.scope = selected_scope

        if not context.client_info:
            if should_use_client_metadata_url(
                context.oauth_metadata,
                context.client_metadata_url,
            ):
                client_information = create_client_info_from_metadata_url(
                    context.client_metadata_url,
                    redirect_uris=context.client_metadata.redirect_uris,
                )
            else:
                registration_request = create_client_registration_request(
                    context.oauth_metadata,
                    context.client_metadata,
                    context.get_authorization_base_url(context.server_url),
                )
                registration_response = await client.send(registration_request)
                client_information = await handle_registration_response(
                    registration_response
                )
            context.client_info = client_information
            await context.storage.set_client_info(client_information)

        try:
            token_request = await oauth._perform_authorization()
        except ClientNotFoundError:
            if getattr(oauth, "_static_client_info", None) is not None:
                msg = (
                    "OAuth server rejected the static client credentials. "
                    "Verify that the client_id (and client_secret, if "
                    "provided) are correct and that the client is "
                    "registered with the server."
                )
                raise ClientNotFoundError(msg) from None
            if not retry_on_stale_client:
                raise
            oauth._initialized = False
            context.current_tokens = None
            context.client_info = None
            await oauth.token_storage_adapter.clear()
            return await oauth_login_access_token_proactive(
                oauth=oauth,
                url=url,
                retry_on_stale_client=False,
            )
        token_response = await client.send(token_request)
        await oauth._handle_token_response(token_response)

    access_token = oauth_context_access_token(oauth)
    if access_token:
        return access_token

    msg = (
        "OAuth login completed but no access token was returned by the "
        "upstream."
    )
    raise RuntimeError(msg)


async def oauth_login_access_token(
    *,
    url: str,
    resolve_callback_url_headless: Any,
    auth_flow_once: Any | None = None,
    proactive_access_token: Any | None = None,
    headless: bool = False,
    token_storage: Any | None = None,
    auth_config: dict[str, Any] | None = None,
) -> str:
    """Run OAuth flow for an HTTP upstream and return an access token."""
    from fastmcp import Client
    from fastmcp.client.auth import OAuth
    from fastmcp.client.auth.oauth import ClientNotFoundError
    from fastmcp.client.transports import (
        ClientTransport,
        SSETransport,
        StreamableHttpTransport,
    )
    from fastmcp.mcp_config import infer_transport_type_from_url

    callback_port = oauth_callback_port(auth_config)
    flow_once = (
        auth_flow_once
        if auth_flow_once is not None
        else oauth_async_auth_flow_once
    )

    class _ConfiguredOAuth(OAuth):
        async def async_auth_flow(
            self, request: Any
        ) -> AsyncGenerator[Any, Any]:
            explicit_scope = getattr(self, "_sift_explicit_scope", None)
            try:
                async with contextlib.aclosing(
                    flow_once(
                        oauth=self,
                        request=request,
                        explicit_scope=explicit_scope,
                    )
                ) as gen:
                    response = None
                    while True:
                        try:
                            yielded_request = await gen.asend(response)
                            response = yield yielded_request
                        except StopAsyncIteration:
                            break
            except ClientNotFoundError:
                if getattr(self, "_static_client_info", None) is not None:
                    msg = (
                        "OAuth server rejected the static client "
                        "credentials. Verify that the client_id "
                        "(and client_secret, if provided) are correct "
                        "and that the client is registered with the "
                        "server."
                    )
                    raise ClientNotFoundError(msg) from None

                self._initialized = False
                context = getattr(self, "context", None)
                if context is not None:
                    context.current_tokens = None
                    context.client_info = None
                await self.token_storage_adapter.clear()

                async with contextlib.aclosing(
                    flow_once(
                        oauth=self,
                        request=request,
                        explicit_scope=explicit_scope,
                    )
                ) as gen:
                    response = None
                    while True:
                        try:
                            yielded_request = await gen.asend(response)
                            response = yield yielded_request
                        except StopAsyncIteration:
                            break

    def _create_oauth(oauth_url: str) -> OAuth:
        return _ConfiguredOAuth(
            oauth_url,
            token_storage=token_storage,
            callback_port=callback_port,
        )

    class _HeadlessOAuth(_ConfiguredOAuth):
        """OAuth provider variant that avoids browser interaction."""

        def __init__(
            self,
            mcp_url: str,
            *,
            token_storage: Any | None = None,
            callback_port: int | None = None,
        ):
            self._callback_url: str | None = None
            super().__init__(
                mcp_url,
                token_storage=token_storage,
                callback_port=callback_port,
            )

        async def redirect_handler(self, authorization_url: str) -> None:
            self._callback_url = await resolve_callback_url_headless(
                authorization_url=authorization_url,
                callback_port=self.redirect_port,
            )

        async def callback_handler(self) -> tuple[str, str | None]:
            callback_url = self._callback_url
            if callback_url is None:
                msg = (
                    "No authorization response received. OAuth redirect did "
                    "not start."
                )
                raise RuntimeError(msg)
            params = parse_qs(urlparse(callback_url).query)
            if "error" in params:
                error = params.get("error", ["unknown_error"])[0]
                error_desc = params.get("error_description", ["Unknown error"])[
                    0
                ]
                msg = f"OAuth authorization failed: {error} - {error_desc}"
                raise RuntimeError(msg)

            auth_code = params.get("code", [None])[0]
            if not isinstance(auth_code, str) or not auth_code:
                msg = "OAuth authorization failed: missing authorization code"
                raise RuntimeError(msg)
            state = params.get("state", [None])[0]
            return auth_code, state if isinstance(state, str) else None

    oauth = (
        _HeadlessOAuth(
            url,
            token_storage=token_storage,
            callback_port=callback_port,
        )
        if headless
        else _create_oauth(url)
    )
    await oauth_apply_client_config(oauth=oauth, auth_config=auth_config)
    inferred = infer_transport_type_from_url(url)
    transport: ClientTransport
    if inferred == "sse":
        transport = SSETransport(url=url, auth=oauth)
    else:
        transport = StreamableHttpTransport(url=url, auth=oauth)

    async with Client(transport, timeout=30.0) as client:
        await client.list_tools()

    access_token = oauth_context_access_token(oauth)
    context = getattr(oauth, "context", None)
    is_token_valid = (
        getattr(context, "is_token_valid", None)
        if context is not None
        else None
    )
    if access_token and (not callable(is_token_valid) or is_token_valid()):
        return access_token

    proactive_login = (
        proactive_access_token
        if proactive_access_token is not None
        else oauth_login_access_token_proactive
    )
    return await proactive_login(oauth=oauth, url=url)
