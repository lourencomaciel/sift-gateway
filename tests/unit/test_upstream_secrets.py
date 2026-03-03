"""Tests for per-upstream secret file store."""

from __future__ import annotations

import asyncio
from pathlib import Path
import stat

import pytest

from sift_gateway.config.upstream_secrets import (
    clear_oauth_client_registration,
    mark_oauth_access_token_stale,
    oauth_cache_dir,
    oauth_cache_dir_path,
    oauth_client_info_cache_key,
    oauth_token_cache_key,
    oauth_token_storage,
    read_secret,
    resolve_secret_ref,
    secrets_dir,
    validate_no_secret_conflict,
    write_secret,
)


class TestSecretsDir:
    def test_secrets_dir_creates_directory(self, tmp_path: Path) -> None:
        result = secrets_dir(tmp_path)
        assert result.is_dir()
        assert result == tmp_path / "state" / "upstream_secrets"
        mode = stat.S_IMODE(result.stat().st_mode)
        assert mode == 0o700


class TestOauthCacheDir:
    def test_oauth_cache_dir_path(self, tmp_path: Path) -> None:
        result = oauth_cache_dir_path(tmp_path, "notion")
        assert result == tmp_path / "state" / "upstream_oauth" / "notion"

    def test_oauth_cache_dir_creates_directory(self, tmp_path: Path) -> None:
        result = oauth_cache_dir(tmp_path, "notion")
        assert result.is_dir()
        mode = stat.S_IMODE(result.stat().st_mode)
        assert mode == 0o700

    def test_oauth_token_storage_disables_ttl_for_token_collection(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        seen: dict[str, object] = {}

        class _FakeDiskStore:
            def __init__(self, *, directory: Path | str) -> None:
                seen["directory"] = Path(directory)
                seen["put"] = []
                seen["put_many"] = []

            async def get(
                self,
                key: str,
                *,
                collection: str | None = None,
            ) -> dict[str, object] | None:
                _ = (key, collection)
                return None

            async def ttl(
                self,
                key: str,
                *,
                collection: str | None = None,
            ) -> tuple[dict[str, object] | None, float | None]:
                _ = (key, collection)
                return None, None

            async def put(
                self,
                key: str,
                value: dict[str, object],
                *,
                collection: str | None = None,
                ttl: float | None = None,
            ) -> None:
                _ = (key, value)
                put_calls = seen["put"]
                assert isinstance(put_calls, list)
                put_calls.append((collection, ttl))

            async def delete(
                self,
                key: str,
                *,
                collection: str | None = None,
            ) -> bool:
                _ = (key, collection)
                return True

            async def get_many(
                self,
                keys: list[str],
                *,
                collection: str | None = None,
            ) -> list[dict[str, object] | None]:
                _ = (keys, collection)
                return []

            async def ttl_many(
                self,
                keys: list[str],
                *,
                collection: str | None = None,
            ) -> list[tuple[dict[str, object] | None, float | None]]:
                _ = (keys, collection)
                return []

            async def put_many(
                self,
                keys: list[str],
                values: list[dict[str, object]],
                *,
                collection: str | None = None,
                ttl: float | None = None,
            ) -> None:
                _ = (keys, values)
                put_many_calls = seen["put_many"]
                assert isinstance(put_many_calls, list)
                put_many_calls.append((collection, ttl))

            async def delete_many(
                self,
                keys: list[str],
                *,
                collection: str | None = None,
            ) -> int:
                _ = (keys, collection)
                return 0

        monkeypatch.setattr(
            "key_value.aio.stores.disk.DiskStore",
            _FakeDiskStore,
        )

        storage = oauth_token_storage(tmp_path, "notion")
        assert seen["directory"] == (
            tmp_path / "state" / "upstream_oauth" / "notion"
        )

        asyncio.run(
            storage.put(
                key="token",
                value={"v": "x"},
                collection="mcp-oauth-token",
                ttl=30.0,
            )
        )
        asyncio.run(
            storage.put(
                key="client",
                value={"v": "x"},
                collection="mcp-oauth-client-info",
                ttl=30.0,
            )
        )
        asyncio.run(
            storage.put_many(
                keys=["token"],
                values=[{"v": "x"}],
                collection="mcp-oauth-token",
                ttl=30.0,
            )
        )
        asyncio.run(
            storage.put_many(
                keys=["client"],
                values=[{"v": "x"}],
                collection="mcp-oauth-client-info",
                ttl=30.0,
            )
        )

        assert seen["put"] == [
            ("mcp-oauth-token", None),
            ("mcp-oauth-client-info", 30.0),
        ]
        assert seen["put_many"] == [
            ("mcp-oauth-token", None),
            ("mcp-oauth-client-info", 30.0),
        ]

    def test_oauth_token_cache_key_normalizes_trailing_slash(self) -> None:
        assert (
            oauth_token_cache_key("https://api.example.test/mcp/")
            == "https://api.example.test/mcp/tokens"
        )

    def test_oauth_client_info_cache_key_normalizes_trailing_slash(
        self,
    ) -> None:
        assert (
            oauth_client_info_cache_key("https://api.example.test/mcp/")
            == "https://api.example.test/mcp/client_info"
        )

    def test_mark_oauth_access_token_stale_updates_refresh_capable_token(
        self,
    ) -> None:
        class _FakeStore:
            def __init__(self) -> None:
                self.values: dict[tuple[str, str | None], dict[str, object]] = {
                    (
                        "https://api.example.test/mcp/tokens",
                        "mcp-oauth-token",
                    ): {
                        "access_token": "old-access",
                        "refresh_token": "refresh-1",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                    }
                }

            async def get(
                self,
                key: str,
                *,
                collection: str | None = None,
            ) -> dict[str, object] | None:
                value = self.values.get((key, collection))
                return dict(value) if isinstance(value, dict) else None

            async def put(
                self,
                key: str,
                value: dict[str, object],
                *,
                collection: str | None = None,
                ttl: float | None = None,
            ) -> None:
                _ = ttl
                self.values[(key, collection)] = dict(value)

        store = _FakeStore()
        changed = asyncio.run(
            mark_oauth_access_token_stale(
                store,  # type: ignore[arg-type]
                server_url="https://api.example.test/mcp",
            )
        )
        assert changed is True
        updated = store.values[
            ("https://api.example.test/mcp/tokens", "mcp-oauth-token")
        ]
        assert updated["access_token"] == ""
        assert updated["refresh_token"] == "refresh-1"
        assert updated["expires_in"] == 0

    def test_mark_oauth_access_token_stale_noop_without_refresh_token(
        self,
    ) -> None:
        class _FakeStore:
            async def get(
                self,
                key: str,
                *,
                collection: str | None = None,
            ) -> dict[str, object] | None:
                _ = (key, collection)
                return {
                    "access_token": "old-access",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                }

            async def put(
                self,
                key: str,
                value: dict[str, object],
                *,
                collection: str | None = None,
                ttl: float | None = None,
            ) -> None:
                _ = (key, value, collection, ttl)
                raise AssertionError("put should not be called")

        changed = asyncio.run(
            mark_oauth_access_token_stale(
                _FakeStore(),  # type: ignore[arg-type]
                server_url="https://api.example.test/mcp",
            )
        )
        assert changed is False

    def test_clear_oauth_client_registration_deletes_client_info_key(
        self,
    ) -> None:
        seen: dict[str, object] = {}

        class _FakeStore:
            async def delete(
                self,
                key: str,
                *,
                collection: str | None = None,
            ) -> bool:
                seen["key"] = key
                seen["collection"] = collection
                return True

        deleted = asyncio.run(
            clear_oauth_client_registration(
                _FakeStore(),  # type: ignore[arg-type]
                server_url="https://api.example.test/mcp",
            )
        )
        assert deleted is True
        assert seen["key"] == "https://api.example.test/mcp/client_info"
        assert seen["collection"] == "mcp-oauth-client-info"


class TestWriteAndReadRoundtrip:
    def test_write_and_read_roundtrip_stdio(self, tmp_path: Path) -> None:
        env = {"GITHUB_TOKEN": "ghp_secret123"}
        path = write_secret(
            tmp_path,
            "github",
            transport="stdio",
            env=env,
        )
        assert path.exists()

        data = read_secret(tmp_path, "github")
        assert data["version"] == 1
        assert data["transport"] == "stdio"
        assert data["env"] == {"GITHUB_TOKEN": "ghp_secret123"}
        assert data["headers"] is None
        assert "updated_at" in data

    def test_write_and_read_roundtrip_http(self, tmp_path: Path) -> None:
        headers = {"Authorization": "Bearer tok_abc"}
        path = write_secret(
            tmp_path,
            "myapi",
            transport="http",
            headers=headers,
        )
        assert path.exists()

        data = read_secret(tmp_path, "myapi")
        assert data["version"] == 1
        assert data["transport"] == "http"
        assert data["env"] is None
        assert data["headers"] == {"Authorization": "Bearer tok_abc"}

    def test_write_and_read_roundtrip_http_with_oauth(
        self, tmp_path: Path
    ) -> None:
        path = write_secret(
            tmp_path,
            "myapi",
            transport="http",
            headers={"Authorization": "Bearer tok_abc"},
            oauth={
                "enabled": True,
                "provider": "fastmcp",
                "token_storage": "disk",
            },
        )
        assert path.exists()

        data = read_secret(tmp_path, "myapi")
        assert data["oauth"] == {
            "enabled": True,
            "provider": "fastmcp",
            "token_storage": "disk",
        }

    def test_write_secret_sets_file_permissions(self, tmp_path: Path) -> None:
        path = write_secret(
            tmp_path,
            "secure",
            transport="stdio",
            env={"KEY": "val"},
        )
        mode = stat.S_IMODE(path.stat().st_mode)
        assert mode == 0o600

    def test_write_overwrites_existing(self, tmp_path: Path) -> None:
        write_secret(
            tmp_path,
            "github",
            transport="stdio",
            env={"OLD": "value"},
        )
        write_secret(
            tmp_path,
            "github",
            transport="stdio",
            env={"NEW": "value"},
        )
        data = read_secret(tmp_path, "github")
        assert data["env"] == {"NEW": "value"}


class TestReadErrors:
    def test_read_missing_secret_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="No secret file found"):
            read_secret(tmp_path, "nonexistent")

    def test_read_invalid_json_raises(self, tmp_path: Path) -> None:
        sdir = secrets_dir(tmp_path)
        bad_file = sdir / "broken.json"
        bad_file.write_text("not valid json {{{", encoding="utf-8")

        with pytest.raises(ValueError, match="Invalid JSON"):
            read_secret(tmp_path, "broken")

    def test_read_non_object_json_raises(self, tmp_path: Path) -> None:
        sdir = secrets_dir(tmp_path)
        bad_file = sdir / "broken.json"
        bad_file.write_text("[]", encoding="utf-8")

        with pytest.raises(ValueError, match="must contain a JSON object"):
            read_secret(tmp_path, "broken")

    def test_read_missing_required_keys_raises(self, tmp_path: Path) -> None:
        sdir = secrets_dir(tmp_path)
        bad_file = sdir / "broken.json"
        bad_file.write_text('{"version": 1}', encoding="utf-8")

        with pytest.raises(ValueError, match="missing required keys"):
            read_secret(tmp_path, "broken")


class TestPrefixValidation:
    def test_prefix_with_path_separator_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="path separators"):
            write_secret(
                tmp_path,
                "foo/bar",
                transport="stdio",
            )

    def test_prefix_with_dotdot_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="must not contain"):
            write_secret(tmp_path, "..", transport="stdio")

    def test_write_secret_invalid_transport_raises(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(ValueError, match="Invalid transport"):
            write_secret(
                tmp_path,
                "github",
                transport="grpc",
            )


class TestResolveSecretRef:
    def test_resolve_secret_ref_happy_path(self, tmp_path: Path) -> None:
        write_secret(
            tmp_path,
            "github",
            transport="stdio",
            env={"TOKEN": "abc"},
        )
        data = resolve_secret_ref(tmp_path, "github")
        assert data["transport"] == "stdio"
        assert data["env"] == {"TOKEN": "abc"}

    def test_resolve_secret_ref_strips_json_suffix(
        self, tmp_path: Path
    ) -> None:
        write_secret(
            tmp_path,
            "github",
            transport="stdio",
            env={"TOKEN": "abc"},
        )
        data = resolve_secret_ref(tmp_path, "github.json")
        assert data["transport"] == "stdio"
        assert data["env"] == {"TOKEN": "abc"}

    def test_resolve_secret_ref_traversal_rejected(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(ValueError, match="must not contain"):
            resolve_secret_ref(tmp_path, "../../etc/passwd")

    def test_resolve_secret_ref_absolute_path_rejected(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(ValueError, match="must not be an absolute path"):
            resolve_secret_ref(tmp_path, "/etc/passwd")


def test_write_secret_cleanup_on_write_failure(
    tmp_path: Path, monkeypatch
) -> None:
    sdir = secrets_dir(tmp_path)

    def _fail_write(_fd: int, _content: bytes) -> int:
        raise OSError("simulated write failure")

    monkeypatch.setattr(
        "sift_gateway.config.upstream_secrets.os.write",
        _fail_write,
    )

    with pytest.raises(OSError, match="simulated write failure"):
        write_secret(
            tmp_path,
            "github",
            transport="stdio",
            env={"TOKEN": "abc"},
        )

    assert list(sdir.glob("*.tmp")) == []


class TestValidateNoSecretConflict:
    def test_validate_no_secret_conflict_both_raises(
        self,
    ) -> None:
        with pytest.raises(
            ValueError,
            match="Cannot specify both inline env/headers",
        ):
            validate_no_secret_conflict(
                config_env={"KEY": "val"},
                config_headers=None,
                secret_ref="github",
            )

    def test_validate_no_secret_conflict_headers_raises(
        self,
    ) -> None:
        with pytest.raises(
            ValueError,
            match="Cannot specify both inline env/headers",
        ):
            validate_no_secret_conflict(
                config_env=None,
                config_headers={"Auth": "Bearer tok"},
                secret_ref="github",
            )

    def test_validate_no_secret_conflict_ref_only_ok(
        self,
    ) -> None:
        # Should not raise
        validate_no_secret_conflict(
            config_env=None,
            config_headers=None,
            secret_ref="github",
        )

    def test_validate_no_secret_conflict_inline_only_ok(
        self,
    ) -> None:
        # Should not raise
        validate_no_secret_conflict(
            config_env={"KEY": "val"},
            config_headers={"Auth": "Bearer tok"},
            secret_ref=None,
        )

    def test_validate_no_secret_conflict_neither_ok(
        self,
    ) -> None:
        # Should not raise
        validate_no_secret_conflict(
            config_env=None,
            config_headers=None,
            secret_ref=None,
        )
