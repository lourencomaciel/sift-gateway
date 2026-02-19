"""Tests for per-upstream secret file store."""

from __future__ import annotations

from pathlib import Path
import stat

import pytest

from sift_gateway.config.upstream_secrets import (
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
