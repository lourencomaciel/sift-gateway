"""Tests for SQLite-backed upstream registry helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sift_gateway.config.upstream_registry import (
    bootstrap_registry_from_config,
    get_registry_upstream_record,
    load_registry_mcp_servers,
    load_registry_upstream_dicts,
    load_registry_upstream_records,
    merge_missing_registry_from_config,
    remove_registry_upstream,
    replace_registry_from_mcp_servers,
    set_registry_upstream_enabled,
    set_registry_upstream_secret_ref,
    upsert_registry_from_mcp_servers,
)
from sift_gateway.config.upstream_secrets import read_secret, write_secret


def _write_config(data_dir: Path, payload: dict[str, object]) -> None:
    state_dir = data_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )


def _read_config(data_dir: Path) -> dict[str, object]:
    return json.loads((data_dir / "state" / "config.json").read_text())


def test_bootstrap_registry_from_config_externalizes_inline_secrets(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "args": ["mcp"],
                    "env": {"GITHUB_TOKEN": "secret"},
                },
                "api": {
                    "url": "https://example.com/mcp",
                    "headers": {"Authorization": "Bearer tok"},
                },
            }
        },
    )

    changed = bootstrap_registry_from_config(tmp_path)

    assert changed == 2
    records = load_registry_upstream_records(tmp_path, include_disabled=True)
    assert {record["prefix"] for record in records} == {"github", "api"}
    assert {
        record["secret_ref"]
        for record in records
        if record["secret_ref"] is not None
    } == {"github", "api"}

    mirrored = _read_config(tmp_path)
    github = mirrored["mcpServers"]["github"]
    assert "env" not in github
    assert github["_gateway"]["secret_ref"] == "github"
    api = mirrored["mcpServers"]["api"]
    assert "headers" not in api
    assert api["_gateway"]["secret_ref"] == "api"

    gh_secret = read_secret(tmp_path, "github")
    assert gh_secret["env"] == {"GITHUB_TOKEN": "secret"}
    api_secret = read_secret(tmp_path, "api")
    assert api_secret["headers"] == {"Authorization": "Bearer tok"}


def test_bootstrap_registry_normalizes_secret_ref_json_suffix(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "_gateway": {"secret_ref": "github.json"},
                }
            }
        },
    )

    changed = bootstrap_registry_from_config(tmp_path)

    assert changed == 1
    row = get_registry_upstream_record(data_dir=tmp_path, prefix="github")
    assert row is not None
    assert row["secret_ref"] == "github"

    mirrored = _read_config(tmp_path)
    assert (
        mirrored["mcpServers"]["github"]["_gateway"]["secret_ref"] == "github"
    )
    assert not (
        tmp_path / "state" / "upstream_secrets" / "github.json.json"
    ).exists()


def test_bootstrap_registry_rejects_inline_secret_conflict_with_secret_ref(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "env": {"GITHUB_TOKEN": "inline"},
                    "_gateway": {"secret_ref": "shared"},
                }
            }
        },
    )
    write_secret(
        tmp_path,
        "shared",
        transport="stdio",
        env={"GITHUB_TOKEN": "original"},
    )

    with pytest.raises(
        ValueError,
        match="Cannot specify both inline env/headers and secret_ref",
    ):
        bootstrap_registry_from_config(tmp_path)

    secret = read_secret(tmp_path, "shared")
    assert secret["env"] == {"GITHUB_TOKEN": "original"}
    assert load_registry_upstream_records(tmp_path, include_disabled=True) == []


def test_bootstrap_registry_from_config_rejects_non_list_args(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "args": "--verbose",
                }
            }
        },
    )

    with pytest.raises(ValueError, match="args must be a JSON array"):
        bootstrap_registry_from_config(tmp_path)


def test_bootstrap_registry_from_config_rejects_non_object_env(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "env": "oops",
                }
            }
        },
    )

    with pytest.raises(ValueError, match="env must be a JSON object"):
        bootstrap_registry_from_config(tmp_path)


def test_bootstrap_registry_from_config_rejects_non_object_headers(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "api": {
                    "url": "https://example.com/mcp",
                    "headers": ["Authorization=Bearer tok"],
                }
            }
        },
    )

    with pytest.raises(ValueError, match="headers must be a JSON object"):
        bootstrap_registry_from_config(tmp_path)


def test_bootstrap_registry_from_config_rejects_non_string_secret_ref(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "_gateway": {"secret_ref": 42},
                }
            }
        },
    )

    with pytest.raises(ValueError, match=r"_gateway\.secret_ref"):
        bootstrap_registry_from_config(tmp_path)


def test_bootstrap_registry_allows_null_secret_ref(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "_gateway": {"secret_ref": None},
                }
            }
        },
    )

    changed = bootstrap_registry_from_config(tmp_path)

    assert changed == 1
    row = get_registry_upstream_record(data_dir=tmp_path, prefix="github")
    assert row is not None
    assert row["secret_ref"] is None
    mirrored = _read_config(tmp_path)
    assert "_gateway" not in mirrored["mcpServers"]["github"]


def test_bootstrap_registry_treats_null_secret_ref_as_unset(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "env": {"GITHUB_TOKEN": "secret"},
                    "_gateway": {"secret_ref": None},
                }
            }
        },
    )

    changed = bootstrap_registry_from_config(tmp_path)

    assert changed == 1
    row = get_registry_upstream_record(data_dir=tmp_path, prefix="github")
    assert row is not None
    assert row["secret_ref"] == "github"
    mirrored = _read_config(tmp_path)
    entry = mirrored["mcpServers"]["github"]
    assert "env" not in entry
    assert entry["_gateway"]["secret_ref"] == "github"
    secret = read_secret(tmp_path, "github")
    assert secret["env"] == {"GITHUB_TOKEN": "secret"}


def test_bootstrap_registry_from_config_rejects_non_object_server_entry(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {"command": "gh"},
                "bad": "oops",
            }
        },
    )

    with pytest.raises(
        ValueError,
        match="server 'bad' config must be a JSON object",
    ):
        bootstrap_registry_from_config(tmp_path)

    assert load_registry_upstream_records(tmp_path, include_disabled=True) == []
    # Config is unchanged because bootstrap failed before any mirror write.
    assert _read_config(tmp_path)["mcpServers"]["bad"] == "oops"


def test_bootstrap_registry_validation_failure_has_no_secret_side_effects(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "env": {"GITHUB_TOKEN": "secret"},
                },
                "bad": {
                    "command": "npx",
                    "args": "--not-a-list",
                },
            }
        },
    )

    with pytest.raises(ValueError, match="args must be a JSON array"):
        bootstrap_registry_from_config(tmp_path)

    assert load_registry_upstream_records(tmp_path, include_disabled=True) == []
    assert not (
        tmp_path / "state" / "upstream_secrets" / "github.json"
    ).exists()


def test_bootstrap_registry_secret_write_failure_rolls_back_registry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "env": {"GITHUB_TOKEN": "secret"},
                }
            }
        },
    )

    def _fail_write_secret(*_args: object, **_kwargs: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(
        "sift_gateway.config.upstream_registry.write_secret",
        _fail_write_secret,
    )

    with pytest.raises(OSError, match="disk full"):
        bootstrap_registry_from_config(tmp_path)

    assert load_registry_upstream_records(tmp_path, include_disabled=True) == []
    assert _read_config(tmp_path)["mcpServers"]["github"]["env"] == {
        "GITHUB_TOKEN": "secret"
    }
    assert not (
        tmp_path / "state" / "upstream_secrets" / "github.json"
    ).exists()


def test_bootstrap_registry_from_config_rejects_non_bool_enabled(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "_gateway": {"enabled": "false"},
                }
            }
        },
    )

    with pytest.raises(ValueError, match=r"_gateway\.enabled"):
        bootstrap_registry_from_config(tmp_path)


def test_bootstrap_registry_rejects_invalid_top_level_mcp_servers_shape(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path, {"mcpServers": []})

    with pytest.raises(ValueError, match="'mcpServers' must be a JSON object"):
        bootstrap_registry_from_config(tmp_path)

    assert load_registry_upstream_records(tmp_path, include_disabled=True) == []


def test_merge_missing_rejects_invalid_top_level_mcp_servers_shape_when_empty(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path, {"mcpServers": []})

    with pytest.raises(ValueError, match="'mcpServers' must be a JSON object"):
        merge_missing_registry_from_config(tmp_path)

    assert load_registry_upstream_records(tmp_path, include_disabled=True) == []


def test_merge_missing_skips_invalid_top_level_shape_when_registry_exists(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path, {"mcpServers": {"github": {"command": "gh"}}})
    replace_registry_from_mcp_servers(
        data_dir=tmp_path,
        servers={"github": {"command": "gh"}},
        source_kind="manual",
    )
    _write_config(tmp_path, {"mcpServers": []})

    changed = merge_missing_registry_from_config(tmp_path)

    assert changed == 0
    records = load_registry_upstream_records(tmp_path, include_disabled=True)
    assert [record["prefix"] for record in records] == ["github"]


def test_bootstrap_registry_skips_invalid_config_when_registry_exists(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {"mcpServers": {"github": {"command": "gh"}}},
    )
    replace_registry_from_mcp_servers(
        data_dir=tmp_path,
        servers={"github": {"command": "gh"}},
        source_kind="manual",
    )
    _write_config(
        tmp_path,
        {"mcpServers": {"github": {"command": "gh"}, "bad": "oops"}},
    )

    changed = bootstrap_registry_from_config(tmp_path)

    assert changed == 0
    records = load_registry_upstream_records(tmp_path, include_disabled=True)
    assert [record["prefix"] for record in records] == ["github"]


def test_merge_missing_skips_invalid_config_when_registry_exists(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {"mcpServers": {"github": {"command": "gh"}}},
    )
    replace_registry_from_mcp_servers(
        data_dir=tmp_path,
        servers={"github": {"command": "gh"}},
        source_kind="manual",
    )
    _write_config(
        tmp_path,
        {"mcpServers": {"github": {"command": "gh"}, "bad": "oops"}},
    )

    changed = merge_missing_registry_from_config(tmp_path)

    assert changed == 0
    records = load_registry_upstream_records(tmp_path, include_disabled=True)
    assert [record["prefix"] for record in records] == ["github"]


def test_merge_missing_skips_invalid_gateway_values_when_registry_exists(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {"mcpServers": {"github": {"command": "gh"}}},
    )
    replace_registry_from_mcp_servers(
        data_dir=tmp_path,
        servers={"github": {"command": "gh"}},
        source_kind="manual",
    )
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {"command": "gh"},
                "bad": {
                    "command": "npx",
                    "_gateway": {"enabled": "false"},
                },
            }
        },
    )

    changed = merge_missing_registry_from_config(tmp_path)

    assert changed == 0
    records = load_registry_upstream_records(tmp_path, include_disabled=True)
    assert [record["prefix"] for record in records] == ["github"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("passthrough_allowed", "nope"),
        ("inherit_parent_env", "yes"),
    ],
)
def test_bootstrap_registry_from_config_rejects_non_bool_gateway_fields(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "_gateway": {field: value},
                }
            }
        },
    )

    with pytest.raises(ValueError, match=rf"_gateway\.{field}"):
        bootstrap_registry_from_config(tmp_path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("semantic_salt_env_keys", "GITHUB_TOKEN"),
        ("semantic_salt_headers", "Authorization"),
    ],
)
def test_bootstrap_registry_from_config_rejects_non_list_semantic_salt_fields(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "_gateway": {field: value},
                }
            }
        },
    )

    with pytest.raises(ValueError, match=rf"_gateway\.{field}"):
        bootstrap_registry_from_config(tmp_path)


def test_bootstrap_registry_from_config_rejects_invalid_pagination_shape(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "_gateway": {"pagination": "bad"},
                }
            }
        },
    )

    with pytest.raises(ValueError, match=r"_gateway\.pagination"):
        bootstrap_registry_from_config(tmp_path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("auto_paginate_max_pages", True),
        ("auto_paginate_max_records", True),
        ("auto_paginate_timeout_seconds", True),
    ],
)
def test_bootstrap_registry_rejects_boolean_numeric_gateway_fields(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "_gateway": {field: value},
                }
            }
        },
    )

    with pytest.raises(ValueError, match=rf"_gateway\.{field}"):
        bootstrap_registry_from_config(tmp_path)


def test_bootstrap_registry_preserves_auto_pagination_fields(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {
                    "command": "gh",
                    "_gateway": {
                        "auto_paginate_max_pages": 7,
                        "auto_paginate_max_records": 250,
                        "auto_paginate_timeout_seconds": 12.5,
                    },
                }
            }
        },
    )

    changed = bootstrap_registry_from_config(tmp_path)
    assert changed == 1

    records = load_registry_upstream_records(tmp_path, include_disabled=True)
    assert len(records) == 1
    row = records[0]
    assert row["auto_paginate_max_pages"] == 7
    assert row["auto_paginate_max_records"] == 250
    assert row["auto_paginate_timeout_seconds"] == 12.5

    upstreams = load_registry_upstream_dicts(tmp_path, enabled_only=True)
    assert upstreams[0]["auto_paginate_max_pages"] == 7
    assert upstreams[0]["auto_paginate_max_records"] == 250
    assert upstreams[0]["auto_paginate_timeout_seconds"] == 12.5

    mirrored = _read_config(tmp_path)
    gw = mirrored["mcpServers"]["github"]["_gateway"]
    assert gw["auto_paginate_max_pages"] == 7
    assert gw["auto_paginate_max_records"] == 250
    assert gw["auto_paginate_timeout_seconds"] == 12.5


def test_merge_missing_registry_from_config_adds_new_only(
    tmp_path: Path,
) -> None:
    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {"command": "gh", "args": ["mcp"]},
            }
        },
    )
    assert bootstrap_registry_from_config(tmp_path) == 1

    _write_config(
        tmp_path,
        {
            "mcpServers": {
                "github": {"command": "gh", "args": ["mcp", "--verbose"]},
                "filesystem": {"command": "npx", "args": ["-y", "@mcp/fs"]},
            }
        },
    )

    changed = merge_missing_registry_from_config(tmp_path)
    assert changed == 1

    records = load_registry_upstream_records(tmp_path, include_disabled=True)
    assert {record["prefix"] for record in records} == {"github", "filesystem"}
    github = next(record for record in records if record["prefix"] == "github")
    # Existing rows are preserved during merge-missing.
    assert github["args"] == ["mcp"]


def test_load_registry_upstream_dicts_filters_disabled_rows(
    tmp_path: Path,
) -> None:
    replace_registry_from_mcp_servers(
        data_dir=tmp_path,
        servers={
            "enabled": {"command": "gh"},
            "disabled": {
                "command": "npx",
                "_gateway": {"enabled": False},
            },
        },
        source_kind="manual",
    )

    enabled_only = load_registry_upstream_dicts(tmp_path, enabled_only=True)
    assert [row["prefix"] for row in enabled_only] == ["enabled"]

    all_rows = load_registry_upstream_dicts(tmp_path, enabled_only=False)
    assert {row["prefix"] for row in all_rows} == {"enabled", "disabled"}


def test_registry_mutations_roundtrip(
    tmp_path: Path,
) -> None:
    replace_registry_from_mcp_servers(
        data_dir=tmp_path,
        servers={"github": {"command": "gh"}},
        source_kind="manual",
    )
    assert set_registry_upstream_enabled(
        data_dir=tmp_path,
        prefix="github",
        enabled=False,
    )
    row = get_registry_upstream_record(data_dir=tmp_path, prefix="github")
    assert row is not None
    assert row["enabled"] is False

    assert set_registry_upstream_secret_ref(
        data_dir=tmp_path,
        prefix="github",
        secret_ref="github",
    )
    row = get_registry_upstream_record(data_dir=tmp_path, prefix="github")
    assert row is not None
    assert row["secret_ref"] == "github"

    assert remove_registry_upstream(data_dir=tmp_path, prefix="github")
    assert (
        get_registry_upstream_record(data_dir=tmp_path, prefix="github") is None
    )
    assert load_registry_mcp_servers(tmp_path) == {}


def test_upsert_secret_write_failure_rolls_back_written_secret_files(
    tmp_path: Path, monkeypatch
) -> None:
    from sift_gateway.config import upstream_registry as registry_module

    real_write_secret = registry_module.write_secret

    def _fail_on_second_write(
        data_dir: str | Path,
        prefix: str,
        *,
        transport: str,
        env: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Path:
        if prefix == "beta":
            raise OSError("disk full")
        return real_write_secret(
            data_dir,
            prefix,
            transport=transport,
            env=env,
            headers=headers,
        )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_registry.write_secret",
        _fail_on_second_write,
    )

    with pytest.raises(OSError, match="disk full"):
        upsert_registry_from_mcp_servers(
            data_dir=tmp_path,
            servers={
                "alpha": {"command": "gh", "env": {"A_TOKEN": "one"}},
                "beta": {"command": "gh", "env": {"B_TOKEN": "two"}},
            },
            merge_missing=False,
            source_kind="manual",
        )

    assert load_registry_upstream_records(tmp_path, include_disabled=True) == []
    assert not (tmp_path / "state" / "upstream_secrets" / "alpha.json").exists()
    assert not (tmp_path / "state" / "upstream_secrets" / "beta.json").exists()


def test_replace_secret_write_failure_restores_existing_secret_file(
    tmp_path: Path, monkeypatch
) -> None:
    from sift_gateway.config import upstream_registry as registry_module

    write_secret(
        tmp_path,
        "alpha",
        transport="stdio",
        env={"LEGACY": "keep"},
    )

    real_write_secret = registry_module.write_secret

    def _fail_after_overwriting_alpha(
        data_dir: str | Path,
        prefix: str,
        *,
        transport: str,
        env: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Path:
        if prefix == "beta":
            raise OSError("disk full")
        return real_write_secret(
            data_dir,
            prefix,
            transport=transport,
            env=env,
            headers=headers,
        )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_registry.write_secret",
        _fail_after_overwriting_alpha,
    )

    with pytest.raises(OSError, match="disk full"):
        replace_registry_from_mcp_servers(
            data_dir=tmp_path,
            servers={
                "alpha": {"command": "gh", "env": {"NEW": "value"}},
                "beta": {"command": "gh", "env": {"B_TOKEN": "two"}},
            },
            source_kind="manual",
        )

    restored = read_secret(tmp_path, "alpha")
    assert restored["env"] == {"LEGACY": "keep"}
    assert not (tmp_path / "state" / "upstream_secrets" / "beta.json").exists()
