from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from sift_mcp.codegen.ast_guard import ALLOWED_IMPORT_ROOTS
from sift_mcp.config.package_install import (
    _filter_shared_roots,
    _load_config,
    _resolve_import_roots,
    _update_allowlist,
    _update_allowlist_by_roots,
    _write_config,
    install_packages,
    uninstall_packages,
)
from sift_mcp.constants import CONFIG_FILENAME, STATE_SUBDIR


def _cfg_path(data_dir: Path) -> Path:
    return data_dir / STATE_SUBDIR / CONFIG_FILENAME


def _write_cfg(data_dir: Path, cfg: dict) -> None:
    path = _cfg_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cfg), encoding="utf-8")


def _read_cfg(data_dir: Path) -> dict:
    return json.loads(
        _cfg_path(data_dir).read_text(encoding="utf-8")
    )


# ---- _load_config / _write_config ----


def test_load_config_missing_file(tmp_path: Path) -> None:
    result = _load_config(tmp_path / "nonexistent.json")
    assert result == {}


def test_load_config_non_dict(tmp_path: Path) -> None:
    path = tmp_path / "bad.json"
    path.write_text("[]", encoding="utf-8")
    assert _load_config(path) == {}


def test_write_config_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "config.json"
    data = {"key": "value", "num": 42}
    _write_config(path, data)
    assert json.loads(path.read_text(encoding="utf-8")) == data


# ---- _update_allowlist ----


def test_add_new_package_materialises_defaults(tmp_path: Path) -> None:
    """Adding a non-default package writes full defaults + new entry."""
    _write_cfg(tmp_path, {})
    _update_allowlist(tmp_path, ["scipy"], add=True)

    cfg = _read_cfg(tmp_path)
    roots = cfg["code_query_allowed_import_roots"]
    assert "scipy" in roots
    for default_root in ALLOWED_IMPORT_ROOTS:
        assert default_root in roots


def test_add_default_package_is_noop(tmp_path: Path) -> None:
    """Adding a package already in defaults doesn't write config."""
    _write_cfg(tmp_path, {})
    _update_allowlist(tmp_path, ["json"], add=True)

    cfg = _read_cfg(tmp_path)
    assert "code_query_allowed_import_roots" not in cfg


def test_add_to_explicit_list_appends(tmp_path: Path) -> None:
    """When config has explicit list, new entry is appended."""
    _write_cfg(
        tmp_path,
        {"code_query_allowed_import_roots": ["json", "os"]},
    )
    _update_allowlist(tmp_path, ["scipy"], add=True)

    roots = _read_cfg(tmp_path)[
        "code_query_allowed_import_roots"
    ]
    assert roots == ["json", "os", "scipy"]


def test_add_duplicate_to_explicit_list_is_noop(
    tmp_path: Path,
) -> None:
    """Adding a package already in explicit list doesn't change it."""
    _write_cfg(
        tmp_path,
        {"code_query_allowed_import_roots": ["json", "scipy"]},
    )
    _update_allowlist(tmp_path, ["scipy"], add=True)

    roots = _read_cfg(tmp_path)[
        "code_query_allowed_import_roots"
    ]
    assert roots == ["json", "scipy"]


def test_remove_from_explicit_list(tmp_path: Path) -> None:
    _write_cfg(
        tmp_path,
        {
            "code_query_allowed_import_roots": [
                "json",
                "scipy",
                "os",
            ]
        },
    )
    _update_allowlist(tmp_path, ["scipy"], add=False)

    roots = _read_cfg(tmp_path)[
        "code_query_allowed_import_roots"
    ]
    assert roots == ["json", "os"]


def test_remove_from_null_list_is_noop(tmp_path: Path) -> None:
    """Removing from null (defaults) config doesn't write anything."""
    _write_cfg(tmp_path, {})
    _update_allowlist(tmp_path, ["scipy"], add=False)

    cfg = _read_cfg(tmp_path)
    assert "code_query_allowed_import_roots" not in cfg


def test_remove_missing_config_file_is_noop(
    tmp_path: Path,
) -> None:
    """Removing when config doesn't exist does nothing."""
    _update_allowlist(tmp_path, ["scipy"], add=False)
    assert not _cfg_path(tmp_path).exists()


def test_add_creates_state_dir_if_missing(tmp_path: Path) -> None:
    """Adding a package creates the state dir if needed."""
    _update_allowlist(tmp_path, ["scipy"], add=True)
    assert _cfg_path(tmp_path).exists()


def test_package_name_extras_stripped(tmp_path: Path) -> None:
    """Package names like 'scipy[all]' use root 'scipy'."""
    _write_cfg(tmp_path, {})
    _update_allowlist(tmp_path, ["scipy[all]"], add=True)

    cfg = _read_cfg(tmp_path)
    roots = cfg["code_query_allowed_import_roots"]
    assert "scipy" in roots
    assert "scipy[all]" not in roots


# ---- _resolve_import_roots ----


def test_resolve_import_roots_uses_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Distribution names differing from import roots are resolved."""
    fake_map = {
        "sklearn": ["scikit_learn", "scikit-learn"],
        "PIL": ["pillow"],
    }
    monkeypatch.setattr(
        "sift_mcp.config.package_install.packages_distributions",
        lambda: fake_map,
    )
    roots = _resolve_import_roots(["scikit-learn", "Pillow"])
    assert "sklearn" in roots
    assert "PIL" in roots


def test_resolve_import_roots_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Falls back to normalised name when metadata unavailable."""
    monkeypatch.setattr(
        "sift_mcp.config.package_install.packages_distributions",
        dict,
    )
    roots = _resolve_import_roots(["my-pkg>=1.0"])
    assert roots == ["my_pkg"]


def test_resolve_import_roots_strips_extras(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Extras brackets are stripped before resolution."""
    monkeypatch.setattr(
        "sift_mcp.config.package_install.packages_distributions",
        dict,
    )
    roots = _resolve_import_roots(["scipy[all]"])
    assert roots == ["scipy"]


def test_resolve_import_roots_hyphenated_dist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hyphenated distribution names match metadata correctly."""
    fake_map = {
        "dateutil": ["python-dateutil"],
    }
    monkeypatch.setattr(
        "sift_mcp.config.package_install.packages_distributions",
        lambda: fake_map,
    )
    roots = _resolve_import_roots(["python-dateutil"])
    assert roots == ["dateutil"]


def test_resolve_import_roots_fallback_normalises_hyphens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fallback normalises hyphens so allowlist entries pass isidentifier."""
    monkeypatch.setattr(
        "sift_mcp.config.package_install.packages_distributions",
        dict,
    )
    roots = _resolve_import_roots(["charset-normalizer"])
    assert roots == ["charset_normalizer"]
    assert all(r.isidentifier() for r in roots)


# ---- install_packages / uninstall_packages ----


def _mock_pip_success(*_args: object, **_kwargs: object) -> object:
    """Simulate successful pip run."""
    return type("Result", (), {"returncode": 0})()


def _mock_pip_failure(*_args: object, **_kwargs: object) -> object:
    """Simulate failed pip run."""
    return type("Result", (), {"returncode": 1})()


def test_install_calls_pip_and_updates_allowlist(
    tmp_path: Path,
) -> None:
    _write_cfg(tmp_path, {})

    with patch(
        "sift_mcp.config.package_install.subprocess.run",
        _mock_pip_success,
    ):
        rc = install_packages(
            ["scipy"], data_dir=tmp_path
        )

    assert rc == 0
    cfg = _read_cfg(tmp_path)
    assert "scipy" in cfg["code_query_allowed_import_roots"]


def test_install_pip_failure_returns_nonzero(
    tmp_path: Path,
) -> None:
    _write_cfg(tmp_path, {})

    with patch(
        "sift_mcp.config.package_install.subprocess.run",
        _mock_pip_failure,
    ):
        rc = install_packages(
            ["scipy"], data_dir=tmp_path
        )

    assert rc == 1
    cfg = _read_cfg(tmp_path)
    assert "code_query_allowed_import_roots" not in cfg


def test_install_no_data_dir_skips_allowlist() -> None:
    with patch(
        "sift_mcp.config.package_install.subprocess.run",
        _mock_pip_success,
    ):
        rc = install_packages(["scipy"], data_dir=None)

    assert rc == 0


def test_uninstall_calls_pip_and_updates_allowlist(
    tmp_path: Path,
) -> None:
    _write_cfg(
        tmp_path,
        {
            "code_query_allowed_import_roots": [
                "json",
                "scipy",
            ]
        },
    )

    with patch(
        "sift_mcp.config.package_install.subprocess.run",
        _mock_pip_success,
    ):
        rc = uninstall_packages(
            ["scipy"], data_dir=tmp_path
        )

    assert rc == 0
    roots = _read_cfg(tmp_path)[
        "code_query_allowed_import_roots"
    ]
    assert "scipy" not in roots
    assert "json" in roots


def test_uninstall_resolves_roots_before_pip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Import roots are resolved before pip removes metadata."""
    _write_cfg(
        tmp_path,
        {"code_query_allowed_import_roots": ["json", "dateutil"]},
    )
    # Simulate metadata available before pip uninstall:
    # python-dateutil distribution maps to 'dateutil' import root.
    fake_map = {"dateutil": ["python-dateutil"]}
    monkeypatch.setattr(
        "sift_mcp.config.package_install.packages_distributions",
        lambda: fake_map,
    )

    with patch(
        "sift_mcp.config.package_install.subprocess.run",
        _mock_pip_success,
    ):
        rc = uninstall_packages(
            ["python-dateutil"], data_dir=tmp_path
        )

    assert rc == 0
    roots = _read_cfg(tmp_path)[
        "code_query_allowed_import_roots"
    ]
    assert "dateutil" not in roots
    assert "json" in roots


def test_update_allowlist_by_roots_removes(
    tmp_path: Path,
) -> None:
    """Pre-resolved roots are removed from explicit allowlist."""
    _write_cfg(
        tmp_path,
        {
            "code_query_allowed_import_roots": [
                "json",
                "dateutil",
                "os",
            ]
        },
    )
    _update_allowlist_by_roots(tmp_path, ["dateutil"])

    roots = _read_cfg(tmp_path)[
        "code_query_allowed_import_roots"
    ]
    assert roots == ["json", "os"]


def test_update_allowlist_by_roots_noop_on_null(
    tmp_path: Path,
) -> None:
    """No-op when config has no explicit allowlist."""
    _write_cfg(tmp_path, {})
    _update_allowlist_by_roots(tmp_path, ["dateutil"])

    cfg = _read_cfg(tmp_path)
    assert "code_query_allowed_import_roots" not in cfg


# ---- _filter_shared_roots ----


def test_filter_shared_roots_preserves_shared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Root shared with another distribution is kept."""
    fake_map = {
        "google": [
            "google-cloud-storage",
            "google-auth",
        ],
    }
    monkeypatch.setattr(
        "sift_mcp.config.package_install.packages_distributions",
        lambda: fake_map,
    )
    safe = _filter_shared_roots(
        ["google-cloud-storage"], ["google"]
    )
    assert safe == []


def test_filter_shared_roots_removes_exclusive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Root exclusively owned by uninstalled package is removed."""
    fake_map = {
        "dateutil": ["python-dateutil"],
    }
    monkeypatch.setattr(
        "sift_mcp.config.package_install.packages_distributions",
        lambda: fake_map,
    )
    safe = _filter_shared_roots(
        ["python-dateutil"], ["dateutil"]
    )
    assert safe == ["dateutil"]


def test_uninstall_preserves_shared_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Uninstalling one google-* pkg keeps 'google' in allowlist."""
    _write_cfg(
        tmp_path,
        {
            "code_query_allowed_import_roots": [
                "json",
                "google",
            ]
        },
    )
    fake_map = {
        "google": [
            "google-cloud-storage",
            "google-auth",
        ],
    }
    monkeypatch.setattr(
        "sift_mcp.config.package_install.packages_distributions",
        lambda: fake_map,
    )

    with patch(
        "sift_mcp.config.package_install.subprocess.run",
        _mock_pip_success,
    ):
        rc = uninstall_packages(
            ["google-cloud-storage"], data_dir=tmp_path
        )

    assert rc == 0
    roots = _read_cfg(tmp_path)[
        "code_query_allowed_import_roots"
    ]
    assert "google" in roots


def test_uninstall_pip_failure_returns_nonzero(
    tmp_path: Path,
) -> None:
    with patch(
        "sift_mcp.config.package_install.subprocess.run",
        _mock_pip_failure,
    ):
        rc = uninstall_packages(
            ["scipy"], data_dir=tmp_path
        )

    assert rc == 1
