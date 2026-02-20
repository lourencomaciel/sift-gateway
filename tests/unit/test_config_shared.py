from __future__ import annotations

from pathlib import Path

from sift_gateway.config import shared


def test_resolve_sift_command_bare_name_ignores_local_directory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "sift-gateway").mkdir()

    monkeypatch.setattr(shared.sys, "argv", ["sift-gateway"])
    monkeypatch.setattr(shared.sys, "executable", str(tmp_path / "python"))
    monkeypatch.setattr(shared.shutil, "which", lambda _command: None)

    assert shared.resolve_sift_command() == "sift-gateway"


def test_resolve_sift_command_bare_name_prefers_path_lookup(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    local_dir = tmp_path / "sift-gateway"
    local_dir.mkdir()

    path_bin = tmp_path / "bin" / "sift-gateway"
    path_bin.parent.mkdir(parents=True, exist_ok=True)
    path_bin.write_text("#!/bin/sh\n", encoding="utf-8")

    def _which(command: str) -> str | None:
        if command == "sift-gateway":
            return str(path_bin)
        return None

    monkeypatch.setattr(shared.sys, "argv", ["sift-gateway"])
    monkeypatch.setattr(shared.sys, "executable", str(tmp_path / "python"))
    monkeypatch.setattr(shared.shutil, "which", _which)

    assert shared.resolve_sift_command() == str(path_bin.resolve())
    assert shared.resolve_sift_command() != str(local_dir.resolve())


def test_resolve_sift_command_accepts_explicit_relative_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    command_path = tmp_path / "tools" / "sift-gateway"
    command_path.parent.mkdir(parents=True, exist_ok=True)
    command_path.write_text("#!/bin/sh\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(shared.sys, "argv", ["./tools/sift-gateway"])
    monkeypatch.setattr(shared.sys, "executable", str(tmp_path / "python"))
    monkeypatch.setattr(shared.shutil, "which", lambda _command: None)

    assert shared.resolve_sift_command() == str(command_path.resolve())


def test_resolve_sift_command_preserves_path_symlink(
    monkeypatch,
    tmp_path: Path,
) -> None:
    path_bin = tmp_path / "bin" / "sift-gateway"
    real_bin = tmp_path / "cellar" / "sift-gateway" / "1.0.0" / "bin" / "sift-gateway"
    real_bin.parent.mkdir(parents=True, exist_ok=True)
    real_bin.write_text("#!/bin/sh\n", encoding="utf-8")
    path_bin.parent.mkdir(parents=True, exist_ok=True)
    path_bin.symlink_to(real_bin)

    monkeypatch.setattr(shared.sys, "argv", ["sift-gateway"])
    monkeypatch.setattr(shared.sys, "executable", str(tmp_path / "python"))
    monkeypatch.setattr(shared.shutil, "which", lambda _command: str(path_bin))

    assert shared.resolve_sift_command() == str(path_bin)
    assert shared.resolve_sift_command() != str(real_bin)
