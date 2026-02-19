from __future__ import annotations

from pathlib import Path

from sift_gateway.openclaw import read_asset, skill_text
from sift_gateway.openclaw.cli import cli


def test_skill_text_loads_packaged_asset() -> None:
    text = skill_text()
    assert text == read_asset("SKILL.md")
    assert "name: sift-gateway" in text
    assert "sift-gateway code" in text


def test_cli_prints_skill_to_stdout(capsys) -> None:
    exit_code = cli([])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "name: sift-gateway" in captured.out
    assert captured.err == ""


def test_cli_writes_skill_file(tmp_path: Path, capsys) -> None:
    output_path = tmp_path / "SKILL.md"
    exit_code = cli(["--output", str(output_path)])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert output_path.exists()
    assert "wrote OpenClaw skill" in captured.out


def test_cli_refuses_overwrite_without_force(tmp_path: Path, capsys) -> None:
    output_path = tmp_path / "SKILL.md"
    output_path.write_text("existing", encoding="utf-8")
    exit_code = cli(["--output", str(output_path)])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "refusing to overwrite existing file" in captured.err
