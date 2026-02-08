import json
from pathlib import Path

from mcp_artifact_gateway.config.settings import GatewayConfig


def test_config_json_precedence_over_defaults(tmp_path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    state_dir = data_dir / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "config.json").write_text(
        json.dumps({"postgres_pool_min": 5, "max_items": 123})
    )

    monkeypatch.delenv("MCP_GATEWAY_POSTGRES_POOL_MIN", raising=False)
    cfg = GatewayConfig(data_dir=data_dir)
    assert cfg.postgres_pool_min == 5
    assert cfg.max_items == 123


def test_env_overrides_config_json(tmp_path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    state_dir = data_dir / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "config.json").write_text(json.dumps({"postgres_pool_min": 5}))

    monkeypatch.setenv("MCP_GATEWAY_POSTGRES_POOL_MIN", "7")
    cfg = GatewayConfig(data_dir=data_dir)
    assert cfg.postgres_pool_min == 7


def test_env_data_dir_used_for_config_json(tmp_path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    state_dir = data_dir / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "config.json").write_text(json.dumps({"postgres_pool_min": 9}))

    monkeypatch.setenv("MCP_GATEWAY_DATA_DIR", str(data_dir))
    cfg = GatewayConfig()
    assert cfg.postgres_pool_min == 9
