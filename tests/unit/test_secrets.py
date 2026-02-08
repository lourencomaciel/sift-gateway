from mcp_artifact_gateway.cursor.secrets import SecretStore, generate_secrets_file


def test_generate_and_load_secrets(tmp_path) -> None:
    path = tmp_path / "secrets.json"
    generate_secrets_file(path, num_secrets=2)

    store = SecretStore(path)
    cfg = store.load()

    assert cfg.cursor_ttl_minutes == 60
    assert len(cfg.active_secrets) == 2
    assert cfg.signing_secret_version == "v2"
    assert store.signing_secret().version == "v2"
    assert store.active_versions() == ["v1", "v2"]
