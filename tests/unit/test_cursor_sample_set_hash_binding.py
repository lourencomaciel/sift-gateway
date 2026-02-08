from mcp_artifact_gateway.cursor.sample_set_hash import compute_sample_set_hash


def test_cursor_sample_set_hash_binding() -> None:
    h1 = compute_sample_set_hash("$.items", [0, 1], "fp", "mapper_v1")
    h2 = compute_sample_set_hash("$.items", [0, 2], "fp", "mapper_v1")
    assert h1 != h2
