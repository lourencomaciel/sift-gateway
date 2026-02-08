import pytest

from mcp_artifact_gateway.cursor.sample_set_hash import compute_sample_set_hash
from mcp_artifact_gateway.util.hashing import sample_set_hash


def test_sample_set_hash_stable() -> None:
    indices = [0, 2, 5]
    h1 = compute_sample_set_hash("$.items", indices, "fp", "mapper_v1")
    h2 = sample_set_hash("$.items", indices, "fp", "mapper_v1")
    assert h1 == h2
    assert len(h1) == 32


def test_sample_set_hash_requires_sorted_unique() -> None:
    with pytest.raises(ValueError):
        compute_sample_set_hash("$.items", [2, 1], "fp", "mapper_v1")
    with pytest.raises(ValueError):
        sample_set_hash("$.items", [1, 1], "fp", "mapper_v1")
