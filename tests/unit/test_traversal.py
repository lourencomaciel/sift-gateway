from mcp_artifact_gateway.retrieval.traversal import iter_children, iter_sample_indices


def test_iter_children_dict_sorted() -> None:
    data = {"b": 1, "a": 2}
    keys = [key for key, _ in iter_children(data)]
    assert keys == ["a", "b"]


def test_iter_children_list_order() -> None:
    data = ["x", "y"]
    keys = [key for key, _ in iter_children(data)]
    assert keys == [0, 1]


def test_iter_sample_indices_sorted() -> None:
    indices = [3, 1, 2]
    assert list(iter_sample_indices(indices)) == [1, 2, 3]
