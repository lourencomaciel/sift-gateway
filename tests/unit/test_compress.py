import pytest

from mcp_artifact_gateway.canon.compress import compress, decompress


@pytest.mark.parametrize("encoding", ["zstd", "gzip", "none"])
def test_roundtrip(encoding: str) -> None:
    data = b"hello world" * 100
    compressed, uncompressed_len = compress(data, encoding)
    assert uncompressed_len == len(data)
    result = decompress(compressed, encoding, expected_len=len(data))
    assert result == data


def test_decompress_length_mismatch() -> None:
    data = b"hello"
    compressed, _ = compress(data, "gzip")
    with pytest.raises(ValueError):
        decompress(compressed, "gzip", expected_len=len(data) + 1)


def test_invalid_encoding() -> None:
    with pytest.raises(ValueError):
        compress(b"x", "brotli")
    with pytest.raises(ValueError):
        decompress(b"x", "brotli")
