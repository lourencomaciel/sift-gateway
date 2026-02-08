from decimal import Decimal
import json

import pytest

from mcp_artifact_gateway.canon.decimal_json import dumps_safe, load_decimal, loads_decimal


def test_loads_decimal_types() -> None:
    data = loads_decimal('{"a":1.5,"b":2,"c":1e3}')
    assert isinstance(data["a"], Decimal)
    assert isinstance(data["b"], int)
    assert isinstance(data["c"], Decimal)
    assert data["c"] == Decimal("1E+3")


def test_load_decimal_from_file_like(tmp_path) -> None:
    path = tmp_path / "data.json"
    path.write_text('{"a":3.14}', encoding="utf-8")
    with path.open("r", encoding="utf-8") as fp:
        data = load_decimal(fp)
    assert isinstance(data["a"], Decimal)


def test_nan_and_infinity_rejected() -> None:
    with pytest.raises(ValueError):
        loads_decimal('{"a":NaN}')
    with pytest.raises(ValueError):
        loads_decimal('{"a":Infinity}')
    with pytest.raises(ValueError):
        loads_decimal('{"a":-Infinity}')


def test_dumps_safe_handles_decimal() -> None:
    payload = {"a": Decimal("2"), "b": Decimal("1.5")}
    encoded = dumps_safe(payload)
    parsed = json.loads(encoded)
    assert parsed["a"] == 2
    assert parsed["b"] == 1.5
