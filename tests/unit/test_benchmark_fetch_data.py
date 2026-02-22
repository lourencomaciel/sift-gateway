"""Unit tests for benchmark fetch_data module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch
import urllib.error

from benchmarks.tier1.datasets import DATASETS, Dataset
from benchmarks.tier1.fetch_data import fetch_all, fetch_dataset


def _make_dataset(
    *,
    name: str = "test_ds",
    url: str = "https://example.com/data.json",
    extraction_path: str | None = None,
    local_filename: str = "test_ds.json",
) -> Dataset:
    return Dataset(
        name=name,
        url=url,
        extraction_path=extraction_path,
        local_filename=local_filename,
    )


def _mock_urlopen(data: bytes) -> MagicMock:
    """Build a mock context manager that returns *data* from read()."""
    resp = MagicMock()
    resp.read.return_value = data
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


class TestFetchDatasetSuccess:
    def test_no_extraction(self, tmp_path: Path) -> None:
        payload = [{"id": 1}, {"id": 2}]
        raw = json.dumps(payload).encode("utf-8")
        with patch(
            "benchmarks.tier1.fetch_data.urllib.request.urlopen",
            return_value=_mock_urlopen(raw),
        ):
            size = fetch_dataset(
                _make_dataset(),
                data_dir=tmp_path,
            )

        dest = tmp_path / "test_ds.json"
        assert dest.exists()
        written = json.loads(dest.read_text("utf-8"))
        assert written == payload
        assert size == len(dest.read_bytes())

    def test_with_extraction_path(self, tmp_path: Path) -> None:
        payload = {"results": [1, 2, 3], "meta": "info"}
        raw = json.dumps(payload).encode("utf-8")
        with patch(
            "benchmarks.tier1.fetch_data.urllib.request.urlopen",
            return_value=_mock_urlopen(raw),
        ):
            size = fetch_dataset(
                _make_dataset(extraction_path="results"),
                data_dir=tmp_path,
            )

        dest = tmp_path / "test_ds.json"
        written = json.loads(dest.read_text("utf-8"))
        assert written == [1, 2, 3]
        assert size > 0


class TestFetchDatasetExtraction:
    def test_missing_key_saves_full_response(
        self,
        tmp_path: Path,
    ) -> None:
        payload = {"other_key": [1, 2]}
        raw = json.dumps(payload).encode("utf-8")
        with patch(
            "benchmarks.tier1.fetch_data.urllib.request.urlopen",
            return_value=_mock_urlopen(raw),
        ):
            size = fetch_dataset(
                _make_dataset(extraction_path="missing"),
                data_dir=tmp_path,
            )

        dest = tmp_path / "test_ds.json"
        written = json.loads(dest.read_text("utf-8"))
        # Full response saved when extraction path not found.
        assert written == payload
        assert size > 0

    def test_non_dict_response_saved_as_is(
        self,
        tmp_path: Path,
    ) -> None:
        payload = [10, 20, 30]
        raw = json.dumps(payload).encode("utf-8")
        with patch(
            "benchmarks.tier1.fetch_data.urllib.request.urlopen",
            return_value=_mock_urlopen(raw),
        ):
            size = fetch_dataset(
                _make_dataset(extraction_path="items"),
                data_dir=tmp_path,
            )

        dest = tmp_path / "test_ds.json"
        written = json.loads(dest.read_text("utf-8"))
        # extraction_path is ignored for non-dict responses.
        assert written == payload
        assert size > 0


class TestFetchDatasetNetworkError:
    def test_url_error_returns_zero(self, tmp_path: Path) -> None:
        with patch(
            "benchmarks.tier1.fetch_data.urllib.request.urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            size = fetch_dataset(
                _make_dataset(),
                data_dir=tmp_path,
            )

        assert size == 0
        dest = tmp_path / "test_ds.json"
        assert not dest.exists()


class TestFetchAll:
    def test_calls_all_datasets(self, tmp_path: Path) -> None:
        fetched_names: list[str] = []

        def fake_urlopen(request: object, **kw: object) -> MagicMock:
            fetched_names.append("called")
            payload = json.dumps([1]).encode("utf-8")
            return _mock_urlopen(payload)

        with patch(
            "benchmarks.tier1.fetch_data.urllib.request.urlopen",
            side_effect=fake_urlopen,
        ):
            fetch_all(tmp_path)

        assert len(fetched_names) == len(DATASETS)
