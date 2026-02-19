from __future__ import annotations

from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.db.repos.search_repo import (
    LIST_ARTIFACTS_SQL,
    LIST_DERIVED_SQL,
    SEARCH_ARTIFACTS_FTS_SQL,
    list_artifacts_params,
    list_derived_params,
    search_artifacts_fts_params,
)


def test_search_artifacts_fts_sql_is_workspace_scoped_without_artifact_refs() -> (
    None
):
    sql = SEARCH_ARTIFACTS_FTS_SQL.lower()
    assert "from artifacts_fts" in sql
    assert "join artifacts a" in sql
    assert "a.workspace_id = %s" in sql
    assert "artifact_refs" not in sql
    assert "bm25(artifacts_fts)" in sql


def test_search_artifacts_fts_params() -> None:
    params = search_artifacts_fts_params(
        query="kubernetes pods",
        limit=25,
        offset=10,
    )
    assert params == (WORKSPACE_ID, "kubernetes pods", 25, 10)


def test_list_artifacts_sql_supports_kind_filter() -> None:
    sql = LIST_ARTIFACTS_SQL.lower()
    assert "a.kind = %s" in sql
    assert "limit %s offset %s" in sql


def test_list_artifacts_params() -> None:
    params = list_artifacts_params(
        include_deleted=False,
        kind="derived_query",
        limit=50,
        offset=5,
    )
    assert params == (
        WORKSPACE_ID,
        0,
        "derived_query",
        "derived_query",
        50,
        5,
    )


def test_list_derived_sql_and_params() -> None:
    sql = LIST_DERIVED_SQL.lower()
    assert "from artifact_lineage_edges" in sql
    assert "le.parent_artifact_id = %s" in sql
    assert "a.kind = %s" in sql

    params = list_derived_params(
        parent_artifact_id="art_parent",
        kind="derived_codegen",
        limit=20,
        offset=1,
    )
    assert params == (
        WORKSPACE_ID,
        "art_parent",
        "derived_codegen",
        "derived_codegen",
        20,
        1,
    )
