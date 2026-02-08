import pytest

from mcp_artifact_gateway.db.repos.artifacts_repo import search_by_session


@pytest.mark.asyncio
async def test_search_by_session_invalid_order_by() -> None:
    with pytest.raises(ValueError):
        await search_by_session(None, "sess", order_by="bad")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_search_by_session_invalid_filter() -> None:
    with pytest.raises(ValueError):
        await search_by_session(None, "sess", filters={"nope": "x"})  # type: ignore[arg-type]
