# Task Plan: Implement Postmortem Improvements (S7, S8, S3, S2)

## Goal
Implement 4 improvements from the Meta Ads postmortem evaluation to reduce context waste and improve LLM-facing UX.

## Current Phase
Complete

## Files Modified
- `src/sidepouch_mcp/pagination/contract.py` — S7: hint field in retrieval pagination
- `src/sidepouch_mcp/query/where_dsl.py` — S2+S8: casts + better error messages
- `src/sidepouch_mcp/query/jsonpath.py` — S8: predicate error message
- `src/sidepouch_mcp/mcp/handlers/artifact_select.py` — S3: cursor embed + extract
- `src/sidepouch_mcp/tools/artifact_select.py` — S3: optional params with cursor
- `tests/unit/test_where_dsl.py` — 18 new cast function tests
- `tests/unit/test_jsonpath.py` — 2 new predicate error tests
- `tests/unit/test_server.py` — updated cursor binding assertions

## Results
- 1252 tests passed (18 new), 13 pre-existing psycopg failures
- Lint + format clean on all modified files
