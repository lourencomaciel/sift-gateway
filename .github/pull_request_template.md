## Summary

- What changed and why?

## Validation

- [ ] `uv run python -m ruff check src tests`
- [ ] `uv run python -m mypy src`
- [ ] `uv run python -m pytest tests/unit/ -q`
- [ ] `PYTHONPATH=src uv run python scripts/check_docs_consistency.py`

## CLI + Docs Contract

- [ ] If CLI flags/commands changed, I updated docs and CLI tests.
- [ ] If docs changed, they are consistent with runtime behavior.
- [ ] No generated docs or contract files were edited manually without the matching generator/check updates.
