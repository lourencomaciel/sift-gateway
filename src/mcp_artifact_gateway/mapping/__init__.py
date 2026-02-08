"""Mapping system for artifact JSON analysis.

Public API re-exports for the mapping package.
"""
from mcp_artifact_gateway.mapping.runner import (
    MappingInput,
    MappingResult,
    RootInventory,
    SampleRecord,
    SelectedJsonPart,
    run_mapping,
    select_json_part,
)
from mcp_artifact_gateway.mapping.worker import (
    WorkerContext,
    check_worker_safety,
    persist_mapping_result,
    run_mapping_worker,
    should_run_mapping,
)

__all__ = [
    "MappingInput",
    "MappingResult",
    "RootInventory",
    "SampleRecord",
    "SelectedJsonPart",
    "WorkerContext",
    "check_worker_safety",
    "persist_mapping_result",
    "run_mapping",
    "run_mapping_worker",
    "select_json_part",
    "should_run_mapping",
]
