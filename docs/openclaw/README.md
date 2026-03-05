# OpenClaw Integration Pack

This pack ships one OpenClaw skill (`reliable-tool-context`) for Sift Gateway.
It is designed for model-facing CLI output where direct inline output or ad hoc
shell inspection becomes unreliable. The goal is reliable tool context for
model decisions.

## Document roles

- `SKILL.md`: model-facing runtime policy. This is the procedural source of
  truth for how the model should build reliable tool context from captured
  artifacts.
- `README.md`: human-facing explanation of why the skill exists, what behavior
  it enforces, and how to install it.

The two files are intentionally different. `SKILL.md` optimizes model behavior;
`README.md` optimizes human understanding.

## CLI usage philosophy

Use Sift to establish reliable tool context when any of these are true:
- Output will be consumed by the model (analysis, transformation, or follow-up
  querying).
- Pagination exists or may exist (`pagination.next.kind=="command"`).
- JSON schema/root confidence is low, or rows may be heterogeneous (even for
  small payloads).
- You need reproducibility, redaction discipline, or auditability.

Use direct CLI only when all of these are true:
- Output is clearly small.
- Schema/root path is obvious.
- It is a one-off human inspection with no follow-up model reasoning.

## Why reliable tool context beats direct first-item inspection

Direct shortcuts like `jq '.[0]'` are useful for quick local peeks, but they are
not reliable as a schema-discovery strategy in production workflows:

- Many payloads are object-wrapped and not top-level arrays.
- Some payloads expose multiple list roots, so first-item sampling can pick the
  wrong collection.
- Heterogeneous rows can make one row unrepresentative.
- Manual paging and copied output increase omission and drift risk.

The skill avoids those failure modes by using Sift contracts (`artifact_id`,
`response_mode`, schema metadata, explicit pagination continuity).

## Sift behavior this pack relies on

These points are aligned with current Sift CLI behavior:

- `sift-gateway run --json -- <command>` captures output and returns an
  `artifact_id`.
- `run` responses may include pagination metadata; continue only when
  `pagination.next.kind == "command"`.
- `sift-gateway code` defaults to `--scope all_related`; use `--scope single`
  when you want anchor-only analysis.
- `response_mode="schema_ref"` may provide either a representative
  `sample_item` or a `schemas` list.
- `run` captures currently use canonical root path `$` for follow-up code
  queries.
- Completeness is tied to `pagination.retrieval_status == COMPLETE`.

## Install

1. Install Sift Gateway:

```bash
uv tool install sift-gateway
```

Alternative:

```bash
pipx install sift-gateway
```

2. Write the packaged skill file:

```bash
mkdir -p ~/.openclaw/skills/reliable-tool-context
sift-gateway-openclaw-skill --output ~/.openclaw/skills/reliable-tool-context/SKILL.md
```

3. Ensure OpenClaw loads that directory (or explicitly enable the skill in your
   OpenClaw skill configuration).

Optional example:

```json5
{
  skills: {
    entries: {
      "sift-gateway-reliable-tool-context": { enabled: true },
    },
  },
}
```

## Sanity check

```bash
sift-gateway run --json -- echo '[{"id":1,"state":"open"},{"id":2,"state":"closed"}]'
sift-gateway code --json <artifact_id> '$' --code "def run(data, schema, params): return len(data)"
```

For current `run` captures, use `$` as root path for follow-up code queries.

## Maintainer note

`docs/openclaw/SKILL.md` is the editable source. Keep
`src/sift_gateway/openclaw/SKILL.md` mirrored to match packaged output.

## Related docs

- [OpenClaw skill](SKILL.md)
- [Quick start guide](../quickstart.md)
- [API contracts](../api_contracts.md)
- [Configuration reference](../config.md)
