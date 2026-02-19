# OpenClaw Integration Pack

This pack makes Sift the default "large output handler" for OpenClaw agents.

## What You Get

- A ready-to-install skill file: `docs/openclaw/SKILL.md`
- OpenClaw-first quickstart and workflow guidance
- Troubleshooting for context overflow patterns
- Copy-paste response templates that stay within tight context budgets

## OpenClaw-First Quickstart

1. Install Sift CLI:

```bash
pipx install sift-gateway
```

2. Install the packaged skill in your OpenClaw skills directory:

```bash
mkdir -p ~/.openclaw/skills/sift-gateway
sift-gateway-openclaw-skill --output ~/.openclaw/skills/sift-gateway/SKILL.md
```

3. Restart OpenClaw (or reload skills) and run a capture flow:

```bash
sift-gateway run -- echo '[{"id":1,"state":"open"},{"id":2,"state":"closed"}]'
sift-gateway query <artifact_id> '$' --limit 1
sift-gateway code <artifact_id> '$' --expr "df.shape[0]"
```

If the first `run` shows `pagination.has_next_page=true`, continue manually with:

```bash
sift-gateway run --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

4. Add one short system instruction in your OpenClaw profile:

```text
When command output may exceed ~4KB, capture with `sift-gateway run` and query incrementally.
```

## `sift-gateway code` Methods

Use one of these modes depending on complexity:

- `--expr "<python_expr>"` for fast DataFrame expressions.
- `--code "<python_source>"` for inline `run(data, schema, params)`.
- `--file <path.py>` for file-based `run(data, schema, params)`.
- `--params '<json_object>'` to pass runtime parameters into `params`.
- `--scope single` to restrict processing to the anchor artifact(s) only.
- Multi-artifact input:
  - shared root path: repeat `--artifact-id` and provide one `--root-path`.
  - per-artifact root paths: repeat both `--artifact-id` and `--root-path` in the same order.

```bash
sift-gateway code <artifact_id> '$.items' --expr "df.shape[0]"
sift-gateway code <artifact_id> '$.items' --scope single --expr "df.shape[0]"
sift-gateway code <artifact_id> '$.items' --code "def run(data, schema, params): return {'rows': len(data)}"
sift-gateway code <artifact_id> '$.items' --file ./analysis.py --params '{"window":"7d"}'
sift-gateway code --artifact-id art_users --artifact-id art_orders --root-path '$.items' --expr "len(df)"
sift-gateway code --artifact-id art_users --artifact-id art_orders --root-path '$.users' --root-path '$.orders' --file ./join.py
```

## Capture vs Inline Decision Rule

- Inline: expected output < 4KB and used once.
- Capture: lists, logs, paginated APIs, JSON blobs, tabular data, or anything reused.
- Always capture: `gh api`, `kubectl ... -o json`, `curl` returning arrays/objects.

## Manual Validation Checklist

- `sift-gateway run -- <cmd>` returns an artifact summary.
- `sift-gateway query <id> '$' --limit 5` returns bounded data.
- Paginated captures can continue with `run --continue-from <id> -- <cmd>`.
- `sift-gateway run` always captures a fresh run result.
- `sift-gateway run --stdin` works from a pipe.
- `sift-gateway diff <id1> <id2>` reports equality or bounded diff lines.

## Related Docs

- `docs/openclaw/SKILL.md`
- `docs/openclaw/troubleshooting.md`
- `docs/openclaw/response-templates.md`
