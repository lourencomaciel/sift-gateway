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
pipx install sift-mcp
```

2. Install the skill in your OpenClaw skills directory:

```bash
mkdir -p ~/.openclaw/skills/sift
cp docs/openclaw/SKILL.md ~/.openclaw/skills/sift/SKILL.md
```

3. Restart OpenClaw (or reload skills) and run a capture flow:

```bash
sift run -- echo '[{"id":1,"state":"open"},{"id":2,"state":"closed"}]'
sift query <artifact_id> '$' --limit 1
sift code <artifact_id> '$' --expr "df.shape[0]"
```

4. Add one short system instruction in your OpenClaw profile:

```text
When command output may exceed ~4KB, capture with `sift run` and query incrementally.
```

## Capture vs Inline Decision Rule

- Inline: expected output < 4KB and used once.
- Capture: lists, logs, paginated APIs, JSON blobs, tabular data, or anything reused.
- Always capture: `gh api`, `kubectl ... -o json`, `curl` returning arrays/objects.

## Manual Validation Checklist

- `sift run -- <cmd>` returns an artifact summary.
- `sift query <id> '$' --limit 5` returns bounded data.
- `sift run` always captures a fresh run result.
- `sift run --stdin` works from a pipe.
- `sift diff <id1> <id2>` reports equality or bounded diff lines.

## Related Docs

- `docs/openclaw/SKILL.md`
- `docs/openclaw/troubleshooting.md`
- `docs/openclaw/response-templates.md`
