# Why Sift exists

This page summarizes research and ecosystem evidence for one repeated failure mode in agent systems: tool output overwhelms model context.

## Short version

Common workarounds map to features Sift already provides:

| Workaround | Why it fails | Sift equivalent |
|---|---|---|
| Truncate tool output | Loses data permanently | Full output is persisted, schema reference is returned |
| Summarize with an LLM | Lossy, adds latency and cost, can fail when context is already full | Lossless schema inference, no summarization call required |
| Save to file + grep/jq | No schema awareness, agent must be Unix-skilled | Schema-aware code queries with pandas/numpy |
| Paginate at the MCP server | Requires upstream changes per server | Transparent proxy, zero upstream changes |
| Remove previous tool output | Loses ability to reference earlier results | Artifacts persist across the full session |
| RAG on tool responses | Embedding overhead, approximate retrieval | Exact structured retrieval via code queries |
| Compact old context | Usually triggers late, and compaction can fail under overflow | Large output stays out of context from the start |

## Protocol creators acknowledge the gap

Anthropic's MCP engineering write-up points to two scaling problems: tool definitions consume prompt space, and intermediate tool results add tokens at each step. Their direction is code execution so agents can process data before it enters context.

Sift addresses the same bottleneck at the gateway layer. It persists output and returns references instead of raw payloads, then runs code queries against stored artifacts. This works without changing the protocol or upstream servers.

Source: [Code execution with MCP](https://www.anthropic.com/engineering/code-execution-with-mcp) (Anthropic, 2025)

## Research snapshot

### Memory pointers for tool output (IBM Research)

IBM researchers proposed memory pointers so agents operate on references instead of full tool payloads. Their method preserved full output, required no tool changes, and used roughly 7x fewer tokens than conventional workflows. Sift follows this same architecture in production form: store full output, return compact references, query details on demand.

Source: [Solving Context Window Overflow in AI Agents](https://arxiv.org/abs/2511.22729) (Bulle Labate et al., November 2025)

### Context rot (Chroma)

Chroma measured 18 LLMs and found performance drops as input length grows. Effective capacity is lower than advertised capacity, and larger windows mostly delay the failure mode.

Source: [Context Rot research](https://research.trychroma.com/context-rot) (Hong et al., 2025), referenced in [The Context Window Problem](https://factory.ai/news/context-window-problem) (Factory.ai)

### Context management remains underresearched (JetBrains / NeurIPS 2025)

Researchers from JetBrains and TU Munich found that agent-generated context often turns into noise, while efficiency-focused context management is still underresearched despite its effect on cost and reliability.

Source: [Cutting Through the Noise: Smarter Context Management for LLM-Powered Agents](https://blog.jetbrains.com/research/2025/12/efficient-context-management/) (Lindenbauer et al., December 2025)

### Recursive context folding (Prime Intellect)

Prime Intellect argues that token cost grows linearly with context length while model quality drops. Their approach delegates context handling to tools and subroutines instead of relying on larger windows.

Source: [Recursive Language Models: the paradigm of 2026](https://www.primeintellect.ai/blog/rlm) (Prime Intellect, 2025)

## What we see in production

Issue trackers across agent ecosystems show the same pattern: large tool output floods context, reactive fixes run late, and sessions become hard to recover.

### OpenClaw issues

| Issue | Observed behavior | Link |
|---|---|---|
| #10694 | A single large tool payload can overflow context in one step and trigger repeated failures | [openclaw/openclaw#10694](https://github.com/openclaw/openclaw/issues/10694) |
| #8596 | Context overflow can cause looped failures with no circuit breaker | [openclaw/openclaw#8596](https://github.com/openclaw/openclaw/issues/8596) |
| #8077 | Manual `/compact` can fail after overflow, leaving users stuck | [openclaw/openclaw#8077](https://github.com/openclaw/openclaw/issues/8077) |
| #3479 | Compaction can fail to produce useful summaries when already over limit | [openclaw/openclaw#3479](https://github.com/openclaw/openclaw/issues/3479) |
| #3154 | Overflow can leave sessions unusable until manual reset | [openclaw/openclaw#3154](https://github.com/openclaw/openclaw/issues/3154) |
| #5771 | Some users report overflow on fresh sessions after very few turns | [openclaw/openclaw#5771](https://github.com/openclaw/openclaw/issues/5771) |

### MCP client issues

| Issue | Observed behavior | Link |
|---|---|---|
| Open WebUI #15884 | MCP tools can return 15K+ tokens per call and fill small windows in a few steps | [open-webui/open-webui discussion #15884](https://github.com/open-webui/open-webui/discussions/15884) |
| GitHub community #169224 | MCP server output can exceed context, prompting pagination/file persistence workarounds | [github community discussion #169224](https://github.com/orgs/community/discussions/169224) |
| Claude Code #20421 | Tool definitions can consume a large share of context before work starts | [anthropics/claude-code#20421](https://github.com/anthropics/claude-code/issues/20421) |
| Roo Code #7042 | Oversized MCP responses from logs/queries can stall sessions | [RooCodeInc/Roo-Code#7042](https://github.com/RooCodeInc/Roo-Code/issues/7042) |
| SAP open-ux-tools #3857 | Single MCP responses can exceed 25K tokens | [SAP/open-ux-tools#3857](https://github.com/SAP/open-ux-tools/issues/3857) |

### Industry writeups

| Source | Reported pattern | Link |
|---|---|---|
| CodeRabbit | Context confusion, context clash, and token bloat in MCP-heavy workflows | [CodeRabbit post](https://www.coderabbit.ai/blog/handling-ballooning-context-in-the-mcp-era-context-engineering-on-steroids) |
| Lunar.dev | Tool metadata alone can consume tens of thousands of tokens | [Lunar.dev post](https://www.lunar.dev/post/why-is-there-mcp-tool-overload-and-how-to-solve-it-for-your-ai-agents) |
| Redis | Agents can degrade after a few tool calls even with very large windows | [Redis post](https://redis.io/blog/context-window-overflow/) |

## What this implies

The ecosystem has tried truncation, summarization, file dumps, compaction, and RAG. Each helps in some cases, but each has failure modes: data loss, late intervention, extra infrastructure, or approximate retrieval.

Sift focuses on a narrower claim and does it directly: keep large tool payloads out of prompt context while preserving full output for exact follow-up queries.
