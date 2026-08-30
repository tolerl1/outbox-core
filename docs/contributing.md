# Contributing

The canonical contributor guide is
[AGENTS.md](https://github.com/tolerl1/outbox-core/blob/main/AGENTS.md)
in the repository root - written for AI coding agents and humans alike, it
covers the repo map, commands, hard invariants, style, and the docs that
must stay in sync with code changes. `CLAUDE.md` and
`.github/copilot-instructions.md` defer to it, and the repo ships agent
environment setup for both GitHub Copilot's coding agent and Claude Code on
the web.

Quick reference:

```bash
uv sync                     # install everything
uv run pytest               # unit tests (integration skips without a DB)
uv run ruff check .
uv run ruff format .
uv run pyright              # strict
uv run --group docs zensical serve   # preview this docs site locally
```

Non-trivial changes to the public API, schema, or delivery semantics start
as a short spec - see
[specs/README.md](https://github.com/tolerl1/outbox-core/blob/main/specs/README.md).

For consumers of the library, `src/outbox/.agents/skills/` bundles three
Agent Skills (`outbox-integrate`, `outbox-new-provider`, `outbox-review`)
directly into the installed package - they teach an agent to wire up the
write path correctly, implement providers, and flag dual-write regressions
in review. Run `uvx library-skills` in a consuming project to symlink them
into `.agents/skills/` or `.claude/skills/`. Agent Skills are an
[open, agent-neutral
format](https://docs.github.com/en/copilot/concepts/agents/about-agent-skills);
GitHub Copilot reads them from the same locations, so these aren't
Claude-only.

Security reports: see
[SECURITY.md](https://github.com/tolerl1/outbox-core/blob/main/SECURITY.md);
please don't open a public issue.
