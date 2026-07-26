# Contributing

The full contributor guide lives in [AGENTS.md](./AGENTS.md) — it's written
for AI coding agents and humans alike, and covers the repo map, commands,
hard invariants, style, versioning rules, and the docs that must stay in
sync with code changes.

The short version:

- `uv sync`, then make `uv run ruff check .`, `uv run ruff format --check .`,
  `uv run pyright`, and `uv run pytest` all pass before opening a PR.
- Schema or relay changes also need the integration suite
  (`uv run pytest -m integration`) against a real Postgres — see
  [AGENTS.md → Commands](./AGENTS.md#commands).
- PR titles must follow [Conventional Commits](https://www.conventionalcommits.org/)
  (the repo squash-merges; a CI check enforces the title).
- Non-trivial changes to the public API, schema, or delivery semantics start
  as a short spec — see [specs/README.md](./specs/README.md).

Security issues: please don't open a public issue — see
[SECURITY.md](./SECURITY.md).
